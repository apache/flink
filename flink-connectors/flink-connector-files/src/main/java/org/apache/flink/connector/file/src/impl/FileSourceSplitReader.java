/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

/** The {@link SplitReader} implementation for the file source. */
@Internal
final class FileSourceSplitReader<T, SplitT extends FileSourceSplit>
        implements SplitReader<RecordAndPosition<T>, SplitT> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSourceSplitReader.class);

    private final Configuration config;
    private final BulkFormat<T, SplitT> readerFactory;

    private final Queue<SplitT> splits;

    @Nullable private BulkFormat.Reader<T> currentReader;
    @Nullable private String currentSplitId;

    public FileSourceSplitReader(Configuration config, BulkFormat<T, SplitT> readerFactory) {
        this.config = config;
        this.readerFactory = readerFactory;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<T>> fetch() throws IOException {
        checkSplitOrStartNext();

        final BulkFormat.RecordIterator<T> nextBatch = currentReader.readBatch();
        return nextBatch == null
                ? finishSplit()
                : FileRecords.forRecords(currentSplitId, nextBatch);
    }

    @Override
    public void handleSplitsChanges(final SplitsChange<SplitT> splitChange) {
        if (!(splitChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitChange.getClass()));
        }

        LOG.debug("Handling split change {}", splitChange);
        splits.addAll(splitChange.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        final SplitT nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }

        currentSplitId = nextSplit.splitId();

        final Optional<CheckpointedPosition> position = nextSplit.getReaderPosition();
        currentReader =
                position.isPresent()
                        ? readerFactory.restoreReader(config, nextSplit)
                        : readerFactory.createReader(config, nextSplit);
    }

    private FileRecords<T> finishSplit() throws IOException {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }

        final FileRecords<T> finishRecords = FileRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }
}
