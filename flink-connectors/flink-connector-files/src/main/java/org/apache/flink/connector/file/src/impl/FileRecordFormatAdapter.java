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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.IteratorResultIterator;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.connector.file.src.util.Utils.doWithCleanupOnException;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The FormatReaderAdapter turns a {@link FileRecordFormat} into a {@link BulkFormat}. */
@Internal
public final class FileRecordFormatAdapter<T> implements BulkFormat<T, FileSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final FileRecordFormat<T> fileFormat;

    public FileRecordFormatAdapter(FileRecordFormat<T> fileFormat) {
        this.fileFormat = checkNotNull(fileFormat);
    }

    @Override
    public BulkFormat.Reader<T> createReader(
            final Configuration config, final FileSourceSplit split) throws IOException {

        final FileRecordFormat.Reader<T> reader =
                fileFormat.createReader(config, split.path(), split.offset(), split.length());

        return doWithCleanupOnException(
                reader,
                () -> {
                    //noinspection CodeBlock2Expr
                    return wrapReader(reader, config, CheckpointedPosition.NO_OFFSET, 0L);
                });
    }

    @Override
    public BulkFormat.Reader<T> restoreReader(
            final Configuration config, final FileSourceSplit split) throws IOException {

        assert split.getReaderPosition().isPresent();
        final CheckpointedPosition checkpointedPosition = split.getReaderPosition().get();

        final Path filePath = split.path();
        final long splitOffset = split.offset();
        final long splitLength = split.length();

        final FileRecordFormat.Reader<T> reader =
                checkpointedPosition.getOffset() == CheckpointedPosition.NO_OFFSET
                        ? fileFormat.createReader(config, filePath, splitOffset, splitLength)
                        : fileFormat.restoreReader(
                                config,
                                filePath,
                                checkpointedPosition.getOffset(),
                                splitOffset,
                                splitLength);

        return doWithCleanupOnException(
                reader,
                () -> {
                    long remaining = checkpointedPosition.getRecordsAfterOffset();
                    while (remaining > 0 && reader.read() != null) {
                        remaining--;
                    }

                    return wrapReader(
                            reader,
                            config,
                            checkpointedPosition.getOffset(),
                            checkpointedPosition.getRecordsAfterOffset());
                });
    }

    @Override
    public boolean isSplittable() {
        return fileFormat.isSplittable();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return fileFormat.getProducedType();
    }

    private static <T> Reader<T> wrapReader(
            final FileRecordFormat.Reader<T> reader,
            final Configuration config,
            final long startingOffset,
            final long startingSkipCount) {

        final int numRecordsPerBatch = config.get(FileRecordFormat.RECORDS_PER_FETCH);
        return new Reader<>(reader, numRecordsPerBatch, startingOffset, startingSkipCount);
    }

    // ------------------------------------------------------------------------

    /** The reader adapter, from {@link FileRecordFormat.Reader} to {@link BulkFormat.Reader}. */
    public static final class Reader<T> implements BulkFormat.Reader<T> {

        private final FileRecordFormat.Reader<T> reader;
        private final int numRecordsPerBatch;

        private long lastOffset;
        private long lastRecordsAfterOffset;

        Reader(
                final FileRecordFormat.Reader<T> reader,
                final int numRecordsPerBatch,
                final long initialOffset,
                final long initialSkipCount) {
            checkArgument(numRecordsPerBatch > 0, "numRecordsPerBatch must be > 0");
            this.reader = checkNotNull(reader);
            this.numRecordsPerBatch = numRecordsPerBatch;
            this.lastOffset = initialOffset;
            this.lastRecordsAfterOffset = initialSkipCount;
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            updateCheckpointablePosition();

            final ArrayList<T> result = new ArrayList<>(numRecordsPerBatch);
            T next;
            int remaining = numRecordsPerBatch;
            while (remaining-- > 0 && ((next = reader.read()) != null)) {
                result.add(next);
            }

            if (result.isEmpty()) {
                return null;
            }

            final RecordIterator<T> iter =
                    new IteratorResultIterator<>(
                            result.iterator(), lastOffset, lastRecordsAfterOffset);
            lastRecordsAfterOffset += result.size();
            return iter;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        private void updateCheckpointablePosition() {
            final CheckpointedPosition position = reader.getCheckpointedPosition();
            if (position != null) {
                this.lastOffset = position.getOffset();
                this.lastRecordsAfterOffset = position.getRecordsAfterOffset();
            }
        }
    }
}
