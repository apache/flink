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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

import java.io.IOException;
import java.util.UUID;

/** The {@link CompactReader} to delegate {@link CompactBulkReader}. */
public class CompactBulkReader<T> implements CompactReader<T> {

    private final BulkFormat.Reader<T> reader;
    private BulkFormat.RecordIterator<T> iterator;

    public CompactBulkReader(BulkFormat.Reader<T> reader) throws IOException {
        this.reader = reader;
        this.iterator = reader.readBatch();
    }

    @Override
    public T read() throws IOException {
        if (iterator == null) {
            return null;
        }

        RecordAndPosition<T> record = iterator.next();
        if (record != null) {
            return record.getRecord();
        } else {
            iterator.releaseBatch();
            iterator = reader.readBatch();
            return read();
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static <T> CompactReader.Factory<T> factory(BulkFormat<T, FileSourceSplit> format) {
        return new Factory<>(format);
    }

    /** Factory to create {@link CompactBulkReader}. */
    private static class Factory<T> implements CompactReader.Factory<T> {

        private static final long serialVersionUID = 1L;

        private final BulkFormat<T, FileSourceSplit> format;

        public Factory(BulkFormat<T, FileSourceSplit> format) {
            this.format = format;
        }

        @Override
        public CompactReader<T> create(CompactContext context) throws IOException {
            final String splitId = UUID.randomUUID().toString();
            final long len = context.getFileSystem().getFileStatus(context.getPath()).getLen();
            return new CompactBulkReader<>(
                    format.createReader(
                            context.getConfig(),
                            new FileSourceSplit(splitId, context.getPath(), 0, len)));
        }
    }
}
