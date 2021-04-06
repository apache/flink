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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** A {@link BulkFormat} that can limit output record number. */
public class LimitableBulkFormat<T, SplitT extends FileSourceSplit>
        implements BulkFormat<T, SplitT> {

    private static final long serialVersionUID = 1L;

    private final BulkFormat<T, SplitT> format;
    private final long limit;

    /**
     * Limit the total number of records read by this format. When the limit is reached, subsequent
     * readers will no longer read data.
     */
    @Nullable private transient AtomicLong globalNumberRead;

    private LimitableBulkFormat(BulkFormat<T, SplitT> format, long limit) {
        this.format = format;
        this.limit = limit;
    }

    @Override
    public Reader<T> createReader(Configuration config, SplitT split) throws IOException {
        return wrapReader(format.createReader(config, split));
    }

    @Override
    public Reader<T> restoreReader(Configuration config, SplitT split) throws IOException {
        return wrapReader(format.restoreReader(config, split));
    }

    private synchronized Reader<T> wrapReader(Reader<T> reader) {
        if (globalNumberRead == null) {
            globalNumberRead = new AtomicLong(0);
        }
        return new LimitableReader<>(reader, globalNumberRead, limit);
    }

    @Override
    public boolean isSplittable() {
        return format.isSplittable();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return format.getProducedType();
    }

    private static class LimitableReader<T> implements Reader<T> {

        private final Reader<T> reader;
        private final AtomicLong numRead;
        private final long limit;

        private LimitableReader(Reader<T> reader, AtomicLong numRead, long limit) {
            this.reader = reader;
            this.numRead = numRead;
            this.limit = limit;
        }

        private boolean reachLimit() {
            return numRead.get() >= limit;
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            if (reachLimit()) {
                return null;
            }

            RecordIterator<T> batch = reader.readBatch();
            return batch == null ? null : new LimitableIterator(batch);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        private class LimitableIterator implements RecordIterator<T> {

            private final RecordIterator<T> iterator;

            private LimitableIterator(RecordIterator<T> iterator) {
                this.iterator = iterator;
            }

            @Nullable
            @Override
            public RecordAndPosition<T> next() {
                if (reachLimit()) {
                    return null;
                }
                numRead.incrementAndGet();
                return iterator.next();
            }

            @Override
            public void releaseBatch() {
                iterator.releaseBatch();
            }
        }
    }

    public static <T, SplitT extends FileSourceSplit> BulkFormat<T, SplitT> create(
            BulkFormat<T, SplitT> format, Long limit) {
        return limit == null ? format : new LimitableBulkFormat<>(format, limit);
    }
}
