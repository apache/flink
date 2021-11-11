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

package org.apache.flink.connector.file.src.util;

import org.apache.flink.connector.file.src.reader.BulkFormat;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Implementation of {@link org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator}
 * that wraps another iterator and performs the mapping of the records. You can use {@link
 * #wrapReader(BulkFormat.Reader, RecordMapper)} to wrap a whole reader.
 *
 * @param <I> Input type
 * @param <O> Mapped output type
 */
public class RecordMapperWrapperRecordIterator<I, O> implements BulkFormat.RecordIterator<O> {

    /** Record mapper definition. */
    @FunctionalInterface
    public interface RecordMapper<I, O> {
        /** Map the record. Both input value and output value are expected to be non-null. */
        O map(I in);
    }

    private final BulkFormat.RecordIterator<I> wrapped;
    private final RecordMapper<I, O> mapper;

    public RecordMapperWrapperRecordIterator(
            BulkFormat.RecordIterator<I> wrapped, RecordMapper<I, O> mapper) {
        this.wrapped = wrapped;
        this.mapper = mapper;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public RecordAndPosition<O> next() {
        RecordAndPosition record = this.wrapped.next();
        if (record == null || record.getRecord() == null) {
            return (RecordAndPosition<O>) record;
        }

        record.record = mapper.map((I) record.record);
        return (RecordAndPosition<O>) record;
    }

    @Override
    public void releaseBatch() {
        this.wrapped.releaseBatch();
    }

    /**
     * Wrap a {@link BulkFormat.Reader} applying a {@link RecordMapper} on the returned iterator.
     *
     * @param <I> Input type
     * @param <O> Mapped output type
     */
    public static <I, O> BulkFormat.Reader<O> wrapReader(
            BulkFormat.Reader<I> wrappedReader, RecordMapper<I, O> recordMapper) {
        return new BulkFormat.Reader<O>() {
            @Nullable
            @Override
            public BulkFormat.RecordIterator<O> readBatch() throws IOException {
                BulkFormat.RecordIterator<I> iterator = wrappedReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return new RecordMapperWrapperRecordIterator<>(iterator, recordMapper);
            }

            @Override
            public void close() throws IOException {
                wrappedReader.close();
            }
        };
    }
}
