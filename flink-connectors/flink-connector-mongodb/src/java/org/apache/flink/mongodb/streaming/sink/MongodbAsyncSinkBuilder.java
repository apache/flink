/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import org.bson.Document;

/**
 * Builder to construct {@link MongodbAsyncSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link MongodbAsyncSink} that writes
 * String values to a Mongodb collection.
 *
 * <pre>{@code
 * ElementConverter<String, PutRecordsRequestEntry> elementConverter =
 *             MongodbAsyncSinkElementConverter.<String>builder()
 *                     .setSerializationSchema(new DocumentDeserializer())
 *                     .build();
 *
 * MongodbAsyncSink<String> kdsSink =
 *                 MongodbAsyncSink.<String>builder()
 *                         .setConnectionString("mongodb://admin:password@192.168.221.201:27017")
 *                         .setElementConverter(elementConverter)
 *                         .setDatabase("your_stream_db")
 *                         .setCollection("your_strem_coll")
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 200
 *   <li>{@code maxInFlightRequests} will be 16
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code flushOnBufferSizeInBytes} will be 64MB i.e. {@code 64 * 1024 * 1024}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code failOnError} will be false
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class MongodbAsyncSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, Document, MongodbAsyncSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 16;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1 * 1024 * 1024;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;

    private Boolean failOnError;
    private String connectionString;
    private String database;
    private String collection;

    MongodbAsyncSinkBuilder() {}

    public MongodbAsyncSinkBuilder<InputT> setConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    public MongodbAsyncSinkBuilder<InputT> setDatabase(String database) {
        this.database = database;
        return this;
    }

    public MongodbAsyncSinkBuilder<InputT> setCollection(String collection) {
        this.collection = collection;
        return this;
    }

    public MongodbAsyncSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    @Override
    public MongodbAsyncSink<InputT> build() {
        return new MongodbAsyncSink<>(
                getElementConverter(),
                getMaxBatchSize() == null ? DEFAULT_MAX_BATCH_SIZE : getMaxBatchSize(),
                getMaxInFlightRequests() == null
                        ? DEFAULT_MAX_IN_FLIGHT_REQUESTS
                        : getMaxInFlightRequests(),
                getMaxBufferedRequests() == null
                        ? DEFAULT_MAX_BUFFERED_REQUESTS
                        : getMaxBufferedRequests(),
                getMaxBatchSizeInBytes() == null
                        ? DEFAULT_MAX_BATCH_SIZE_IN_B
                        : getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS() == null
                        ? DEFAULT_MAX_TIME_IN_BUFFER_MS
                        : getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes() == null
                        ? DEFAULT_MAX_RECORD_SIZE_IN_B
                        : getMaxRecordSizeInBytes(),
                connectionString,
                database,
                collection,
                failOnError == null ? DEFAULT_FAIL_ON_ERROR : failOnError);
    }
}
