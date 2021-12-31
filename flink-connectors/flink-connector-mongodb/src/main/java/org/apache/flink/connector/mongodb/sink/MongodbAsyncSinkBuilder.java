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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import org.bson.Document;

import java.util.Optional;
import java.util.Properties;

/**
 * Builder to construct {@link MongodbAsyncSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link MongodbAsyncSink} that writes
 * String values to a Mongodb collection.
 *
 * <pre>{@code
 * ElementConverter<String, PutRecordsRequestEntry> elementConverter =
 *             MongodbAsyncSinkElementConverter.<String>builder()
 *                     .setSerializationSchema(new Documentserializer())
 *                     .build();
 *
 * MongodbAsyncSink<String> mongodbSink =
 *                 MongodbAsyncSink.<String>builder()
 *                         .setElementConverter(elementConverter)
 *                         .setDatabase("your_stream_db")
 *                         .setCollection("your_strem_coll")
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 500
 *   <li>{@code maxInFlightRequests} will be 16
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code maxBatchSizeInBytes} will be 5 MB i.e. {@code 5 * 1024 * 1024}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 1 MB i.e. {@code 1 * 1024 * 1024}
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
    private String collectionName;
    private String databaseName;
    private Properties mongodbClientProperties;

    MongodbAsyncSinkBuilder() {}

    public MongodbAsyncSinkBuilder<InputT> setDatabase(String database) {
        this.databaseName = database;
        return this;
    }

    public MongodbAsyncSinkBuilder<InputT> setCollection(String collection) {
        this.collectionName = collection;
        return this;
    }

    public MongodbAsyncSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public MongodbAsyncSinkBuilder<InputT> setMongodbClientProperties(
            Properties mongodbClientProperties) {
        this.mongodbClientProperties = mongodbClientProperties;
        return this;
    }

    @Override
    public MongodbAsyncSink<InputT> build() {
        return new MongodbAsyncSink<>(
                getElementConverter(),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                Optional.ofNullable(failOnError).orElse(DEFAULT_FAIL_ON_ERROR),
                databaseName,
                collectionName,
                Optional.ofNullable(mongodbClientProperties).orElse(new Properties()));
    }
}
