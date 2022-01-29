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

import java.util.Optional;
import java.util.Properties;

/**
 * Builder to construct {@link MongoDbSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link MongoDbSink} that writes
 * String values to a MongoDb collection named your_collection, in a database named your_collection.
 *
 * <pre>{@code
 * MongoDbSink<String> mongoDbSink =
 *                 MongoDbSink.<String>builder()
 *                         .setDatabaseName("your_database")
 *                         .setCollectionName("your_collection")
 *                         .setDocumentGenerator(element -> ImmutableMap.of("doc_key", "doc_value"))
 *                         .setFilterGenerator(element -> ImmutableMap.of("filter_key", "filter_value"))
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 500
 *   <li>{@code maxInFlightRequests} will be 16
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code maxBatchSizeInBytes} will be 16 MB i.e. {@code 16 * 1024 * 1024}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 16 MB i.e. {@code 16 * 1024 * 1024}
 *   <li>{@code failOnError} will be false
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class MongoDbSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, MongoReplaceOneModel, MongoDbSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 16;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 16 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 16 * 1024 * 1024;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;

    private Boolean failOnError;
    private String databaseName;
    private String collectionName;
    private Properties mongoClientProperties;
    private DocumentGenerator<InputT> documentGenerator;
    private FilterGenerator<InputT> filterGenerator;

    MongoDbSinkBuilder() {}

    /**
     * Sets the name of the MongoDb database that the sink will connect to. There is no default for
     * this parameter, therefore, this must be provided at sink creation time otherwise the build
     * will fail.
     *
     * @param databaseName the name of the database
     * @return {@link MongoDbSinkBuilder} itself
     */
    public MongoDbSinkBuilder<InputT> setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Sets the name of the MongoDb collection that the sink will connect to. There is no default
     * for this parameter, therefore, this must be provided at sink creation time otherwise the
     * build will fail.
     *
     * @param collectionName the name of the collection
     * @return {@link MongoDbSinkBuilder} itself
     */
    public MongoDbSinkBuilder<InputT> setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public MongoDbSinkBuilder<InputT> setDocumentGenerator(
            DocumentGenerator<InputT> documentGenerator) {
        this.documentGenerator = documentGenerator;
        return this;
    }

    public MongoDbSinkBuilder<InputT> setFilterGenerator(FilterGenerator<InputT> filterGenerator) {
        this.filterGenerator = filterGenerator;
        return this;
    }

    public MongoDbSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public MongoDbSinkBuilder<InputT> setMongoClientProperties(Properties mongoClientProperties) {
        this.mongoClientProperties = mongoClientProperties;
        return this;
    }

    @Override
    public MongoDbSink<InputT> build() {
        return new MongoDbSink<>(
                new MongoDbSinkElementConverter.Builder<InputT>()
                        .setDocumentGenerator(documentGenerator)
                        .setFilterGenerator(filterGenerator)
                        .build(),
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
                Optional.ofNullable(mongoClientProperties).orElse(new Properties()));
    }
}
