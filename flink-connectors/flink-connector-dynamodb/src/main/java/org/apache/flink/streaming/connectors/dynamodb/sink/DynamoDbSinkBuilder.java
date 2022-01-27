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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import java.util.Optional;
import java.util.Properties;

/**
 * Builder to construct {@link DynamoDbSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link DynamoDbSink} that writes
 * records into DynamoDb
 *
 * <pre>{@code
 * private static class DummyElementConverter
 *         implements ElementConverter<String, DynamoDbWriteRequest> {
 *
 *     @Override
 *     public DynamoDbWriteRequest apply(String element, SinkWriter.Context context) {
 *         final Map<String, AttributeValue> map = new HashMap<>();
 *         map.put("your-key", AttributeValue.builder().s(element).build());
 *         return new DynamoDbWriteRequest(
 *                 "your-table-name",
 *                 WriteRequest.builder()
 *                         .putRequest(PutRequest.builder().item(map).build())
 *                         .build());
 *      }
 * }
 * DynamoDbSink<String> dynamoDbSink = DynamoDbSink.<String>builder()
 *                                          .setElementConverter(new DummyElementConverter())
 *                                       .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 25
 *   <li>{@code maxInFlightRequests} will be 50
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code maxBatchSizeInBytes} will be 16 MB i.e. {@code 16 * 1000 * 1000}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 400 KB i.e. {@code 400 * 1000 * 1000}
 *   <li>{@code failOnError} will be false
 *   <li>{@code dynamoDbTablesConfig} will be empty meaning no records deduplication will be
 *       performed by the sink
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class DynamoDbSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, DynamoDbWriteRequest, DynamoDbSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 25;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 16 * 1000 * 1000;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 400 * 1000;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;

    private boolean failOnError;
    private DynamoDbTablesConfig dynamoDbTablesConfig;
    private Properties dynamodbClientProperties;

    private ElementConverter<InputT, DynamoDbWriteRequest> elementConverter;

    public DynamoDbSinkBuilder<InputT> setDynamoDbProperties(Properties properties) {
        this.dynamodbClientProperties = properties;
        return this;
    }

    /**
     * @param elementConverter the {@link ElementConverter} to be used for the sink
     * @return {@link DynamoDbSinkBuilder} itself
     */
    public DynamoDbSinkBuilder<InputT> setElementConverter(
            ElementConverter<InputT, DynamoDbWriteRequest> elementConverter) {
        this.elementConverter = elementConverter;
        return this;
    }

    public ElementConverter<InputT, DynamoDbWriteRequest> getElementConverter() {
        return elementConverter;
    }

    /**
     * @param dynamoDbTablesConfig the {@link DynamoDbTablesConfig} if provided for the table, the
     *     DynamoDb sink will attempt to deduplicate records with the same primary and/or secondary
     *     keys in the same batch request. Only the latest record with the same combination of key
     *     attributes is preserved in the request.
     */
    public DynamoDbSinkBuilder<InputT> setDynamoDbTablesConfig(
            DynamoDbTablesConfig dynamoDbTablesConfig) {
        this.dynamoDbTablesConfig = dynamoDbTablesConfig;
        return this;
    }

    public DynamoDbSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    @Override
    public DynamoDbSink<InputT> build() {
        return new DynamoDbSink<>(
                getElementConverter(),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                Optional.of(failOnError).orElse(DEFAULT_FAIL_ON_ERROR),
                Optional.ofNullable(dynamoDbTablesConfig).orElse(new DynamoDbTablesConfig()),
                Optional.ofNullable(dynamodbClientProperties).orElse(new Properties()));
    }
}
