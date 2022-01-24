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
 * @param <InputT> input records to the sink
 */
@PublicEvolving
public class DynamoDbSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, DynamoDbWriteRequest, DynamoDbSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 25;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 10;
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
     * @param dynamoDbTablesConfig the {@link DynamoDbTablesConfig} to be used for the sink
     * @return
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
