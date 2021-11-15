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

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

/**
 * Builder to construct {@link DynamoDbSink}.
 *
 * @param <InputT> input records to the sink
 */
@PublicEvolving
public class DynamoDbSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, DynamoDbWriteRequest, DynamoDbSinkBuilder<InputT>> {

    private DynamoDbClientProvider dynamoDbClientProvider;
    private DynamoDbTablesConfig dynamoDbTablesConfig;

    private ElementConverter<InputT, DynamoDbWriteRequest> elementConverter;

    /**
     * @param dynamoDbClientProvider the {@link DynamoDbClientProvider} to be used for the sink
     * @return DynamoDbSinkBuilder itself
     */
    public DynamoDbSinkBuilder<InputT> setDynamoDbClientProvider(
            DynamoDbClientProvider dynamoDbClientProvider) {
        this.dynamoDbClientProvider = dynamoDbClientProvider;
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

    @Override
    public DynamoDbSink<InputT> build() {
        return new DynamoDbSink<>(
                dynamoDbClientProvider,
                getElementConverter(),
                dynamoDbTablesConfig,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes());
    }
}
