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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import java.io.IOException;
import java.util.Collection;

/**
 * Flink Sink to produce data into a single or multiple DynamoDb tables.
 *
 * @param <InputT> type of incoming records
 * @see DynamoDbSinkBuilder on how to construct a KafkaSink
 */
@PublicEvolving
public class DynamoDbSink<InputT> extends AsyncSinkBase<InputT, DynamoDbWriteRequest> {

    private DynamoDbClientProvider dynamoDbClientProvider;
    private DynamoDbTablesConfig dynamoDbTablesConfig;

    protected DynamoDbSink(
            DynamoDbClientProvider dynamoDbClientProvider,
            ElementConverter<InputT, DynamoDbWriteRequest> elementConverter,
            DynamoDbTablesConfig dynamoDbTablesConfig,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.dynamoDbClientProvider = dynamoDbClientProvider;
        this.dynamoDbTablesConfig = dynamoDbTablesConfig;
    }

    /**
     * Create a {@link DynamoDbSinkBuilder} to construct a new {@link DynamoDbSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link DynamoDbSinkBuilder}
     */
    public static <IN> DynamoDbSinkBuilder<IN> builder() {
        return new DynamoDbSinkBuilder<>();
    }

    @Internal
    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<DynamoDbWriteRequest>> createWriter(
            InitContext context) throws IOException {
        return null;
    }

    @Internal
    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<DynamoDbWriteRequest>> restoreWriter(
            InitContext context,
            Collection<BufferedRequestState<DynamoDbWriteRequest>> recoveredState)
            throws IOException {
        return null;
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<BufferedRequestState<DynamoDbWriteRequest>>
            getWriterStateSerializer() {
        return new DynamoDbWriterStateSerializer();
    }
}
