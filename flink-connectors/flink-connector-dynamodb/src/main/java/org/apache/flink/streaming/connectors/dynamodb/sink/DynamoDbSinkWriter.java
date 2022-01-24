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

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.util.AWSDynamoDbUtil;
import org.apache.flink.streaming.connectors.dynamodb.util.DynamoDbExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * TODO.
 *
 * @param <InputT>
 */
class DynamoDbSinkWriter<InputT> extends AsyncSinkWriter<InputT, DynamoDbWriteRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSinkWriter.class);

    /* A counter for the total number of records that have encountered an error during put */
    private final Counter numRecordsOutErrorsCounter;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    private final DynamoDbTablesConfig tablesConfig;
    private final DynamoDbAsyncClient client;
    private final boolean failOnError;

    public DynamoDbSinkWriter(
            ElementConverter<InputT, DynamoDbWriteRequest> elementConverter,
            InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            DynamoDbTablesConfig tablesConfig,
            Properties dynamoDbClientProperties,
            Collection<BufferedRequestState<DynamoDbWriteRequest>> states) {
        super(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                states);
        this.failOnError = failOnError;
        this.tablesConfig = tablesConfig;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.client = AWSDynamoDbUtil.createClient(dynamoDbClientProperties);
    }

    @Override
    protected void submitRequestEntries(
            List<DynamoDbWriteRequest> requestEntries,
            Consumer<List<DynamoDbWriteRequest>> requestResultConsumer) {

        TableRequestsContainer container = new TableRequestsContainer(tablesConfig);
        requestEntries.forEach(container::put);

        CompletableFuture<BatchWriteItemResponse> future =
                client.batchWriteItem(
                        BatchWriteItemRequest.builder()
                                .requestItems(container.getRequestItems())
                                .build());

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, requestResultConsumer);
                    } else if (response.unprocessedItems() != null
                            && !response.unprocessedItems().isEmpty()) {
                        handlePartiallyUnprocessedRequest(response, requestResultConsumer);
                    } else {
                        requestResultConsumer.accept(Collections.emptyList());
                    }
                });
    }

    private void handlePartiallyUnprocessedRequest(
            BatchWriteItemResponse response, Consumer<List<DynamoDbWriteRequest>> requestResult) {
        List<DynamoDbWriteRequest> unprocessed = new ArrayList<>();

        for (String tableName : response.unprocessedItems().keySet()) {
            for (WriteRequest request : response.unprocessedItems().get(tableName)) {
                unprocessed.add(new DynamoDbWriteRequest(tableName, request));
            }
        }

        LOG.warn("DynamoDB Sink failed to persist {} entries", unprocessed.size());
        numRecordsOutErrorsCounter.inc(unprocessed.size());

        requestResult.accept(unprocessed);
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<DynamoDbWriteRequest> requestEntries,
            Consumer<List<DynamoDbWriteRequest>> requestResult) {
        LOG.warn("DynamoDB Sink failed to persist {} entries", requestEntries.size(), err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (DynamoDbExceptionUtils.isNotRetryableException(err.getCause())) {
            getFatalExceptionCons()
                    .accept(
                            new DynamoDbSinkException(
                                    "Encountered non-recoverable exception", err));
        } else if (failOnError) {
            getFatalExceptionCons()
                    .accept(new DynamoDbSinkException.DynamoDbSinkFailFastException(err));
        } else {
            requestResult.accept(requestEntries);
        }
    }

    @Override
    protected long getSizeInBytes(DynamoDbWriteRequest requestEntry) {
        // dynamodb calculates item size as a sum of all attributes and all values, but doing so on
        // every operation may be too expensive, so this is just an estimate
        return requestEntry.getWriteRequest().toString().getBytes(StandardCharsets.UTF_8).length;
    }
}
