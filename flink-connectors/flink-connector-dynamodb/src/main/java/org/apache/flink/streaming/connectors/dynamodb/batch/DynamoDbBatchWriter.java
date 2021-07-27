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

package org.apache.flink.streaming.connectors.dynamodb.batch;

import org.apache.flink.streaming.connectors.dynamodb.DynamoDbProducer;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteResponse;
import org.apache.flink.streaming.connectors.dynamodb.retry.BatchWriterRetryPolicy;
import org.apache.flink.streaming.connectors.dynamodb.retry.DynamoDbExceptionUtils;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A callable that writes batch items to DynamoDB with retries until all items are processed. Every
 * new retry is attempted after backoff time that grows exponentially between retries.
 */
public class DynamoDbBatchWriter implements Callable<ProducerWriteResponse> {

    private final DynamoDbClient client;
    private final ProducerWriteRequest<DynamoDbRequest> producerWriteRequest;
    private final BatchWriterRetryPolicy retryPolicy;
    private final DynamoDbProducer.Listener listener;

    public DynamoDbBatchWriter(
            DynamoDbClient client,
            BatchWriterRetryPolicy retryPolicy,
            DynamoDbProducer.Listener listener,
            ProducerWriteRequest<DynamoDbRequest> request) {
        this.client = client;
        this.listener = listener;
        this.retryPolicy = retryPolicy;
        this.producerWriteRequest = request;
    }

    protected BatchWriteItemRequest createRequest(
            Map<String, ? extends Collection<WriteRequest>> items) {
        return BatchWriteItemRequest.builder().requestItems(items).build();
    }

    /**
     * Batch writes the write request to the DynamoDB endpoint.
     *
     * @return unique id of the batch
     */
    @Override
    public ProducerWriteResponse call() {
        BatchWriteItemRequest request = mapToBatchRequest(producerWriteRequest);
        String requestId = producerWriteRequest.getId();

        listener.beforeWrite(requestId, producerWriteRequest);
        long start = System.nanoTime();
        BatchWriterAttemptResult result = write(request);
        long stop = System.nanoTime();
        long elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(stop - start);

        ProducerWriteResponse response =
                new ProducerWriteResponse(
                        requestId,
                        result.isFinallySuccessful(),
                        result.getAttemptNumber(),
                        result.getException(),
                        elapsedTimeMs);

        if (response.getException() != null) {
            listener.afterWrite(
                    producerWriteRequest.getId(), producerWriteRequest, response.getException());
        } else {
            listener.afterWrite(requestId, producerWriteRequest, response);
        }

        return response;
    }

    /**
     * DynamoDB batch client does not use request classes that extend from DynamoDbRequest. It uses
     * own classes like PutRequest, DeleteRequest, so we have to convert the request here.
     */
    private BatchWriteItemRequest mapToBatchRequest(ProducerWriteRequest<DynamoDbRequest> request) {
        List<WriteRequest> writeRequests = new ArrayList<>();

        for (DynamoDbRequest req : request.getRequests()) {
            if (req instanceof PutItemRequest) {
                PutRequest putRequest =
                        PutRequest.builder().item(((PutItemRequest) req).item()).build();
                writeRequests.add(WriteRequest.builder().putRequest(putRequest).build());
            } else if (req instanceof DeleteItemRequest) {
                DeleteRequest deleteRequest =
                        DeleteRequest.builder().key(((DeleteItemRequest) req).key()).build();
                writeRequests.add(WriteRequest.builder().deleteRequest(deleteRequest).build());
            } else {
                throw new InvalidRequestException(
                        "Not supported request type for request: " + request.toString());
            }
        }
        return BatchWriteItemRequest.builder()
                .requestItems(Collections.singletonMap(request.getTableName(), writeRequests))
                .build();
    }

    /**
     * Retries to write batch items until all items are processed, received a non-retryable
     * exception, or retry limit is reached.
     *
     * @return true if the write was successful after all retries
     */
    public BatchWriterAttemptResult write(BatchWriteItemRequest request) {
        BatchWriteItemResponse result;
        BatchWriteItemRequest req = createRequest(request.requestItems());
        boolean interrupted = false;
        BatchWriterAttemptResult currentAttemptResult = new BatchWriterAttemptResult();

        try {
            do {
                try {
                    currentAttemptResult.setAttemptNumber(
                            currentAttemptResult.getAttemptNumber() + 1);
                    result = client.batchWriteItem(req);

                    if (result.hasUnprocessedItems()) {
                        req = createRequest(result.unprocessedItems());
                        interrupted = sleepFor(retryPolicy.getBackOffTime(currentAttemptResult));
                        currentAttemptResult.setFinallySuccessful(false);
                    } else {
                        currentAttemptResult.setFinallySuccessful(true);
                    }
                } catch (Exception e) {
                    currentAttemptResult.setFinallySuccessful(false);
                    currentAttemptResult.setException(e);

                    if (DynamoDbExceptionUtils.isNotRetryableException(e)) {
                        return currentAttemptResult;
                    }

                    if (DynamoDbExceptionUtils.isThrottlingException(e)) {
                        interrupted = sleepFor(retryPolicy.getBackOffTime(currentAttemptResult));
                    }
                }
            } while (retryPolicy.shouldRetry(currentAttemptResult));

            return currentAttemptResult;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** @return true if sleep was interrupted. */
    private boolean sleepFor(int delay) {
        if (delay <= 0) {
            return false;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            return true;
        }
        return false;
    }
}
