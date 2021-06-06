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

import org.apache.flink.streaming.connectors.dynamodb.batch.DynamoDbBatchWriter;
import org.apache.flink.streaming.connectors.dynamodb.batch.retry.DefaultBatchWriterRetryPolicy;
import org.apache.flink.streaming.connectors.dynamodb.retry.WriterAttemptResult;
import org.apache.flink.streaming.connectors.dynamodb.retry.WriterRetryPolicy;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for validation in {@link
 * org.apache.flink.streaming.connectors.dynamodb.batch.DynamoDbBatchWriter}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DynamoDBBatchWriterTest {

    @Mock DynamoDbProducer.Listener listener;

    @Mock DynamoDbClient client;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    private ProducerWriteRequest<DynamoDbRequest> getRequest(String requestId) {
        return new ProducerWriteRequest<>(
                requestId,
                "testTable",
                Arrays.asList(
                        PutItemRequest.builder().build(), DeleteItemRequest.builder().build()));
    }

    private BatchWriteItemResponse getResponse() {
        return BatchWriteItemResponse.builder().build();
    }

    private BatchWriteItemResponse getResponseWithUnprocessedItems() {
        return BatchWriteItemResponse.builder()
                .unprocessedItems(
                        Collections.singletonMap(
                                "testTable",
                                Arrays.asList(
                                        WriteRequest.builder().build(),
                                        WriteRequest.builder().build())))
                .build();
    }

    private AwsServiceException getThrottlingException() {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
                .build();
    }

    private WriterRetryPolicy getShouldNotRetryPolicy() {
        return new WriterRetryPolicy() {
            @Override
            public boolean shouldRetry(WriterAttemptResult attemptResult) {
                return false;
            }

            @Override
            public int getBackOffTime(WriterAttemptResult attemptResult) {
                return 0;
            }

            @Override
            public boolean isNotRetryableException(Exception e) {
                return false;
            }

            @Override
            public boolean isThrottlingException(Exception e) {
                return false;
            }
        };
    }

    @Test
    public void testRetriesUnprocessedItems() {
        WriterRetryPolicy retryPolicy = new DefaultBatchWriterRetryPolicy();
        ProducerWriteRequest<DynamoDbRequest> request = getRequest("req_id");

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(getResponseWithUnprocessedItems())
                .thenReturn(getResponse());

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, retryPolicy, listener, request);
        ProducerWriteResponse response = writer.call();

        assertEquals(
                "request id of the request and response is equal",
                request.getId(),
                response.getId());
        assertTrue("write was successful", response.isSuccessful());
        assertEquals("was successful after 2 attempts", 2, response.getNumberOfAttempts());
        verify(client, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
        verify(listener, times(1)).beforeWrite(request.getId(), request);
        verify(listener, times(1)).afterWrite(request.getId(), request, response);
    }

    @Test
    public void testRetriesAfterThrottlingError() {
        WriterRetryPolicy retryPolicy = new DefaultBatchWriterRetryPolicy();
        ProducerWriteRequest<DynamoDbRequest> request = getRequest("req_id");

        AwsServiceException exception = getThrottlingException();

        BatchWriteItemResponse response1 = getResponse();
        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(exception)
                .thenReturn(getResponse());

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, retryPolicy, listener, request);
        ProducerWriteResponse response = writer.call();

        assertEquals(
                "request id of the request and response is equal",
                request.getId(),
                response.getId());
        assertTrue("write was successful", response.isSuccessful());
        assertEquals("was successful after 2 attempts", 2, response.getNumberOfAttempts());
        verify(client, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
        verify(listener, times(1)).beforeWrite(request.getId(), request);
        verify(listener, times(1)).afterWrite(request.getId(), request, response);
    }

    @Test
    public void testNotSuccessfulWhenShouldNotRetryAndUnprocessedItems() {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(getResponseWithUnprocessedItems())
                .thenReturn(getResponseWithUnprocessedItems());

        ProducerWriteRequest<DynamoDbRequest> request = getRequest("req_id");

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, getShouldNotRetryPolicy(), listener, request);
        ProducerWriteResponse response = writer.call();

        assertEquals(
                "request id of the request and response is equal",
                request.getId(),
                response.getId());
        assertFalse("write was not successful", response.isSuccessful());
        verify(listener, times(1)).beforeWrite(request.getId(), request);
        verify(listener, times(1)).afterWrite(request.getId(), request, response);
    }

    @Test
    public void testSuccessfulWhenShouldNotRetryAndAllItemsProcessed() {
        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(getResponse());

        ProducerWriteRequest<DynamoDbRequest> request = getRequest("req_id");

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, getShouldNotRetryPolicy(), listener, request);
        ProducerWriteResponse response = writer.call();

        assertEquals(
                "request id of the request and response is equal",
                request.getId(),
                response.getId());
        assertTrue("write was successful", response.isSuccessful());
        verify(listener, times(1)).beforeWrite(request.getId(), request);
        verify(listener, times(1)).afterWrite(request.getId(), request, response);
    }

    @Test
    public void testDoesNotRetryWhenNotRetryableException() {
        WriterRetryPolicy retryPolicy = new DefaultBatchWriterRetryPolicy();

        ProducerWriteRequest<DynamoDbRequest> request = getRequest("req_id");
        Exception exception = ResourceNotFoundException.builder().build();

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(exception)
                .thenReturn(getResponse());

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, retryPolicy, listener, request);
        ProducerWriteResponse response = writer.call();

        assertEquals(
                "request id of the request and response is equal",
                request.getId(),
                response.getId());
        assertFalse("write was not successful", response.isSuccessful());
        assertEquals("attempted once", 1, response.getNumberOfAttempts());
        verify(client, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
        verify(listener, times(1)).beforeWrite(request.getId(), request);
        verify(listener, times(1)).afterWrite(request.getId(), request, exception);
    }
}
