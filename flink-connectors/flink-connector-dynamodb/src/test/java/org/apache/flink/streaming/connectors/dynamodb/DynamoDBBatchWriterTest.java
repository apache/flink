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

import org.apache.flink.streaming.connectors.dynamodb.batch.BatchWriterAttemptResult;
import org.apache.flink.streaming.connectors.dynamodb.batch.DynamoDbBatchWriter;
import org.apache.flink.streaming.connectors.dynamodb.retry.BatchWriterRetryPolicy;
import org.apache.flink.streaming.connectors.dynamodb.retry.DefaultBatchWriterRetryPolicy;

import org.junit.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests for validation in {@link DynamoDbBatchWriter}. */
public class DynamoDBBatchWriterTest {

    private BatchWriteItemRequest getRequest() {
        return BatchWriteItemRequest.builder()
                .requestItems(
                        Collections.singletonMap(
                                "testTable",
                                Arrays.asList(
                                        WriteRequest.builder().build(),
                                        WriteRequest.builder().build())))
                .build();
    }

    private BatchWriteItemResponse getResponse() {
        return BatchWriteItemResponse.builder().build();
    }

    private BatchWriteItemResponse getResponseWithUnprocessedItems() {
        return BatchWriteItemResponse.builder()
                .unprocessedItems(getRequest().requestItems())
                .build();
    }

    private AwsServiceException getThrottlingException() {
        return AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
                .build();
    }

    private BatchWriterRetryPolicy getShouldNotRetryPolicy() {
        return new BatchWriterRetryPolicy() {
            @Override
            public boolean shouldRetry(BatchWriterAttemptResult attemptResult) {
                return false;
            }

            @Override
            public int getBackOffTime(BatchWriterAttemptResult attemptResult) {
                return 0;
            }
        };
    }

    @Test
    public void testRetriesUnprocessedItems() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        BatchWriterRetryPolicy retryPolicy = new DefaultBatchWriterRetryPolicy();

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(getResponseWithUnprocessedItems())
                .thenReturn(getResponse());

        DynamoDbBatchWriter writer = new DynamoDbBatchWriter(client, retryPolicy, getRequest());
        WriteResponse response = writer.call();

        assertTrue("write was successful", response.isSuccessful());
        assertEquals("was successful after 2 attempts", 2, response.getNumberOfAttempts());
        verify(client, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testRetriesAfterThrottlingError() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        BatchWriterRetryPolicy retryPolicy = new DefaultBatchWriterRetryPolicy();

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(getThrottlingException())
                .thenReturn(getResponse());

        DynamoDbBatchWriter writer = new DynamoDbBatchWriter(client, retryPolicy, getRequest());
        WriteResponse response = writer.call();

        assertTrue("write was successful", response.isSuccessful());
        assertEquals("was successful after 2 attempts", 2, response.getNumberOfAttempts());
        verify(client, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void testNotSuccessfulWhenShouldNotRetryAndUnprocessedItems() {
        DynamoDbClient client = mock(DynamoDbClient.class);

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(getResponseWithUnprocessedItems())
                .thenReturn(getResponseWithUnprocessedItems());

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, getShouldNotRetryPolicy(), getRequest());
        WriteResponse response = writer.call();

        assertFalse("write was not successful", response.isSuccessful());
    }

    @Test
    public void testSuccessfulWhenShouldNotRetryAndAllItemsProcessed() {
        DynamoDbClient client = mock(DynamoDbClient.class);

        when(client.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(getResponse());

        DynamoDbBatchWriter writer =
                new DynamoDbBatchWriter(client, getShouldNotRetryPolicy(), getRequest());
        WriteResponse response = writer.call();

        assertTrue("write was successful", response.isSuccessful());
    }

    @Test
    public void testDoesNotRetryWhenResourceNotFoundException() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        BatchWriterRetryPolicy retryPolicy = new DefaultBatchWriterRetryPolicy();

        when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(getResponse());

        DynamoDbBatchWriter writer = new DynamoDbBatchWriter(client, retryPolicy, getRequest());
        WriteResponse response = writer.call();

        assertFalse("write was not successful", response.isSuccessful());
        assertEquals("attempted once", 1, response.getNumberOfAttempts());
        verify(client, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
    }
}
