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

import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Unit tests for {@link BatchCollectorTest}. */
@RunWith(MockitoJUnitRunner.class)
public class BatchCollectorTest {

    @Mock private Consumer<ProducerWriteRequest<DynamoDbRequest>> consumer;

    @Captor private ArgumentCaptor<ProducerWriteRequest> batchCaptor;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    private static final String PARTITION_KEY_NAME = "part_key_name";
    private static final String SORT_KEY_NAME = "sort_key_name";

    private ImmutableMap<String, AttributeValue> createItemValues(
            String partitionKeyValue, String sortKeyValue) {
        return ImmutableMap.of(
                PARTITION_KEY_NAME,
                AttributeValue.builder().s(partitionKeyValue).build(),
                SORT_KEY_NAME,
                AttributeValue.builder().s(sortKeyValue).build());
    }

    public PutItemRequest createPutItemRequest(
            String tableName, String primaryKeyValue, String sortKeyValue) {
        ImmutableMap<String, AttributeValue> itemValues =
                createItemValues(primaryKeyValue, sortKeyValue);

        return PutItemRequest.builder().tableName(tableName).item(itemValues).build();
    }

    @Test
    public void testBatchPromoted() {
        String tableName = "testTable";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableName, PARTITION_KEY_NAME, SORT_KEY_NAME);

        BatchCollector collector = new BatchCollector(3, tableConfig, consumer);
        collector.accumulateAndPromote(createPutItemRequest(tableName, "1", "1"));
        collector.accumulateAndPromote(createPutItemRequest(tableName, "2", "2"));
        collector.accumulateAndPromote(createPutItemRequest(tableName, "3", "3"));

        verify(consumer, times(1)).accept(batchCaptor.capture());
        ProducerWriteRequest<PutItemRequest> capturedRequest = batchCaptor.getValue();

        assertEquals("promoted batch with 3 requests", 3, capturedRequest.getRequests().size());
        for (PutItemRequest request : capturedRequest.getRequests()) {
            assertEquals(
                    "same table name for all requests in the batch",
                    tableName,
                    request.tableName());
        }
    }

    @Test
    public void testFlushOutstandingRecords() {
        String tableOne = "testTable1";
        String tableTwo = "testTable2";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableOne, PARTITION_KEY_NAME, SORT_KEY_NAME);
        tableConfig.addTableConfig(tableTwo, PARTITION_KEY_NAME, SORT_KEY_NAME);

        BatchCollector collector = new BatchCollector(3, tableConfig, consumer);
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "1"));
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "2"));
        collector.accumulateAndPromote(createPutItemRequest(tableTwo, "2", "1"));
        collector.flush();

        verify(consumer, times(2)).accept(batchCaptor.capture());

        int numOfRequests =
                batchCaptor.getAllValues().stream()
                        .mapToInt(request -> request.getRequests().size())
                        .sum();

        assertEquals("total number of flushed requests", 3, numOfRequests);
    }

    @Test
    public void testRequestNotDeduplicatedWhenNoTableConfig() {
        String tableOne = "testTable1";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();

        BatchCollector collector = new BatchCollector(2, tableConfig, consumer);
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "1"));
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "1"));

        verify(consumer, times(1)).accept(batchCaptor.capture());

        int numOfRequests =
                batchCaptor.getAllValues().stream()
                        .mapToInt(request -> request.getRequests().size())
                        .sum();

        assertEquals("number of requests without deduplication configuration", 2, numOfRequests);
    }

    @Test
    public void testDeduplicatedOnCompositeKey() {
        String tableOne = "testTable1";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableOne, PARTITION_KEY_NAME, SORT_KEY_NAME);

        BatchCollector collector = new BatchCollector(3, tableConfig, consumer);

        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "1"));
        collector.accumulateAndPromote(
                createPutItemRequest(tableOne, "1", "1")); // will be deduplicated
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "2", "1"));
        collector.flush();

        verify(consumer, times(1)).accept(batchCaptor.capture());

        int numOfRequests =
                batchCaptor.getAllValues().stream()
                        .mapToInt(request -> request.getRequests().size())
                        .sum();

        assertEquals("total number of requests after deduplication", 2, numOfRequests);
    }

    @Test
    public void testDeduplicatedOnPartitionKey() {
        String tableOne = "testTable1";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableOne, PARTITION_KEY_NAME);

        BatchCollector collector = new BatchCollector(3, tableConfig, consumer);
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", null));
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", null));
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "2", null));

        collector.flush();

        verify(consumer, times(1)).accept(batchCaptor.capture());

        int numOfRequests =
                batchCaptor.getAllValues().stream()
                        .mapToInt(request -> request.getRequests().size())
                        .sum();
        assertEquals(
                "total number of requests after deduplication on partition key", 2, numOfRequests);
    }

    @Test
    public void testPromotedPerTable() {
        String tableOne = "testTable1";
        String tableTwo = "testTable2";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableOne, PARTITION_KEY_NAME, SORT_KEY_NAME);
        tableConfig.addTableConfig(tableTwo, PARTITION_KEY_NAME, SORT_KEY_NAME);

        BatchCollector collector = new BatchCollector(2, tableConfig, consumer);
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "1"));
        collector.accumulateAndPromote(createPutItemRequest(tableOne, "1", "2"));
        collector.accumulateAndPromote(createPutItemRequest(tableTwo, "2", "1"));

        // second promote with the entities for tableOne
        verify(consumer, times(1)).accept(batchCaptor.capture());

        ProducerWriteRequest<PutItemRequest> capturedRequestOne = batchCaptor.getValue();

        for (PutItemRequest request : capturedRequestOne.getRequests()) {
            assertEquals("table one on the first batch promote", tableOne, request.tableName());
        }

        collector.accumulateAndPromote(createPutItemRequest(tableTwo, "2", "2"));

        // second promote with the entities for tableTwo
        verify(consumer, times(2)).accept(batchCaptor.capture());

        ProducerWriteRequest<PutItemRequest> capturedRequestTwo = batchCaptor.getValue();

        for (PutItemRequest request : capturedRequestTwo.getRequests()) {
            assertEquals("table two on the second batch promote", tableTwo, request.tableName());
        }
    }
}
