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

import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TableRequestsContainer}. */
public class TableRequestsContainerTest {

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

    public DynamoDbWriteRequest createPutItemRequest(
            String tableName, String primaryKeyValue, String sortKeyValue) {
        ImmutableMap<String, AttributeValue> itemValues =
                createItemValues(primaryKeyValue, sortKeyValue);
        return new DynamoDbWriteRequest(
                tableName,
                WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(itemValues).build())
                        .build());
    }

    public int getNumberOfItems(TableRequestsContainer container) {
        return container.getRequestItems().keySet().stream()
                .mapToInt(tableName -> container.getRequestItems().get(tableName).size())
                .sum();
    }

    @Test
    public void testRequestNotDeduplicatedWhenNoTableConfig() {
        String tableOne = "testTable1";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();

        TableRequestsContainer container = new TableRequestsContainer(tableConfig);
        container.put(createPutItemRequest(tableOne, "1", "1"));
        container.put(createPutItemRequest(tableOne, "1", "1"));

        assertEquals(
                "number of requests without deduplication configuration",
                2,
                getNumberOfItems(container));
    }

    @Test
    public void testDeduplicatedOnCompositeKey() {
        String tableOne = "testTable1";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableOne, PARTITION_KEY_NAME, SORT_KEY_NAME);

        TableRequestsContainer container = new TableRequestsContainer(tableConfig);

        container.put(createPutItemRequest(tableOne, "1", "1"));
        container.put(createPutItemRequest(tableOne, "1", "1")); // to be deduplicated
        container.put(createPutItemRequest(tableOne, "2", "1"));

        assertEquals(
                "total number of requests after deduplication", 2, getNumberOfItems(container));
    }

    @Test
    public void testDeduplicatedOnPartitionKey() {
        String tableOne = "testTable1";
        DynamoDbTablesConfig tableConfig = new DynamoDbTablesConfig();
        tableConfig.addTableConfig(tableOne, PARTITION_KEY_NAME);

        TableRequestsContainer container = new TableRequestsContainer(tableConfig);
        container.put(createPutItemRequest(tableOne, "1", null));
        container.put(createPutItemRequest(tableOne, "1", null));
        container.put(createPutItemRequest(tableOne, "2", null));

        assertEquals(
                "total number of requests after deduplication on partition key",
                2,
                getNumberOfItems(container));
    }
}
