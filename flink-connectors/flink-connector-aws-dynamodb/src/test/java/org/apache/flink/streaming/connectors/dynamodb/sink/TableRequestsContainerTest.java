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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tests for {@link TableRequestsContainer}. */
public class TableRequestsContainerTest {
    @Test
    public void testRequestNotDeduplicatedWhenNoTableConfig() {
        new Scenario()
                .witItem("table", ImmutableMap.of("pk", "1", "sk", "1"))
                .witItem("table", ImmutableMap.of("pk", "1", "sk", "1"))
                .withDescription("number of requests without deduplication configuration")
                .withExpectedNumberOfItems(2)
                .runScenario();
    }

    @Test
    public void testDeduplicatedOnCompositeKey() {
        new Scenario()
                .withTableConfig("tableOne", "pk", "sk")
                .witItem("tableOne", ImmutableMap.of("pk", "1", "sk", "1"))
                // to be deduplicated
                .witItem("tableOne", ImmutableMap.of("pk", "1", "sk", "1"))
                .witItem("tableOne", ImmutableMap.of("pk", "2", "sk", "1"))
                .withDescription("total number of requests after deduplication")
                .withExpectedNumberOfItems(2)
                .runScenario();
    }

    @Test
    public void testDeduplicatedOnPartitionKey() {
        new Scenario()
                .withTableConfig("tableOne", "pk")
                .witItem("tableOne", ImmutableMap.of("pk", "1", "sk", "1"))
                // to be deduplicated
                .witItem("tableOne", ImmutableMap.of("pk", "1", "sk", "1"))
                .witItem("tableOne", ImmutableMap.of("pk", "2", "sk", "1"))
                .withDescription(
                        "total number of requests after deduplication on partition key only")
                .withExpectedNumberOfItems(2)
                .runScenario();
    }

    @Test
    public void testNotDeduplicatedWhenOnDifferentTable() {
        new Scenario()
                .withTableConfig("tableOne", "pk", "sk")
                .withTableConfig("tableTwo", "pk", "sk")
                .witItem("tableOne", ImmutableMap.of("pk", "1", "sk", "1"))
                .witItem("tableOne", ImmutableMap.of("pk", "2", "sk", "1"))
                .witItem("tableTwo", ImmutableMap.of("pk", "1", "sk", "1"))
                .withDescription(
                        "requests should not be deduplecated if belong to different tables")
                .withExpectedNumberOfItems(3)
                .runScenario();
    }

    private class Scenario {
        private String description;
        private int expectedNumberOfItems;
        private final DynamoDbTablesConfig tablesConfig = new DynamoDbTablesConfig();
        private final List<DynamoDbWriteRequest> requests = new ArrayList<>();

        public void runScenario() {
            TableRequestsContainer container = new TableRequestsContainer(tablesConfig);
            requests.forEach(container::put);

            int numberOfItemsInContainer =
                    container.getRequestItems().keySet().stream()
                            .mapToInt(
                                    tableName -> container.getRequestItems().get(tableName).size())
                            .sum();

            Assertions.assertThat(numberOfItemsInContainer)
                    .as(description)
                    .isEqualTo(expectedNumberOfItems);
        }

        public DynamoDbWriteRequest createPutItemRequest(
                String tableName, Map<String, String> attributes) {
            Map<String, AttributeValue> items =
                    attributes.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> AttributeValue.builder().s(e.getValue()).build()));

            return new DynamoDbWriteRequest(
                    tableName,
                    WriteRequest.builder()
                            .putRequest(PutRequest.builder().item(items).build())
                            .build());
        }

        public Scenario withTableConfig(
                String tableName, String partitionKeyName, String sortKeyName) {
            this.tablesConfig.addTableConfig(tableName, partitionKeyName, sortKeyName);
            return this;
        }

        public Scenario withTableConfig(String tableName, String partitionKeyName) {
            this.tablesConfig.addTableConfig(tableName, partitionKeyName);
            return this;
        }

        public Scenario witItem(String tableName, Map<String, String> attributes) {
            this.requests.add(createPutItemRequest(tableName, attributes));
            return this;
        }

        public Scenario withExpectedNumberOfItems(int expectedNumberOfItems) {
            this.expectedNumberOfItems = expectedNumberOfItems;
            return this;
        }

        public Scenario withDescription(String description) {
            this.description = description;
            return this;
        }
    }
}
