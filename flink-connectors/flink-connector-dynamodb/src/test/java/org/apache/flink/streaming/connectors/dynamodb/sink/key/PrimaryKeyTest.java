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

package org.apache.flink.streaming.connectors.dynamodb.sink.key;

import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.sink.InvalidRequestException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Unit tests for {@link PrimaryKey}. */
public class PrimaryKeyTest {

    private static final String PARTITION_KEY_NAME = "part_key_name";
    private static final String SORT_KEY_NAME = "sort_key_name";

    private ImmutableMap<String, AttributeValue> createItemValues() {
        return ImmutableMap.of(
                PARTITION_KEY_NAME,
                AttributeValue.builder()
                        .s("123")
                        .n("456")
                        .b(SdkBytes.fromString("789", StandardCharsets.UTF_8))
                        .build(),
                SORT_KEY_NAME,
                AttributeValue.builder().s("101112").build(),
                "some_item",
                AttributeValue.builder().bool(false).build());
    }

    public WriteRequest createPutItemRequest(Map<String, AttributeValue> itemValues) {
        return WriteRequest.builder()
                .putRequest(PutRequest.builder().item(itemValues).build())
                .build();
    }

    public WriteRequest createDeleteItemRequest(Map<String, AttributeValue> itemValues) {
        return WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder().key(itemValues).build())
                .build();
    }

    @Test
    public void testPartitionKeysOfTwoDifferentRequestsEqual() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);

        PrimaryKey putRequestKey =
                PrimaryKey.build(config, createPutItemRequest(createItemValues()));
        PrimaryKey deleteRequestKey =
                PrimaryKey.build(config, createDeleteItemRequest(createItemValues()));

        Assert.assertEquals(putRequestKey, deleteRequestKey);
        Assert.assertEquals(putRequestKey.hashCode(), deleteRequestKey.hashCode());
    }

    @Test
    public void testCompositeKeysOfTwoDifferentRequestsEqual() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME, SORT_KEY_NAME);

        PrimaryKey putRequestKey =
                PrimaryKey.build(config, createPutItemRequest(createItemValues()));
        PrimaryKey deleteRequestKey =
                PrimaryKey.build(config, createDeleteItemRequest(createItemValues()));

        Assert.assertEquals(putRequestKey, deleteRequestKey);
        Assert.assertEquals(putRequestKey.hashCode(), deleteRequestKey.hashCode());
    }

    @Test(expected = InvalidRequestException.class)
    public void testExceptOnEmptyRequest() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);

        WriteRequest request =
                WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(new HashMap<>()).build())
                        .build();

        PrimaryKey.build(config, createPutItemRequest(new HashMap<>()));
    }

    @Test(expected = InvalidRequestException.class)
    public void testExceptWhenNoPartitionKey() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);
        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of("some_item", AttributeValue.builder().bool(false).build());

        PrimaryKey.build(config, createPutItemRequest(itemValues));
    }

    @Test(expected = InvalidRequestException.class)
    public void testExceptWhenNoPartitionKeyCompositeKey() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME, SORT_KEY_NAME);

        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of(
                        SORT_KEY_NAME,
                        AttributeValue.builder().s("101112").build(),
                        "some_item",
                        AttributeValue.builder().bool(false).build());

        PrimaryKey.build(config, createPutItemRequest(itemValues));
    }

    @Test(expected = InvalidRequestException.class)
    public void testExceptWhenNoSortKey() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME, SORT_KEY_NAME);

        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of(
                        PARTITION_KEY_NAME,
                        AttributeValue.builder().s("101112").build(),
                        "some_item",
                        AttributeValue.builder().bool(false).build());

        PrimaryKey.build(config, createPutItemRequest(itemValues));
    }
}
