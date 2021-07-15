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

package org.apache.flink.streaming.connectors.dynamodb.batch.key;

import org.apache.flink.streaming.connectors.dynamodb.batch.InvalidRequestException;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

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

    public PutItemRequest createPutItemRequest() {
        ImmutableMap<String, AttributeValue> itemValues = createItemValues();

        return PutItemRequest.builder().item(itemValues).build();
    }

    public PutItemRequest createDeleteItemRequest() {
        ImmutableMap<String, AttributeValue> itemValues = createItemValues();

        return PutItemRequest.builder().item(itemValues).build();
    }

    @Test
    public void testPartitionKeysOfTwoDifferentRequestsEqual() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);

        PrimaryKey putRequestKey = PrimaryKey.build(config, createPutItemRequest());
        PrimaryKey deleteRequestKey = PrimaryKey.build(config, createDeleteItemRequest());

        assertEquals(putRequestKey, deleteRequestKey);
        assertEquals(putRequestKey.hashCode(), deleteRequestKey.hashCode());
    }

    @Test
    public void testCompositeKeysOfTwoDifferentRequestsEqual() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME, SORT_KEY_NAME);

        PrimaryKey putRequestKey = PrimaryKey.build(config, createPutItemRequest());
        PrimaryKey deleteRequestKey = PrimaryKey.build(config, createDeleteItemRequest());

        assertEquals(putRequestKey, deleteRequestKey);
        assertEquals(putRequestKey.hashCode(), deleteRequestKey.hashCode());
    }

    @Test(expected = InvalidRequestException.class)
    public void testExceptOnEmptyRequest() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);

        DynamoDbRequest putRequest = PutItemRequest.builder().item(new HashMap<>()).build();

        PrimaryKey.build(config, putRequest);
    }

    @Test(expected = InvalidRequestException.class)
    public void testExceptWhenNoPartitionKey() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);
        ImmutableMap<String, AttributeValue> itemValues =
                ImmutableMap.of("some_item", AttributeValue.builder().bool(false).build());

        DynamoDbRequest putRequest = PutItemRequest.builder().item(itemValues).build();

        PrimaryKey.build(config, putRequest);
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

        DynamoDbRequest putRequest = PutItemRequest.builder().item(itemValues).build();

        PrimaryKey.build(config, putRequest);
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

        DynamoDbRequest putRequest = PutItemRequest.builder().item(itemValues).build();

        PrimaryKey.build(config, putRequest);
    }

    @Test(expected = InvalidRequestException.class)
    public void testUpdateRequestNotSupported() {
        DynamoDbTablesConfig.TableConfig config =
                new DynamoDbTablesConfig.TableConfig(PARTITION_KEY_NAME);

        DynamoDbRequest putRequest = UpdateItemRequest.builder().key(new HashMap<>()).build();

        PrimaryKey.build(config, putRequest);
    }
}
