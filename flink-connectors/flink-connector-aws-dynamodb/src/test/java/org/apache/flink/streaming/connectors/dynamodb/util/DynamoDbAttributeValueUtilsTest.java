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

package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbAttributeValue;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.Test;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamoDbAttributeValueUtils}. */
public class DynamoDbAttributeValueUtilsTest {

    @Test
    public void testToAttributeValueMap() {
        Map<String, DynamoDbAttributeValue> dynamoDbAttributeValueMap = new HashMap<>();
        dynamoDbAttributeValueMap.put("string", DynamoDbAttributeValue.builder().s("a").build());
        dynamoDbAttributeValueMap.put("number", DynamoDbAttributeValue.builder().n("1").build());
        dynamoDbAttributeValueMap.put("bool", DynamoDbAttributeValue.builder().bool(true).build());
        dynamoDbAttributeValueMap.put(
                "binary", DynamoDbAttributeValue.builder().b(new byte[] {1, 2}).build());
        dynamoDbAttributeValueMap.put("null", DynamoDbAttributeValue.builder().nul(true).build());
        dynamoDbAttributeValueMap.put(
                "stringSet",
                DynamoDbAttributeValue.builder()
                        .ss(new HashSet<>(Arrays.asList("a", "b", "c")))
                        .build());
        dynamoDbAttributeValueMap.put(
                "numberSet",
                DynamoDbAttributeValue.builder()
                        .ns(new HashSet<>(Arrays.asList("1", "2", "3")))
                        .build());
        dynamoDbAttributeValueMap.put(
                "binarySet",
                DynamoDbAttributeValue.builder()
                        .bs(new HashSet<>(Arrays.asList(new byte[] {1, 2}, new byte[] {3})))
                        .build());
        dynamoDbAttributeValueMap.put(
                "list",
                DynamoDbAttributeValue.builder()
                        .l(
                                Arrays.asList(
                                        DynamoDbAttributeValue.builder().s("a").build(),
                                        DynamoDbAttributeValue.builder().n("1").build()))
                        .build());
        dynamoDbAttributeValueMap.put(
                "map",
                DynamoDbAttributeValue.builder()
                        .m(
                                ImmutableMap.of(
                                        "string",
                                        DynamoDbAttributeValue.builder().s("b").build(),
                                        "number",
                                        DynamoDbAttributeValue.builder().n("2").build()))
                        .build());
        Map<String, AttributeValue> attributeValueMap =
                DynamoDbAttributeValueUtils.toAttributeValueMap(dynamoDbAttributeValueMap);
        assertThat(attributeValueMap).hasSameSizeAs(dynamoDbAttributeValueMap);
        assertThat(attributeValueMap.get("string").s()).isEqualTo("a");
        assertThat(attributeValueMap.get("number").n()).isEqualTo("1");
        assertThat(attributeValueMap.get("bool").bool()).isEqualTo(true);
        assertThat(attributeValueMap.get("binary").b().asByteArrayUnsafe())
                .isEqualTo(new byte[] {1, 2});
        assertThat(attributeValueMap.get("null").nul()).isEqualTo(true);
        assertThat(attributeValueMap.get("stringSet").ss())
                .containsAll(Arrays.asList("a", "b", "c"));
        assertThat(attributeValueMap.get("numberSet").ns())
                .containsAll(Arrays.asList("1", "2", "3"));
        assertThat(attributeValueMap.get("binarySet").bs())
                .map(BytesWrapper::asByteArrayUnsafe)
                .containsAll(Arrays.asList(new byte[] {1, 2}, new byte[] {3}));
        assertThat(attributeValueMap.get("list").l().get(0).s()).isEqualTo("a");
        assertThat(attributeValueMap.get("list").l().get(1).n()).isEqualTo("1");
        assertThat(attributeValueMap.get("map").m().get("string").s()).isEqualTo("b");
        assertThat(attributeValueMap.get("map").m().get("number").n()).isEqualTo("2");
    }
}
