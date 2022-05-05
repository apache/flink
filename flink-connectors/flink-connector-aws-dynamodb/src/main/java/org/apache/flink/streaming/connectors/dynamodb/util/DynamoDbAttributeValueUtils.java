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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbAttributeValue;
import org.apache.flink.streaming.connectors.dynamodb.sink.DynamoDbRequest;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Utils class to convert this sink representation of AWS sdk2 dynamodb classes. */
@Internal
public class DynamoDbAttributeValueUtils {

    public static WriteRequest toWriteRequest(DynamoDbRequest dynamoDbRequest) {
        if (dynamoDbRequest.putRequest() != null) {
            return WriteRequest.builder()
                    .putRequest(
                            PutRequest.builder()
                                    .item(toAttributeValueMap(dynamoDbRequest.putRequest().item()))
                                    .build())
                    .build();
        } else if (dynamoDbRequest.deleteRequest() != null) {
            return WriteRequest.builder()
                    .deleteRequest(
                            DeleteRequest.builder()
                                    .key(toAttributeValueMap(dynamoDbRequest.deleteRequest().key()))
                                    .build())
                    .build();
        }
        throw new IllegalArgumentException(
                "WriteRequest must either contain a PutRequest or a DeleteRequest");
    }

    /**
     * Converts a DynamoDbAttributeValue into the low-level {@link AttributeValue} representation.
     *
     * @param value the given DynamoDbAttributeValue
     * @return a low level implementation or null if value is null
     */
    public static AttributeValue toAttributeValue(DynamoDbAttributeValue value) {
        if (value == null) {
            return null;
        }
        if (value.nul() != null) {
            return AttributeValue.builder().nul(value.nul()).build();
        } else if (value.bool() != null) {
            return AttributeValue.builder().bool(value.bool()).build();
        } else if (value.s() != null) {
            return AttributeValue.builder().s(value.s()).build();
        } else if (value.n() != null) {
            return AttributeValue.builder().n(value.n()).build();
        } else if (value.b() != null) {
            return AttributeValue.builder().b(SdkBytes.fromByteArrayUnsafe(value.b())).build();
        } else if (value.ss() != null) {
            return AttributeValue.builder().ss(value.ss()).build();
        } else if (value.ns() != null) {
            return AttributeValue.builder().ns(value.ns()).build();
        } else if (value.bs() != null) {
            Set<byte[]> bs = value.bs();
            Set<SdkBytes> bytesSet = new HashSet<>(bs.size());
            for (byte[] b : bs) {
                bytesSet.add(SdkBytes.fromByteArrayUnsafe(b));
            }
            return AttributeValue.builder().bs(bytesSet).build();
        } else if (value.l() != null) {
            List<DynamoDbAttributeValue> l = value.l();
            List<AttributeValue> attributeValueList = new ArrayList<>(l.size());
            for (DynamoDbAttributeValue dynamoDbAttributeValue : l) {
                attributeValueList.add(toAttributeValue(dynamoDbAttributeValue));
            }
            return AttributeValue.builder().l(attributeValueList).build();
        } else if (value.m() != null) {
            Map<String, DynamoDbAttributeValue> m = value.m();
            Map<String, AttributeValue> attributeValueMap = toAttributeValueMap(m);
            return AttributeValue.builder().m(attributeValueMap).build();
        } else {
            throw new IllegalArgumentException(
                    "DynamoDbAttributeValue value must not be empty: " + value);
        }
    }

    /**
     * Converts a map of string to DynamoDbAttributeValue objects into the low-level representation;
     * or null if the input is null.
     */
    public static Map<String, AttributeValue> toAttributeValueMap(
            Map<String, DynamoDbAttributeValue> dynamoDbAttributeValueMap) {
        if (dynamoDbAttributeValueMap == null) {
            return null;
        }
        Map<String, AttributeValue> attributeValueMap =
                new HashMap<>(dynamoDbAttributeValueMap.size());
        for (Map.Entry<String, DynamoDbAttributeValue> entry :
                dynamoDbAttributeValueMap.entrySet()) {
            attributeValueMap.put(entry.getKey(), toAttributeValue(entry.getValue()));
        }
        return attributeValueMap;
    }
}
