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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.UUID;

/** Represents DynamoDB primary key. */
public class PrimaryKey {

    private final String partitionKeyValue;
    @Nullable private final String sortKeyValue;

    private PrimaryKey(String partitionKeyValue) {
        this.partitionKeyValue = partitionKeyValue;
        this.sortKeyValue = null;
    }

    private PrimaryKey(String partitionKeyValue, @Nullable String sortKeyValue) {
        this.partitionKeyValue = partitionKeyValue;
        this.sortKeyValue = sortKeyValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PrimaryKey that = (PrimaryKey) o;

        return new EqualsBuilder()
                .append(partitionKeyValue, that.partitionKeyValue)
                .append(sortKeyValue, that.sortKeyValue)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(partitionKeyValue)
                .append(sortKeyValue)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "PrimaryKey{"
                + "partitionKeyValue='"
                + partitionKeyValue
                + '\''
                + ", sortKeyValue='"
                + sortKeyValue
                + '\''
                + '}';
    }

    public static PrimaryKey build(DynamoDbTablesConfig.TableConfig config, WriteRequest request) {
        if (config != null) {
            Map<String, AttributeValue> requestItems = getRequestItems(request);

            AttributeValue partitionKeyAttributeValue =
                    requestItems.get(config.getPartitionKeyName());
            AttributeValue sortKeyAttributeValue = requestItems.get(config.getSortKeyName());

            if (config.getPartitionKeyName() != null && partitionKeyAttributeValue == null) {
                throw new InvalidRequestException(
                        "Request "
                                + request.toString()
                                + " does not contain partition key "
                                + config.getPartitionKeyName());
            }

            if (config.getSortKeyName() != null && sortKeyAttributeValue == null) {
                throw new InvalidRequestException(
                        "Request "
                                + request.toString()
                                + " does not contain sort key "
                                + config.getSortKeyName());
            }

            if (partitionKeyAttributeValue != null && sortKeyAttributeValue != null) {
                return new PrimaryKey(
                        getKeyValue(partitionKeyAttributeValue),
                        getKeyValue(sortKeyAttributeValue));
            } else if (partitionKeyAttributeValue != null) {
                return new PrimaryKey(getKeyValue(partitionKeyAttributeValue));
            }
        }

        // fake key, because no dynamodb table configuration provided
        return new PrimaryKey(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    /**
     * Returns string value of a partition key attribute. Each primary key attribute must be defined
     * as type String, Number, or binary as per DynamoDB specification.
     */
    private static String getKeyValue(AttributeValue value) {
        StringBuilder builder = new StringBuilder();

        if (value.n() != null) {
            builder.append(value.n());
        }

        if (value.s() != null) {
            builder.append(value.s());
        }

        if (value.b() != null) {
            builder.append(value.b().asUtf8String());
        }

        return builder.toString();
    }

    private static Map<String, AttributeValue> getRequestItems(WriteRequest request) {
        if (request.putRequest() != null) {
            if (request.putRequest().hasItem()) {
                return request.putRequest().item();
            } else {
                throw new InvalidRequestException(
                        "PutItemRequest " + request.toString() + " does not contain request items");
            }
        } else if (request.deleteRequest() != null) {
            if (request.deleteRequest().hasKey()) {
                return request.deleteRequest().key();
            } else {
                throw new InvalidRequestException(
                        "DeleteItemRequest "
                                + request.toString()
                                + " does not contain request key");
            }
        } else {
            throw new InvalidRequestException("Empty write request" + request.toString());
        }
    }
}
