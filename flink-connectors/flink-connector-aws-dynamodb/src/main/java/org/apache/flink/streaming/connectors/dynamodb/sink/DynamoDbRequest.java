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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Represents a DynamoDb write request.
 *
 * <p>Only <b>one</b> of the fields dynamoDbPutRequest or dynamoDbDeleteRequest should be set.
 */
@PublicEvolving
public class DynamoDbRequest implements Serializable {

    private final String tableName;
    private final DynamoDbPutRequest dynamoDbPutRequest;
    private final DynamoDbDeleteRequest dynamoDbDeleteRequest;

    private DynamoDbRequest(
            String tableName,
            DynamoDbPutRequest dynamoDbPutRequest,
            DynamoDbDeleteRequest dynamoDbDeleteRequest) {
        this.tableName = tableName;
        this.dynamoDbPutRequest = dynamoDbPutRequest;
        this.dynamoDbDeleteRequest = dynamoDbDeleteRequest;
    }

    public String tableName() {
        return tableName;
    }

    public DynamoDbPutRequest putRequest() {
        return dynamoDbPutRequest;
    }

    public DynamoDbDeleteRequest deleteRequest() {
        return dynamoDbDeleteRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbRequest that = (DynamoDbRequest) o;
        return Objects.equals(tableName, that.tableName)
                && Objects.equals(dynamoDbPutRequest, that.dynamoDbPutRequest)
                && Objects.equals(dynamoDbDeleteRequest, that.dynamoDbDeleteRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, dynamoDbPutRequest, dynamoDbDeleteRequest);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder. */
    public static class Builder {
        private String tableName;
        private DynamoDbPutRequest dynamoDbPutRequest;
        private DynamoDbDeleteRequest dynamoDbDeleteRequest;

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder putRequest(DynamoDbPutRequest dynamoDbPutRequest) {
            this.dynamoDbPutRequest = dynamoDbPutRequest;
            return this;
        }

        public Builder deleteRequest(DynamoDbDeleteRequest dynamoDbDeleteRequest) {
            this.dynamoDbDeleteRequest = dynamoDbDeleteRequest;
            return this;
        }

        public DynamoDbRequest build() {
            checkNotNull(tableName);
            checkState(dynamoDbPutRequest != null || dynamoDbDeleteRequest != null);
            return new DynamoDbRequest(tableName, dynamoDbPutRequest, dynamoDbDeleteRequest);
        }
    }
}
