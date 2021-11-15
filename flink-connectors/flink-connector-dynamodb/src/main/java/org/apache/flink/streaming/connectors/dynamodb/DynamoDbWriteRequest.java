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

import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.Serializable;
import java.util.Objects;

/**
 * represents a single DynamoDb {@link WriteRequest}. contains the name of the DynamoDb table name
 * to write to as well as the {@link WriteRequest}
 */
public class DynamoDbWriteRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final WriteRequest writeRequest;

    public DynamoDbWriteRequest(String tableName, WriteRequest writeRequest) {
        this.tableName = tableName;
        this.writeRequest = writeRequest;
    }

    public String getTableName() {
        return tableName;
    }

    public WriteRequest getWriteRequest() {
        return writeRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbWriteRequest that = (DynamoDbWriteRequest) o;
        return Objects.equals(tableName, that.tableName)
                && Objects.equals(writeRequest, that.writeRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, writeRequest);
    }
}
