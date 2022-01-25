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
import org.apache.flink.streaming.connectors.dynamodb.sink.key.PrimaryKey;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Container to accumulate requests in batches per table. De-duplicates batch request entities as
 * per PrimaryKey definition. DynamoDB Batch API rejects the whole batch request if the request
 * contains at least two items with identical hash and range keys (which essentially is two put
 * operations).
 */
class TableRequestsContainer {

    private final DynamoDbTablesConfig tablesConfig;
    private final LinkedHashMap<String, Map<PrimaryKey, WriteRequest>> container;

    public TableRequestsContainer(DynamoDbTablesConfig tablesConfig) {
        this.tablesConfig = tablesConfig;
        this.container = new LinkedHashMap<>();
    }

    public void put(DynamoDbWriteRequest request) {
        Map<PrimaryKey, WriteRequest> tableRequests =
                container.computeIfAbsent(request.getTableName(), k -> new LinkedHashMap<>());
        tableRequests.put(
                PrimaryKey.build(
                        tablesConfig.getTableConfig(request.getTableName()),
                        request.getWriteRequest()),
                request.getWriteRequest());
    }

    /** Reduces requests de-duplicated by table name and primary key to table write requests map. */
    public Map<String, List<WriteRequest>> getRequestItems() {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        for (String tableName : container.keySet()) {
            requestItems.put(tableName, ImmutableList.copyOf(container.get(tableName).values()));
        }
        return requestItems;
    }
}
