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

package org.apache.flink.streaming.connectors.dynamodb.batch;

import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.batch.key.PrimaryKey;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Collects elements in batches, splits them by table name and calls the supplied batch processor
 * after the configured batch size is reached. De-duplicates elements in the batch based on the
 * deduplication keys. When deduplication is required, the most recent item will be written.
 */
public class BatchCollector {

    private final int batchSize;
    private final Consumer<ProducerWriteRequest<DynamoDbRequest>> batchConsumer;
    private final DynamoDbTablesConfig tableConfig;
    private final TableRequestsContainer container;

    public BatchCollector(
            int batchSize,
            DynamoDbTablesConfig tableConfig,
            Consumer<ProducerWriteRequest<DynamoDbRequest>> batchConsumer) {
        this.batchSize = batchSize;
        requireNonNull(batchConsumer);
        this.batchConsumer = batchConsumer;
        this.tableConfig = tableConfig;
        this.container = new TableRequestsContainer(batchSize);
    }

    public void accumulateAndPromote(DynamoDbRequest request) {
        String tableName = getTableName(request);
        DynamoDbTablesConfig.TableConfig tableConfig = null;
        if (this.tableConfig != null) {
            tableConfig = this.tableConfig.getTableConfig(tableName);
        }

        int containerSize =
                container.addRequest(tableName, PrimaryKey.build(tableConfig, request), request);

        if (containerSize >= batchSize) {
            promote(tableName);
            container.remove(tableName);
        }
    }

    private void promote(String tableName) {
        List<DynamoDbRequest> requests = ImmutableList.copyOf(container.get(tableName).values());
        batchConsumer.accept(
                new ProducerWriteRequest<>(UUID.randomUUID().toString(), tableName, requests));
    }

    public void flush() {
        Iterator<Map.Entry<String, Map<PrimaryKey, DynamoDbRequest>>> it =
                container.entrySet().iterator();
        while (it.hasNext()) {
            promote(it.next().getKey());
            it.remove();
        }
    }

    private static String getTableName(DynamoDbRequest request) {
        if (request instanceof PutItemRequest) {
            return ((PutItemRequest) request).tableName();
        } else if (request instanceof DeleteItemRequest) {
            return ((DeleteItemRequest) request).tableName();
        } else {
            throw new InvalidRequestException(
                    "Not supported request type for request: " + request.toString());
        }
    }

    /** Container to accumulate requests in batches per table. */
    private static class TableRequestsContainer
            extends LinkedHashMap<String, Map<PrimaryKey, DynamoDbRequest>> {

        private final int batchSize;

        public TableRequestsContainer(int batchSize) {
            this.batchSize = batchSize;
        }

        private Map<PrimaryKey, DynamoDbRequest> getPutIfAbsent(String tableName) {
            Map<PrimaryKey, DynamoDbRequest> tableRequests = get(tableName);

            if (tableRequests == null) {
                tableRequests = new LinkedHashMap<>(batchSize);
                put(tableName, tableRequests);
            }
            return tableRequests;
        }

        public int addRequest(String tableName, PrimaryKey requestKey, DynamoDbRequest request) {
            Map<PrimaryKey, DynamoDbRequest> tableRequests = getPutIfAbsent(tableName);
            tableRequests.put(requestKey, request);
            return tableRequests.size();
        }
    }
}
