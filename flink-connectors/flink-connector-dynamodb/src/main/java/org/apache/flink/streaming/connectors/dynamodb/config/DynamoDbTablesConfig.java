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

package org.apache.flink.streaming.connectors.dynamodb.config;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Represents DynamoDB tables configuration. */
@PublicEvolving
public class DynamoDbTablesConfig {

    private final Map<String, TableConfig> perTableConfig;

    public DynamoDbTablesConfig() {
        this.perTableConfig = new HashMap<>();
    }

    public void addTableConfig(String tableName, String partitionKeyName, String sortKeyName) {
        this.perTableConfig.put(tableName, new TableConfig(partitionKeyName, sortKeyName));
    }

    public void addTableConfig(String tableName, String partitionKeyName) {
        this.perTableConfig.put(tableName, new TableConfig(partitionKeyName));
    }

    public TableConfig getTableConfig(String tableName) {
        return perTableConfig.get(tableName);
    }

    /** DynamoDB table configuration. */
    public static class TableConfig {

        private final String partitionKeyName;

        @Nullable private final String sortKeyName;

        public TableConfig(String partitionKeyName, String sortKeyName) {
            this.partitionKeyName = partitionKeyName;
            this.sortKeyName = sortKeyName;
        }

        public TableConfig(String partitionKeyName) {
            this.partitionKeyName = partitionKeyName;
            this.sortKeyName = null;
        }

        public String getPartitionKeyName() {
            return partitionKeyName;
        }

        public String getSortKeyName() {
            return sortKeyName;
        }
    }
}
