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

/** Represents DynamoDB table configuration. */
@PublicEvolving
public class DynamoDbTableConfig {

    private final Map<String, KeyConfig> perTableConfig;

    public DynamoDbTableConfig() {
        this.perTableConfig = new HashMap<>();
    }

    public void addKeyConfig(String tableName, String partitionKeyName, String sortKeyName) {
        this.perTableConfig.put(tableName, new KeyConfig(partitionKeyName, sortKeyName));
    }

    public void addKeyConfig(String tableName, String partitionKeyName) {
        this.perTableConfig.put(tableName, new KeyConfig(partitionKeyName));
    }

    public KeyConfig getKeyConfig(String tableName) {
        return perTableConfig.get(tableName);
    }

    /** DynamoDB Primary Key configuration. */
    public static class KeyConfig {

        private final String partitionKeyName;

        @Nullable private final String sortKeyName;

        public KeyConfig(String partitionKeyName, String sortKeyName) {
            this.partitionKeyName = partitionKeyName;
            this.sortKeyName = sortKeyName;
        }

        public KeyConfig(String partitionKeyName) {
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
