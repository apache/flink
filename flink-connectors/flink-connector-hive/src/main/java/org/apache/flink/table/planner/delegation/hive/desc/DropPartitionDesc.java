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

package org.apache.flink.table.planner.delegation.hive.desc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Desc to represent DROP PARTITIONS. */
public class DropPartitionDesc implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;
    private final List<Map<String, String>> specs;
    private final boolean ifExists;

    public DropPartitionDesc(
            String dbName, String tableName, List<Map<String, String>> specs, boolean ifExists) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.specs = specs;
        this.ifExists = ifExists;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Map<String, String>> getSpecs() {
        return specs;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
