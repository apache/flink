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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.hadoop.hive.ql.parse.JoinType;

import java.util.List;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.NamedJoinInfo. */
public class HiveParserNamedJoinInfo {

    private List<String> tableAliases;
    private List<String> namedColumns;
    private final JoinType hiveJoinType;

    public HiveParserNamedJoinInfo(
            List<String> aliases, List<String> namedColumns, JoinType hiveJoinType) {
        super();
        this.tableAliases = aliases;
        this.namedColumns = namedColumns;
        this.hiveJoinType = hiveJoinType;
    }

    public List<String> getAliases() {
        return tableAliases;
    }

    public void setAliases(List<String> aliases) {
        this.tableAliases = aliases;
    }

    public List<String> getNamedColumns() {
        return namedColumns;
    }

    public void setNamedColumns(List<String> namedColumns) {
        this.namedColumns = namedColumns;
    }

    public JoinType getHiveJoinType() {
        return hiveJoinType;
    }
}
