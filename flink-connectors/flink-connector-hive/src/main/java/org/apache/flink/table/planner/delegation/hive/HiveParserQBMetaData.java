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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.QBMetaData. */
public class HiveParserQBMetaData {
    public static final int DEST_TABLE = 1;
    // alias -> <tableName, CatalogBaseTable>
    private final Map<String, Tuple2<String, CatalogTable>> aliasToTable = new LinkedHashMap<>();
    private final Map<String, Tuple2<String, CatalogTable>> nameToDestTable = new HashMap<>();
    private final Map<String, Tuple3<String, CatalogTable, CatalogPartitionSpec>>
            nameToDestPartition = new HashMap<>();
    private final Map<String, String> nameToDestFile = new HashMap<>();
    private final Map<String, Integer> nameToDestType = new HashMap<>();
    private final Map<String, Map<String, String>> aliasToPartSpec = new LinkedHashMap<>();

    public HiveParserQBMetaData() {}

    public void setSrcForAlias(String alias, String tabName, CatalogTable tab) {
        this.aliasToTable.put(alias, Tuple2.of(tabName, tab));
    }

    public void setDestForAlias(String alias, String tabName, CatalogTable tab) {
        this.nameToDestType.put(alias, 1);
        this.nameToDestTable.put(alias, Tuple2.of(tabName, tab));
    }

    public void setDestForAlias(
            String alias, String tabName, CatalogTable tab, CatalogPartitionSpec part) {
        this.nameToDestType.put(alias, 2);
        this.nameToDestPartition.put(alias, Tuple3.of(tabName, tab, part));
    }

    public void setDestForAlias(String alias, String fname, boolean isDfsFile) {
        this.nameToDestType.put(alias, isDfsFile ? 3 : 5);
        this.nameToDestFile.put(alias, fname);
    }

    public Map<String, Tuple2<String, CatalogTable>> getNameToDestTable() {
        return this.nameToDestTable;
    }

    public String getDestFileForAlias(String alias) {
        return nameToDestFile.get(alias.toLowerCase());
    }

    public CatalogPartitionSpec getDestPartitionForAlias(String alias) {
        return this.nameToDestPartition.get(alias.toLowerCase()).f2;
    }

    public Map<String, Tuple3<String, CatalogTable, CatalogPartitionSpec>>
            getNameToDestPartition() {
        return this.nameToDestPartition;
    }

    public Tuple2<String, CatalogTable> getSrcForAlias(String alias) {
        return this.aliasToTable.get(alias.toLowerCase());
    }

    public Map<String, String> getPartSpecForAlias(String alias) {
        return this.aliasToPartSpec.get(alias);
    }

    public void setPartSpecForAlias(String alias, Map<String, String> partSpec) {
        this.aliasToPartSpec.put(alias, partSpec);
    }

    public Integer getDestTypeForAlias(String alias) {
        return nameToDestType.get(alias.toLowerCase());
    }
}
