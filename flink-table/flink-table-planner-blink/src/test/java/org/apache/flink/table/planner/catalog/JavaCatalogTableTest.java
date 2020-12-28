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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Tests for resolving types of computed columns (including time attributes) of tables from catalog.
 */
@RunWith(Parameterized.class)
public class JavaCatalogTableTest extends TableTestBase {
    @Parameterized.Parameters(name = "streamingMode = {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter public boolean isStreamingMode;

    private TableTestUtil getTestUtil() {
        if (isStreamingMode) {
            return streamTestUtil(new TableConfig());
        } else {
            return batchTestUtil(new TableConfig());
        }
    }

    @Test
    public void testResolvingSchemaOfCustomCatalogTableSql() throws Exception {
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(isStreamingMode),
                false);
        tableEnvironment.registerCatalog("testCatalog", genericInMemoryCatalog);
        tableEnvironment.executeSql(
                "CREATE VIEW testTable2 AS SELECT * FROM testCatalog.`default`.testTable");

        testUtil.verifyExecPlan(
                "SELECT COUNT(*) FROM testTable2 GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE)");
    }

    @Test
    public void testResolvingSchemaOfCustomCatalogTableTableApi() throws Exception {
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(isStreamingMode),
                false);
        tableEnvironment.registerCatalog("testCatalog", genericInMemoryCatalog);

        Table table =
                tableEnvironment
                        .from("testCatalog.`default`.testTable")
                        .window(Tumble.over(lit(10).minute()).on($("rowtime")).as("w"))
                        .groupBy($("w"))
                        .select(lit(1).count());
        testUtil.verifyExecPlan(table);
    }

    @Test
    public void testResolvingProctimeOfCustomTableSql() throws Exception {
        if (!isStreamingMode) {
            // proctime not supported in batch
            return;
        }
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(isStreamingMode),
                false);
        tableEnvironment.registerCatalog("testCatalog", genericInMemoryCatalog);

        testUtil.verifyExecPlan(
                "SELECT COUNT(*) FROM testCatalog.`default`.testTable "
                        + "GROUP BY TUMBLE(proctime, INTERVAL '10' MINUTE)");
    }

    @Test
    public void testResolvingProctimeOfCustomTableTableApi() throws Exception {
        if (!isStreamingMode) {
            // proctime not supported in batch
            return;
        }
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(isStreamingMode),
                false);
        tableEnvironment.registerCatalog("testCatalog", genericInMemoryCatalog);

        Table table =
                tableEnvironment
                        .from("testCatalog.`default`.testTable")
                        .window(Tumble.over(lit(10).minute()).on($("proctime")).as("w"))
                        .groupBy($("w"))
                        .select(lit(1).count());
        testUtil.verifyExecPlan(table);
    }

    private static class CustomCatalogTable implements CatalogTable {

        private final boolean isStreamingMode;

        private CustomCatalogTable(boolean isStreamingMode) {
            this.isStreamingMode = isStreamingMode;
        }

        @Override
        public boolean isPartitioned() {
            return false;
        }

        @Override
        public List<String> getPartitionKeys() {
            return Collections.emptyList();
        }

        @Override
        public CatalogTable copy(Map<String, String> options) {
            return this;
        }

        @Override
        public Map<String, String> toProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> getProperties() {
            Map<String, String> map = new HashMap<>();
            map.put("connector", "values");
            map.put("bounded", Boolean.toString(!isStreamingMode));
            return map;
        }

        @Override
        public TableSchema getSchema() {
            return TableSchema.builder()
                    .field("count", DataTypes.BIGINT())
                    .field("rowtime", DataTypes.TIMESTAMP())
                    .field("proctime", DataTypes.TIMESTAMP(), "proctime()")
                    .watermark("rowtime", "rowtime - INTERVAL '5' SECONDS", DataTypes.TIMESTAMP())
                    .build();
        }

        @Override
        public String getComment() {
            return null;
        }

        @Override
        public CatalogBaseTable copy() {
            return this;
        }

        @Override
        public Optional<String> getDescription() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return Optional.empty();
        }
    }
}
