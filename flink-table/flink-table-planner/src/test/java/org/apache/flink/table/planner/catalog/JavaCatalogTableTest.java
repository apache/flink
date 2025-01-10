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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Tests for resolving types of computed columns (including time attributes) of tables from catalog.
 */
@ExtendWith(ParameterizedTestExtension.class)
class JavaCatalogTableTest extends TableTestBase {
    @Parameters(name = "streamingMode = {0}")
    private static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @Parameter private boolean streamingMode;

    private TableTestUtil getTestUtil() {
        if (streamingMode) {
            return streamTestUtil(TableConfig.getDefault());
        } else {
            return batchTestUtil(TableConfig.getDefault());
        }
    }

    @TestTemplate
    void testResolvingSchemaOfCustomCatalogTableSql() throws Exception {
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(streamingMode),
                false);
        tableEnvironment.registerCatalog("testCatalog", genericInMemoryCatalog);
        tableEnvironment.executeSql(
                "CREATE VIEW testTable2 AS SELECT * FROM testCatalog.`default`.testTable");

        testUtil.verifyExecPlan(
                "SELECT COUNT(*) FROM testTable2 GROUP BY TUMBLE(rowtime, INTERVAL '10' MINUTE)");
    }

    @TestTemplate
    void testResolvingSchemaOfCustomCatalogTableTableApi() throws Exception {
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(streamingMode),
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

    @TestTemplate
    void testResolvingProctimeOfCustomTableSql() throws Exception {
        if (!streamingMode) {
            // proctime not supported in batch
            return;
        }
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(streamingMode),
                false);
        tableEnvironment.registerCatalog("testCatalog", genericInMemoryCatalog);

        testUtil.verifyExecPlan(
                "SELECT COUNT(*) FROM testCatalog.`default`.testTable "
                        + "GROUP BY TUMBLE(proctime, INTERVAL '10' MINUTE)");
    }

    @TestTemplate
    void testResolvingProctimeOfCustomTableTableApi() throws Exception {
        if (!streamingMode) {
            // proctime not supported in batch
            return;
        }
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        GenericInMemoryCatalog genericInMemoryCatalog = new GenericInMemoryCatalog("in-memory");
        genericInMemoryCatalog.createTable(
                new ObjectPath("default", "testTable"),
                new CustomCatalogTable(streamingMode),
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

    @TestTemplate
    void testTimeAttributeOfView() {
        if (!streamingMode) {
            // time attributes not supported in batch
            return;
        }
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        tableEnvironment.registerCatalog("cat", new CustomCatalog("cat"));
        tableEnvironment.executeSql(
                "CREATE TABLE t(i INT, ts TIMESTAMP_LTZ(3), WATERMARK FOR "
                        + "ts AS ts) WITH ('connector' = 'datagen')");
        tableEnvironment.executeSql("CREATE VIEW `cat`.`default`.v AS SELECT * FROM t");
        testUtil.verifyExecPlan(
                "SELECT sum(i), window_start "
                        + "FROM TUMBLE(\n"
                        + "     DATA => TABLE `cat`.`default`.v,\n"
                        + "     TIMECOL => DESCRIPTOR(ts),\n"
                        + "     SIZE => INTERVAL '10' MINUTES)\n"
                        + "GROUP BY window_start, window_end");
    }

    @TestTemplate
    void testTimeAttributeOfViewSelect() {
        if (!streamingMode) {
            // time attributes not supported in batch
            return;
        }
        TableTestUtil testUtil = getTestUtil();
        TableEnvironment tableEnvironment = testUtil.getTableEnv();
        tableEnvironment.registerCatalog("cat", new CustomCatalog("cat"));
        tableEnvironment.executeSql(
                "CREATE TABLE `cat`.`default`.`t`("
                        + " order_id INT, "
                        + " customer_id INT, "
                        + " product_id INT, "
                        + " product_ids ARRAY<INT>, "
                        + " ts TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts) "
                        + "WITH ('connector' = 'datagen')");
        tableEnvironment.executeSql(
                "CREATE VIEW `cat`.`default`.v AS "
                        + "SELECT `o`.`order_id`, `o`.`customer_id`, `pids`.`product_id`, `o`.`ts`\n"
                        + "FROM `cat`.`default`.`t` AS `o`\n"
                        + "CROSS JOIN UNNEST(`o`.`product_ids`) AS `pids` (`product_id`)");
        testUtil.verifyExecPlan("SELECT * FROM `cat`.`default`.v");
    }

    private static class CustomCatalog extends GenericInMemoryCatalog {
        public CustomCatalog(String name) {
            super(name);
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
            CatalogBaseTable table = super.getTable(tablePath);
            if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
                return new CustomView((CatalogView) table);
            }
            return table;
        }
    }

    private static class CustomView implements CatalogView {

        private final CatalogView origin;

        public CustomView(CatalogView table) {
            this.origin = table;
        }

        @Override
        public String getOriginalQuery() {
            return origin.getOriginalQuery();
        }

        @Override
        public String getExpandedQuery() {
            return origin.getExpandedQuery();
        }

        @Override
        public Map<String, String> getOptions() {
            return origin.getOptions();
        }

        @Override
        public Schema getUnresolvedSchema() {
            Schema originalSchema = origin.getUnresolvedSchema();
            return Schema.newBuilder()
                    .fromColumns(
                            originalSchema.getColumns().stream()
                                    .map(
                                            c -> {
                                                if (c instanceof UnresolvedPhysicalColumn) {
                                                    DataType dataType =
                                                            (DataType)
                                                                    ((UnresolvedPhysicalColumn) c)
                                                                            .getDataType();
                                                    String stringType =
                                                            dataType.getLogicalType()
                                                                    .asSerializableString();
                                                    return new UnresolvedPhysicalColumn(
                                                            c.getName(), DataTypes.of(stringType));
                                                }
                                                throw new UnsupportedOperationException(
                                                        "Unexpected column type");
                                            })
                                    .collect(Collectors.toList()))
                    .build();
        }

        @Override
        public String getComment() {
            return origin.getComment();
        }

        @Override
        public CatalogBaseTable copy() {
            return new CustomView((CatalogView) origin.copy());
        }

        @Override
        public Optional<String> getDescription() {
            return origin.getDescription();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return origin.getDetailedDescription();
        }
    }

    private static class CustomCatalogTable implements CatalogTable {

        private final boolean streamingMode;

        private CustomCatalogTable(boolean streamingMode) {
            this.streamingMode = streamingMode;
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
        public Map<String, String> getOptions() {
            Map<String, String> map = new HashMap<>();
            map.put("connector", "values");
            map.put("bounded", Boolean.toString(!streamingMode));
            return map;
        }

        @Override
        public TableSchema getSchema() {
            return TableSchema.builder()
                    .field("count", DataTypes.BIGINT())
                    .field("rowtime", DataTypes.TIMESTAMP(3))
                    .field("proctime", DataTypes.TIMESTAMP(3), "proctime()")
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
