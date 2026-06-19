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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestTimeTravelCatalog;
import org.apache.flink.table.planner.functions.sql.ml.SqlVectorSearchTableFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.planner.utils.DateTimeTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.plan.RelOptPlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base ITCase for {@link SqlVectorSearchTableFunction}. */
public abstract class VectorSearchTableFunctionTestBase extends TableTestBase {

    private TableTestUtil util;

    protected abstract TableTestUtil getUtil();

    @BeforeEach
    public void setup() {
        util = getUtil();

        // Create test table
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE QueryTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  d ARRAY<FLOAT>,\n"
                                + "  rowtime TIMESTAMP(3),\n"
                                + "  proctime as PROCTIME(),\n"
                                + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true' "
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE VectorTable (\n"
                                + "  e INT,\n"
                                + "  f BIGINT,\n"
                                + "  g ARRAY<FLOAT>\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'enable-vector-search' = 'true',"
                                + "  'bounded' = 'true'"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE VectorTableWithProctime (\n"
                                + "  e INT,\n"
                                + "  f BIGINT,\n"
                                + "  g ARRAY<FLOAT>,\n"
                                + "  proctime as PROCTIME()\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'enable-vector-search' = 'true',\n"
                                + "  'bounded' = 'true'"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE VectorTableWithMetadata(\n"
                                + "  e INT,\n"
                                + "  f ARRAY<FLOAT> METADATA,\n"
                                + "  g ARRAY<FLOAT>,\n"
                                + "  h AS e + 1\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'readable-metadata' = 'f:ARRAY<FLOAT>',\n"
                                + "  'bounded' = 'true'"
                                + ")");
    }

    @Test
    void testSimple() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10"
                        + ")\n"
                        + ")";
        util.verifyRelPlan(sql);
    }

    @Test
    void testLiteralValue() {
        String sql =
                "SELECT * FROM LATERAL TABLE(VECTOR_SEARCH(TABLE VectorTable, DESCRIPTOR(`g`), ARRAY[1.5, 2.0], 10))";
        util.verifyRelPlan(sql);
    }

    @Test
    void testLiteralValueWithoutLateralKeyword() {
        String sql =
                "SELECT * FROM TABLE(VECTOR_SEARCH(TABLE VectorTable, DESCRIPTOR(`g`), ARRAY[1.5, 2.0], 10))";
        util.verifyRelPlan(sql);
    }

    @Test
    void testNamedArgument() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    SEARCH_TABLE => TABLE VectorTable,\n"
                        + "    COLUMN_TO_QUERY => QueryTable.d,\n"
                        + "    COLUMN_TO_SEARCH => DESCRIPTOR(`g`),\n"
                        + "    TOP_K => 10"
                        + "  )\n"
                        + ")";
        util.verifyRelPlan(sql);
    }

    @Test
    void testOutOfOrderNamedArgument() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    COLUMN_TO_QUERY => QueryTable.d,\n"
                        + "    COLUMN_TO_SEARCH => DESCRIPTOR(`g`),\n"
                        + "    TOP_K => 10,\n"
                        + "    SEARCH_TABLE => TABLE VectorTable\n"
                        + "  )\n"
                        + ")";
        util.verifyRelPlan(sql);
    }

    @Test
    void testNamedArgumentWithRuntimeConfig() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    COLUMN_TO_QUERY => QueryTable.d,\n"
                        + "    COLUMN_TO_SEARCH => DESCRIPTOR(`g`),\n"
                        + "    TOP_K => 10,\n"
                        + "    CONFIG => MAP['async', 'true', 'timeout', '100s'],\n"
                        + "    SEARCH_TABLE => TABLE VectorTable\n"
                        + "  )\n"
                        + ")";
        util.verifyRelPlan(sql);
    }

    @Test
    void testNameConflicts() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE NameConflictTable(\n"
                                + "  a INT,\n"
                                + "  score ARRAY<FLOAT>,\n"
                                + "  score0 ARRAY<FLOAT>,\n"
                                + "  score1 ARRAY<FLOAT>\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true'"
                                + ")");
        util.verifyRelPlan(
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE NameConflictTable, DESCRIPTOR(`score`), QueryTable.d, 10))");
    }

    @Test
    void testDescriptorTypeIsNotExpected() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`f`), QueryTable.d, 10"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Expect search column `f` type is ARRAY<FLOAT> or ARRAY<DOUBLE>, but its type is BIGINT."));
    }

    @Test
    void testDescriptorContainsMultipleColumns() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`f`, `g`), QueryTable.d, 10"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Expect parameter COLUMN_TO_SEARCH for VECTOR_SEARCH only contains one column, but multiple columns are found in operand DESCRIPTOR(`f`, `g`)."));
    }

    @Test
    void testQueryColumnIsNotArray() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.c, 10"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Can not cast the query column type STRING to target type FLOAT ARRAY. Please keep the query column type is same to the search column type."));
    }

    @Test
    void testIllegalTopKValue1() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10.0"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Expect parameter top_k is an INTEGER NOT NULL literal in VECTOR_SEARCH, but it is 10.0 with type DECIMAL(3, 1) NOT NULL."));
    }

    @Test
    void testIllegalTopKValue2() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, QueryTable.a"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Expect parameter top_k is an INTEGER NOT NULL literal in VECTOR_SEARCH, but it is QueryTable.a with type INT."));
    }

    @Test
    void testIllegalTopKValue3() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 0"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Parameter top_k must be greater than 0, but was 0."));
    }

    @Test
    void testSearchTableWithCalc() {
        // calc -> source
        util.verifyRelPlan(
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTableWithProctime, DESCRIPTOR(`g`), QueryTable.d, 10))");
    }

    @Test
    void testSearchTableWithProjection() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE FUNCTION add_one AS '%s'",
                                JavaUserDefinedScalarFunctions.JavaFunc0.class.getName()));
        util.verifyRelPlan(
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    (SELECT add_one(e) as e1, g, proctime FROM VectorTableWithProctime), DESCRIPTOR(`g`), QueryTable.d, 10))");
    }

    @Test
    void testSearchTableWithMetadataTable() {
        util.verifyRelPlan(
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "  VECTOR_SEARCH(\n"
                        + "    TABLE VectorTableWithMetadata,\n"
                        + "    DESCRIPTOR(`g`),\n"
                        + "    QueryTable.d,\n"
                        + "    10"
                        + "  )\n"
                        + ")");
    }

    @Test
    void testSearchTableWithDescriptorUsingMetadata() {
        util.verifyRelPlan(
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "  VECTOR_SEARCH(\n"
                        + "    TABLE VectorTableWithMetadata,\n"
                        + "    DESCRIPTOR(`f`),\n"
                        + "    QueryTable.d,\n"
                        + "    10"
                        + "  )\n"
                        + ")");
    }

    @Test
    void testSearchTableUsingUDFComputedColumn() {
        util.tableEnv()
                .executeSql(
                        String.format("CREATE FUNCTION udf AS '%s'", TestArrayUDF.class.getName()));
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE VectorTableWithComputedColumn (\n"
                                + "  e INT NOT NULL,\n"
                                + "  f BIGINT,\n"
                                + "  g ARRAY<FLOAT>,\n"
                                + "  h as udf(e)\n"
                                + ") with (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true'"
                                + ")");
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                                                + "  VECTOR_SEARCH(\n"
                                                + "    TABLE VectorTableWithComputedColumn,\n"
                                                + "    DESCRIPTOR(`h`),\n"
                                                + "    QueryTable.d,\n"
                                                + "    10"
                                                + "  )\n"
                                                + ")"))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TableException.class,
                                "VECTOR_SEARCH can not find column `h` in the search_table default_catalog.default_database.VectorTableWithComputedColumn physical output type. "
                                        + "Currently, Flink doesn't support to use computed column as the search column."));
    }

    @Test
    void testSearchTableWithSnapshot() throws Exception {
        String catalogName = "ttc";
        TestTimeTravelCatalog catalog = new TestTimeTravelCatalog(catalogName);
        Map<String, String> options = new HashMap<>();
        options.put("connector", "values");
        options.put("bounded", "true");
        catalog.registerTableForTimeTravel(
                "t1",
                Schema.newBuilder()
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.ARRAY(DataTypes.DOUBLE()))
                        .build(),
                options,
                DateTimeTestUtil.toEpochMills(
                        "2023-07-31 00:00:00", "yyyy-MM-dd HH:mm:ss", ZoneId.of("UTC")));

        TableEnvironment tEnv = util.tableEnv();
        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM (select *, proctime() pts from t1) qt, LATERAL TABLE(\n"
                                                + "VECTOR_SEARCH(\n"
                                                + "    (SELECT * FROM t1 FOR SYSTEM_TIME AS OF qt.pts), DESCRIPTOR(`f2`), qt.f2, 10))"))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                RelOptPlanner.CannotPlanException.class,
                                "VECTOR_SEARCH does not support FlinkLogicalSnapshot node in parameter search_table."));
    }

    @Test
    void testSearchTableWithFilter() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    (SELECT * FROM VectorTable WHERE e > 0),\n"
                        + "    DESCRIPTOR(`g`),\n"
                        + "    QueryTable.d,\n"
                        + "    10"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                RelOptPlanner.CannotPlanException.class,
                                "VECTOR_SEARCH does not support filter on parameter search_table."));
    }

    @Test
    void testSearchTableWithWatermark() {
        // watermark assigner -> calc -> scan
        if (util.isBounded()) {
            return;
        }
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE IllegalTable (\n"
                                + "  e INT,\n"
                                + "  f BIGINT,\n"
                                + "  g ARRAY<FLOAT>,\n"
                                + "  rowtime TIMESTAMP(3),\n"
                                + "  proctime as PROCTIME(),\n"
                                + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                                + ") with (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE IllegalTable, DESCRIPTOR(`g`), QueryTable.d, 10))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                RelOptPlanner.CannotPlanException.class,
                                "VECTOR_SEARCH does not support FlinkLogicalWatermarkAssigner node in parameter search_table."));
    }

    @Test
    void testSearchTableNonExistColumn() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`z`), QueryTable.d, 10"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(FlinkAssertions.anyCauseMatches("Unknown identifier 'z'"));
    }

    @Test
    public void testIllegalRuntimeConfigType() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10, 10"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class, "Config param should be a MAP."));
    }

    @Test
    public void testIllegalConfigValue1() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10, MAP['async', 'yes']"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                IllegalArgumentException.class,
                                "Unrecognized option for boolean: yes. Expected either true or false(case insensitive)"));
    }

    @Test
    public void testIllegalConfigValue2() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10, MAP['async', 'true', 'max-concurrent-operations', '-1']"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                ValidationException.class,
                                "Invalid runtime config option 'max-concurrent-operations'. Its value should be positive integer but was -1."));
    }

    @Test
    public void testPreferAsync() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10, MAP['async', 'true']"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyExecPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                TableException.class, "Require async mode"));
    }

    @Test
    public void testUsingRuntimeConfigToAdjustConnectorParameter() {
        String sql =
                "SELECT * FROM QueryTable, LATERAL TABLE(\n"
                        + "VECTOR_SEARCH(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`g`), QueryTable.d, 10, MAP['enable-vector-search', 'false']"
                        + ")\n"
                        + ")";
        assertThatThrownBy(() -> util.verifyExecPlan(sql))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                IllegalArgumentException.class,
                                "Require option enable-vector-search true."));
    }

    public static class TestArrayUDF extends ScalarFunction {
        public Float[] eval(int i) {
            return new Float[] {(float) i};
        }
    }
}
