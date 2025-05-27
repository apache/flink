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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for model table value function. */
public class MLPredictTableFunctionTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        // Create test table
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  d DECIMAL(10, 3),\n"
                                + "  rowtime TIMESTAMP(3),\n"
                                + "  proctime as PROCTIME(),\n"
                                + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                                + ") with (\n"
                                + "  'connector' = 'values'\n"
                                + ")");

        // Create test model
        util.tableEnv()
                .executeSql(
                        "CREATE MODEL MyModel\n"
                                + "INPUT (a INT, b BIGINT)\n"
                                + "OUTPUT(e STRING, f ARRAY<INT>)\n"
                                + "with (\n"
                                + "  'provider' = 'openai'\n"
                                + ")");
    }

    @Test
    public void testNamedArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(INPUT => TABLE MyTable, "
                        + "MODEL => MODEL MyModel, "
                        + "ARGS  => DESCRIPTOR(a, b)))";
        assertReachesRelConverter(sql);
    }

    @Test
    public void testOptionalNamedArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(INPUT => TABLE MyTable, "
                        + "MODEL  => MODEL MyModel, "
                        + "ARGS   => DESCRIPTOR(a, b),"
                        + "CONFIG => MAP['key', 'value']))";
        assertReachesRelConverter(sql);
    }

    @Test
    public void testSimple() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b)))";
        assertReachesRelConverter(sql);
    }

    @Test
    public void testConfigWithCast() {
        // 'async' and 'timeout' in the map are both cast to VARCHAR(7)
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', 'true', 'timeout', '100s']))";
        assertReachesRelConverter(sql);
    }

    @Test
    public void testTooFewArguments() {
        String sql = "SELECT *\n" + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "Invalid number of arguments to function 'ML_PREDICT'. Was expecting 3 arguments");
    }

    @Test
    public void testTooManyArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['key', 'value'], 'arg0'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "Invalid number of arguments to function 'ML_PREDICT'. Was expecting 3 arguments");
    }

    @Test
    public void testNonExistModel() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL NonExistModel, DESCRIPTOR(a, b), MAP['key', 'value'], 'arg0'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Object 'NonExistModel' not found");
    }

    @Test
    public void testConflictOutputColumnName() {
        util.tableEnv()
                .executeSql(
                        "CREATE MODEL ConflictModel\n"
                                + "INPUT (a INT, b BIGINT)\n"
                                + "OUTPUT(c STRING, d ARRAY<INT>)\n"
                                + "with (\n"
                                + "  'provider' = 'openai'\n"
                                + ")");

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL ConflictModel, DESCRIPTOR(a, b)))";
        assertReachesRelConverter(sql);
    }

    @Test
    public void testMissingModelParam() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, DESCRIPTOR(a, b), DESCRIPTOR(a, b)))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "SQL validation failed. Second operand must be a model identifier.");
    }

    @Test
    public void testMismatchInputSize() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b, c)))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "SQL validation failed. Number of descriptor input columns (3) does not match model input size (2)");
    }

    @Test
    public void testNonExistColumn() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(no_col)))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unknown identifier 'no_col'");
    }

    @Test
    public void testNonSimpleColumn() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(MyTable.a)))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Table or column alias must be a simple identifier");
    }

    @ParameterizedTest
    @MethodSource("compatibleTypeProvider")
    public void testCompatibleInputTypes(String tableType, String modelType) {
        // Create test table with dynamic type
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE TypeTable (\n"
                                        + "  col %s\n"
                                        + ") with (\n"
                                        + "  'connector' = 'values'\n"
                                        + ")",
                                tableType));

        // Create test model with dynamic type
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE MODEL TypeModel\n"
                                        + "INPUT (x %s)\n"
                                        + "OUTPUT (res STRING)\n"
                                        + "with (\n"
                                        + "  'provider' = 'openai'\n"
                                        + ")",
                                modelType));

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE TypeTable, MODEL TypeModel, DESCRIPTOR(col)))";
        assertReachesRelConverter(sql);
    }

    @ParameterizedTest
    @MethodSource("incompatibleTypeProvider")
    public void testIncompatibleInputTypes(String tableType, String modelType) {
        // Create test table with dynamic type
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE TypeTable (\n"
                                        + "  col %s\n"
                                        + ") with (\n"
                                        + "  'connector' = 'values'\n"
                                        + ")",
                                tableType));

        // Create test model with dynamic type
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE MODEL TypeModel\n"
                                        + "INPUT (x %s)\n"
                                        + "OUTPUT (res STRING)\n"
                                        + "with (\n"
                                        + "  'provider' = 'openai'\n"
                                        + ")",
                                modelType));

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE TypeTable, MODEL TypeModel, DESCRIPTOR(col)))";

        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be assigned to model input type");
    }

    @Test
    public void testWrongConfigType() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_PREDICT(TABLE MyTable, MODEL MyModel, DESCRIPTOR(a, b), MAP['async', true]))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "ML_PREDICT config param can only be a MAP of string literals. The item at position 1 is TRUE.");
    }

    private void assertReachesRelConverter(String sql) {
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining("while converting MODEL");
    }

    private static Stream<Arguments> compatibleTypeProvider() {
        return Stream.of(
                // NOT NULL to NULLABLE type
                Arguments.of("STRING NOT NULL", "STRING"),

                // Exact matches - primitive types
                Arguments.of("BOOLEAN", "BOOLEAN"),
                Arguments.of("TINYINT", "TINYINT"),
                Arguments.of("SMALLINT", "SMALLINT"),
                Arguments.of("INT", "INT"),
                Arguments.of("BIGINT", "BIGINT"),
                Arguments.of("FLOAT", "FLOAT"),
                Arguments.of("DOUBLE", "DOUBLE"),
                Arguments.of("DECIMAL(10,2)", "DECIMAL(10,2)"),
                Arguments.of("STRING", "STRING"),
                Arguments.of("BINARY(10)", "BINARY(10)"),
                Arguments.of("VARBINARY(10)", "VARBINARY(10)"),
                Arguments.of("DATE", "DATE"),
                Arguments.of("TIME(3)", "TIME(3)"),
                Arguments.of("TIMESTAMP(3)", "TIMESTAMP(3)"),
                Arguments.of("TIMESTAMP_LTZ(3)", "TIMESTAMP_LTZ(3)"),

                // Numeric type promotions
                Arguments.of("TINYINT", "SMALLINT"),
                Arguments.of("SMALLINT", "INT"),
                Arguments.of("INT", "BIGINT"),
                Arguments.of("FLOAT", "DOUBLE"),
                Arguments.of("DECIMAL(5,2)", "DECIMAL(10,2)"),
                Arguments.of(
                        "DECIMAL(10,2)", "DECIMAL(5,2)"), // This is also allowed, is this a bug?

                // String type compatibility
                Arguments.of("CHAR(10)", "STRING"),
                Arguments.of("VARCHAR(20)", "STRING"),

                // Temporal types
                Arguments.of("TIMESTAMP(3)", "TIMESTAMP(3)"),
                Arguments.of("DATE", "DATE"),
                Arguments.of("TIME(3)", "TIME(3)"),

                // Array types
                Arguments.of("ARRAY<INT>", "ARRAY<INT>"),
                Arguments.of("ARRAY<TINYINT>", "ARRAY<SMALLINT>"),
                Arguments.of("ARRAY<DECIMAL(5,2)>", "ARRAY<DECIMAL(10,2)>"),
                Arguments.of("ARRAY<VARCHAR(20)>", "ARRAY<STRING>"),

                // Map types
                Arguments.of("MAP<STRING, INT>", "MAP<STRING, INT>"),
                Arguments.of("MAP<STRING, DECIMAL(5,2)>", "MAP<STRING, DECIMAL(10,2)>"),
                Arguments.of("MAP<VARCHAR(20), ARRAY<INT>>", "MAP<STRING, ARRAY<INT>>"),

                // Row types
                Arguments.of("ROW<a INT, b STRING>", "ROW<a INT, b STRING>"),
                Arguments.of(
                        "ROW<a INT, b STRING>", "ROW<x INT, y STRING>"), // Different field name
                Arguments.of(
                        "ROW<a DECIMAL(5,2), b ARRAY<INT>>", "ROW<a DECIMAL(10,2), b ARRAY<INT>>"),
                Arguments.of(
                        "ROW<a VARCHAR(20), b MAP<STRING, INT>>",
                        "ROW<a STRING, b MAP<STRING, INT>>"),

                // Nested complex types
                Arguments.of(
                        "ROW<a ARRAY<INT>, b MAP<STRING, ARRAY<DECIMAL(5,2)>>>",
                        "ROW<a ARRAY<INT>, b MAP<STRING, ARRAY<DECIMAL(10,2)>>>"),
                Arguments.of(
                        "MAP<STRING, ROW<a INT, b ARRAY<VARCHAR(20)>>>",
                        "MAP<STRING, ROW<a INT, b ARRAY<STRING>>>"));
    }

    private static Stream<Arguments> incompatibleTypeProvider() {
        return Stream.of(
                // NULLABLE to NOT NULL type
                Arguments.of("STRING", "STRING NOT NULL"),

                // Incompatible primitive types
                Arguments.of("BOOLEAN", "INT"),
                Arguments.of("STRING", "INT"),
                Arguments.of("INT", "STRING"),
                Arguments.of("TIMESTAMP(3)", "INT"),
                Arguments.of("DATE", "TIMESTAMP(3)"),
                Arguments.of("BINARY(10)", "STRING"),

                // Incompatible numeric types (wrong direction)
                Arguments.of("BIGINT", "INT"), // Cannot downcast
                Arguments.of("DOUBLE", "FLOAT"), // Cannot downcast

                // Incompatible array types
                Arguments.of("ARRAY<INT>", "ARRAY<STRING>"),
                Arguments.of("ARRAY<INT>", "ARRAY<SMALLINT>"),
                Arguments.of("INT", "ARRAY<INT>"),

                // Incompatible map types
                Arguments.of("MAP<INT, STRING>", "MAP<STRING, STRING>"), // Key type mismatch
                Arguments.of("MAP<STRING, INT>", "MAP<STRING, STRING>"), // Value type mismatch
                Arguments.of("MAP<STRING, DOUBLE>", "MAP<STRING, FLOAT>"), // Cannot downcast value
                Arguments.of("MAP<INT, DOUBLE>", "MAP<SMALLINT, DOUBLE>"), // Cannot downcast key

                // Incompatible row types
                Arguments.of("ROW<a INT, b STRING>", "ROW<a STRING, b INT>"), // Field type mismatch
                Arguments.of("ROW<a INT>", "ROW<a INT, b STRING>"), // Field count mismatch

                // Incompatible nested types
                Arguments.of(
                        "ROW<a ARRAY<STRING>, b MAP<STRING, INT>>",
                        "ROW<a ARRAY<INT>, b MAP<STRING, STRING>>"),
                Arguments.of("MAP<STRING, ARRAY<INT>>", "MAP<STRING, ARRAY<STRING>>"),
                Arguments.of("ARRAY<MAP<STRING, INT>>", "ARRAY<MAP<INT, INT>>"));
    }
}
