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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.functions.sql.ml.SqlMLEvaluateTableFunction;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for ML evaluate table value function. */
public class MLEvaluateTableFunctionTest extends TableTestBase {

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
                                + "  label STRING,\n"
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
                                + "OUTPUT(prediction STRING)\n"
                                + "with (\n"
                                + "  'provider' = 'test-model',\n"
                                + "  'endpoint' = 'someendpoint',\n"
                                + "  'task' = 'classification'\n"
                                + ")");
    }

    @Test
    public void testNamedArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "INPUT => TABLE MyTable, "
                        + "MODEL => MODEL MyModel, "
                        + "LABEL => DESCRIPTOR(label), "
                        + "TASK => 'classification', "
                        + "ARGS => DESCRIPTOR(a, b)))";
        assertReachOptimizer(sql);
    }

    @Test
    public void testOptionalNamedArgumentsWithTask() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "INPUT => TABLE MyTable, "
                        + "MODEL => MODEL MyModel, "
                        + "LABEL => DESCRIPTOR(label), "
                        + "ARGS => DESCRIPTOR(a, b), "
                        + "TASK => 'classification'))";
        assertReachOptimizer(sql);
    }

    @Test
    public void testOptionalNamedArgumentsWithTaskAndConfig() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "INPUT => TABLE MyTable, "
                        + "MODEL => MODEL MyModel, "
                        + "LABEL => DESCRIPTOR(label), "
                        + "ARGS => DESCRIPTOR(a, b), "
                        + "TASK => 'classification', "
                        + "CONFIG => MAP['metrics', 'accuracy,f1']))";
        assertReachOptimizer(sql);
    }

    @Test
    public void testSimple() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), DESCRIPTOR(a, b), 'classification'))";
        assertReachOptimizer(sql);
    }

    @Test
    public void testTooFewArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(label)))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "Invalid number of arguments to function 'ML_EVALUATE'. Was expecting 5 arguments");
    }

    @Test
    public void testTooManyArguments() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), DESCRIPTOR(a, b), "
                        + "'classification', MAP['metrics', 'accuracy'], 'extra'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "Invalid number of arguments to function 'ML_EVALUATE'. Was expecting 5 arguments");
    }

    @Test
    public void testNonExistModel() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL NonExistModel, DESCRIPTOR(label), DESCRIPTOR(a, b)))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Object 'NonExistModel' not found");
    }

    @Test
    public void testMissingModelParam() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, DESCRIPTOR(label), DESCRIPTOR(a, b), DESCRIPTOR(label), 'classification'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Second operand must be a model identifier.");
    }

    @Test
    public void testThirdParamNotDescriptor() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, 'label', DESCRIPTOR(a, b), 'classification'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Cannot apply 'ML_EVALUATE' to arguments of type 'ML_EVALUATE");
    }

    @Test
    public void testFourthParamNotDescriptor() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), 'a,b', 'regression'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Cannot apply 'ML_EVALUATE' to arguments of type 'ML_EVALUATE");
    }

    @Test
    public void testModelWithMultipleOutputFields() {
        // Create test model with multiple output fields
        util.tableEnv()
                .executeSql(
                        "CREATE MODEL MultiOutputModel\n"
                                + "INPUT (col INT)\n"
                                + "OUTPUT(prediction STRING, confidence DOUBLE)\n"
                                + "with (\n"
                                + "  'provider' = 'test-model',\n"
                                + "  'endpoint' = 'someendpoint',\n"
                                + "  'task' = 'classification'\n"
                                + ")");

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MultiOutputModel, DESCRIPTOR(label), DESCRIPTOR(a), 'regression'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Model output must have exactly one field for evaluation");
    }

    @Test
    public void testLabelDescriptorWithMultipleColumns() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(label, c), DESCRIPTOR(a, b), 'regression'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Label descriptor must have exactly one column");
    }

    @Test
    public void testMismatchInputSize() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), DESCRIPTOR(a, b, c), 'regression'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Number of input descriptor columns (3) does not match model input size (2)");
    }

    @Test
    public void testNonExistColumn() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(no_label), DESCRIPTOR(a, b), 'regression'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unknown identifier 'no_label'");
    }

    @Test
    public void testNonSimpleColumn() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE MyTable, MODEL MyModel, DESCRIPTOR(MyTable.label), DESCRIPTOR(a, b), 'regression'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Table or column alias must be a simple identifier");
    }

    @Test
    public void testUnsupportedTask() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), DESCRIPTOR(a, b), "
                        + "'unsupported_task'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "Unsupported task: unsupported_task. Supported tasks are: [REGRESSION, CLUSTERING, CLASSIFICATION, EMBEDDING, TEXT_GENERATION].");
    }

    @Test
    public void testUnsupportedTaskType() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), DESCRIPTOR(a, b), "
                        + "1))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "Expected a valid task string literal, but got: SqlNumericLiteral.");
    }

    @Test
    public void testUnsupportedConfigType() {
        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE("
                        + "TABLE MyTable, MODEL MyModel, DESCRIPTOR(label), DESCRIPTOR(a, b), "
                        + "'classification', 1))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining("Config param should be a MAP.");
    }

    @ParameterizedTest
    @MethodSource("compatibleTypeProvider")
    public void testCompatibleInputTypes(String tableType, String modelType) {
        // Create test table with different types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE TypeTable (\n"
                                        + "  col %s,\n"
                                        + "  label STRING\n"
                                        + ") with (\n"
                                        + "  'connector' = 'values'\n"
                                        + ")",
                                tableType));

        // Create test model with different types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE MODEL TypeModel\n"
                                        + "INPUT (col %s)\n"
                                        + "OUTPUT(prediction STRING)\n"
                                        + "with (\n"
                                        + "  'provider' = 'test-model',\n"
                                        + "  'endpoint' = 'someendpoint',\n"
                                        + "  'task' = 'classification'\n"
                                        + ")",
                                modelType));

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE TypeTable, MODEL TypeModel, DESCRIPTOR(label), DESCRIPTOR(col), 'classification'))";
        assertReachOptimizer(sql);
    }

    @ParameterizedTest
    @MethodSource("incompatibleTypeProvider")
    public void testIncompatibleInputTypes(String tableType, String modelType) {
        // Create test table with different types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE TypeTable (\n"
                                        + "  col %s,\n"
                                        + "  label STRING\n"
                                        + ") with (\n"
                                        + "  'connector' = 'values'\n"
                                        + ")",
                                tableType));

        // Create test model with different types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE MODEL TypeModel\n"
                                        + "INPUT (col %s)\n"
                                        + "OUTPUT(prediction STRING)\n"
                                        + "with (\n"
                                        + "  'provider' = 'test-model',\n"
                                        + "  'endpoint' = 'someendpoint',\n"
                                        + "  'task' = 'classification'\n"
                                        + ")",
                                modelType));

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE TypeTable, MODEL TypeModel, DESCRIPTOR(label), DESCRIPTOR(col), 'classification'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be assigned to model input type");
    }

    @ParameterizedTest
    @MethodSource("compatibleOutputTypeProvider")
    public void testCompatibleOutputTypes(String tableType, String modelType) {
        // Create test table with different label types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE TypeTable (\n"
                                        + "  col INT,\n"
                                        + "  label %s\n"
                                        + ") with (\n"
                                        + "  'connector' = 'values'\n"
                                        + ")",
                                tableType));

        // Create test model with different output types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE MODEL TypeModel\n"
                                        + "INPUT (col INT)\n"
                                        + "OUTPUT(prediction %s)\n"
                                        + "with (\n"
                                        + "  'provider' = 'test-model',\n"
                                        + "  'endpoint' = 'someendpoint',\n"
                                        + "  'task' = 'classification'\n"
                                        + ")",
                                modelType));

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE TypeTable, MODEL TypeModel, DESCRIPTOR(label), DESCRIPTOR(col), 'classification'))";
        assertReachOptimizer(sql);
    }

    @ParameterizedTest
    @MethodSource("incompatibleOutputTypeProvider")
    public void testIncompatibleOutputTypes(String tableType, String modelType) {
        // Create test table with different label types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE TypeTable (\n"
                                        + "  col INT,\n"
                                        + "  label %s\n"
                                        + ") with (\n"
                                        + "  'connector' = 'values'\n"
                                        + ")",
                                tableType));

        // Create test model with different output types
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE MODEL TypeModel\n"
                                        + "INPUT (col INT)\n"
                                        + "OUTPUT(prediction %s)\n"
                                        + "with (\n"
                                        + "  'provider' = 'test-model',\n"
                                        + "  'endpoint' = 'someendpoint',\n"
                                        + "  'task' = 'classification'\n"
                                        + ")",
                                modelType));

        String sql =
                "SELECT *\n"
                        + "FROM TABLE(ML_EVALUATE(TABLE TypeTable, MODEL TypeModel, DESCRIPTOR(label), DESCRIPTOR(col), 'classification'))";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be assigned to model output type");
    }

    @Test
    public void testIsOptional() {
        SqlMLEvaluateTableFunction function = new SqlMLEvaluateTableFunction();
        SqlOperandTypeChecker operandMetadata = function.getOperandTypeChecker();

        assertThat(operandMetadata).isNotNull();
        // First three parameters (INPUT, MODEL, DESCRIPTOR, DESCRIPTOR, TASK) are mandatory
        for (int i = 0; i < 4; i++) {
            assertThat(operandMetadata.isOptional(i)).isFalse();
        }

        // Fourth parameter (CONFIG) is optional
        assertThat(operandMetadata.isOptional(5)).isTrue();

        // Parameters beyond the maximum count should not be optional
        assertThat(operandMetadata.isOptional(6)).isFalse();

        assertThat(operandMetadata.getOperandCountRange().getMin()).isEqualTo(5);
        assertThat(operandMetadata.getOperandCountRange().getMax()).isEqualTo(6);
    }

    private void assertReachOptimizer(String sql) {
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Cannot generate a valid execution plan for the given query");
    }

    private static Stream<Arguments> compatibleTypeProvider() {
        return Stream.of(
                // Identical types
                Arguments.of("INT", "INT"),
                Arguments.of("BIGINT", "BIGINT"),
                Arguments.of("DOUBLE", "DOUBLE"),
                Arguments.of("STRING", "STRING"),
                Arguments.of("BOOLEAN", "BOOLEAN"),
                // Numeric type widening
                Arguments.of("TINYINT", "SMALLINT"),
                Arguments.of("SMALLINT", "INT"),
                Arguments.of("INT", "BIGINT"),
                Arguments.of("BIGINT", "DECIMAL(19,0)"),
                Arguments.of("DECIMAL(10,2)", "DOUBLE"),
                Arguments.of("FLOAT", "DOUBLE"),
                // String type compatibility
                Arguments.of("CHAR(10)", "STRING"),
                Arguments.of("VARCHAR(10)", "STRING"),
                Arguments.of("STRING", "STRING"));
    }

    private static Stream<Arguments> incompatibleTypeProvider() {
        return Stream.of(
                // Incompatible numeric types
                Arguments.of("DOUBLE", "INT"),
                Arguments.of("DECIMAL(10,2)", "INT"),
                Arguments.of("STRING", "INT"),
                // Incompatible string types
                Arguments.of("INT", "STRING"),
                Arguments.of("DOUBLE", "STRING"),
                // Incompatible boolean types
                Arguments.of("INT", "BOOLEAN"),
                Arguments.of("STRING", "BOOLEAN"));
    }

    private static Stream<Arguments> compatibleOutputTypeProvider() {
        return Stream.of(
                // Identical types
                Arguments.of("INT", "INT"),
                Arguments.of("BIGINT", "BIGINT"),
                Arguments.of("DOUBLE", "DOUBLE"),
                Arguments.of("STRING", "STRING"),
                Arguments.of("BOOLEAN", "BOOLEAN"),
                // Numeric type widening
                Arguments.of("TINYINT", "SMALLINT"),
                Arguments.of("SMALLINT", "INT"),
                Arguments.of("INT", "BIGINT"),
                Arguments.of("BIGINT", "DECIMAL(19,0)"),
                Arguments.of("DECIMAL(10,2)", "DOUBLE"),
                Arguments.of("FLOAT", "DOUBLE"),
                // String type compatibility
                Arguments.of("CHAR(10)", "STRING"),
                Arguments.of("VARCHAR(10)", "STRING"),
                Arguments.of("STRING", "STRING"),
                // Array types
                Arguments.of("ARRAY<INT>", "ARRAY<INT>"),
                Arguments.of("ARRAY<DOUBLE>", "ARRAY<DOUBLE>"),
                Arguments.of("ARRAY<STRING>", "ARRAY<STRING>"));
    }

    private static Stream<Arguments> incompatibleOutputTypeProvider() {
        return Stream.of(
                // Incompatible numeric types
                Arguments.of("DOUBLE", "INT"),
                Arguments.of("DECIMAL(10,2)", "INT"),
                Arguments.of("STRING", "INT"),
                // Incompatible string types
                Arguments.of("INT", "STRING"),
                Arguments.of("DOUBLE", "STRING"),
                // Incompatible boolean types
                Arguments.of("INT", "BOOLEAN"),
                Arguments.of("STRING", "BOOLEAN"),
                // Incompatible array types
                Arguments.of("ARRAY<INT>", "ARRAY<STRING>"),
                Arguments.of("ARRAY<DOUBLE>", "ARRAY<INT>"),
                Arguments.of("STRING", "ARRAY<STRING>"),
                Arguments.of("ARRAY<STRING>", "STRING"));
    }
}
