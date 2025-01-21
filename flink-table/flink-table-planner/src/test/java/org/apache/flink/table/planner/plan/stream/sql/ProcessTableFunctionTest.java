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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.EnumSet;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.annotation.ArgumentTrait.OPTIONAL_PARTITION_BY;
import static org.apache.flink.table.annotation.ArgumentTrait.PASS_COLUMNS_THROUGH;
import static org.apache.flink.table.annotation.ArgumentTrait.SUPPORT_UPDATES;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_ROW;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the type inference and planning part of {@link ProcessTableFunction}. */
public class ProcessTableFunctionTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE VIEW t AS SELECT * FROM (VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score)");
        util.tableEnv()
                .executeSql("CREATE VIEW t_name_diff AS SELECT 'Bob' AS name, 12 AS different");
        util.tableEnv()
                .executeSql("CREATE VIEW t_type_diff AS SELECT 'Bob' AS name, TRUE AS isValid");
        util.tableEnv()
                .executeSql("CREATE VIEW t_updating AS SELECT name, COUNT(*) FROM t GROUP BY name");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t_sink (name STRING, data STRING) WITH ('connector' = 'blackhole')");
    }

    @Test
    void testScalarArgsNoUid() {
        util.addTemporarySystemFunction("f", ScalarArgsFunction.class);
        util.verifyRelPlan("SELECT * FROM f(i => 1, b => true)");
    }

    @Test
    void testScalarArgsWithUid() {
        util.addTemporarySystemFunction("f", ScalarArgsFunction.class);
        // argument 'uid' is also reordered
        util.verifyRelPlan("SELECT * FROM f(uid => 'my-uid', i => 1, b => true)");
    }

    @Test
    void testUnknownScalarArg() {
        util.addTemporarySystemFunction("f", ScalarArgsFunction.class);
        // argument 'invalid' is ignored
        util.verifyRelPlan("SELECT * FROM f(i => 1, b => true, invalid => 'invalid')");
    }

    @Test
    void testTableAsRow() {
        util.addTemporarySystemFunction("f", TableAsRowFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t, i => 1)");
    }

    @Test
    void testTypedTableAsRow() {
        util.addTemporarySystemFunction("f", TypedTableAsRowFunction.class);
        util.verifyRelPlan("SELECT * FROM f(u => TABLE t, i => 1)");
    }

    @Test
    void testTypedTableAsRowIgnoringColumnNames() {
        util.addTemporarySystemFunction("f", TypedTableAsRowFunction.class);
        // function expects <STRING name, INT score>
        // but table is <STRING name, INT different>
        util.verifyRelPlan("SELECT * FROM f(u => TABLE t_name_diff, i => 1)");
    }

    @Test
    void testTableAsSet() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)");
    }

    @Test
    void testTableAsSetOptionalPartitionBy() {
        util.addTemporarySystemFunction("f", TableAsSetOptionalPartitionFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t, i => 1)");
    }

    @Test
    void testTypedTableAsSet() {
        util.addTemporarySystemFunction("f", TypedTableAsSetFunction.class);
        util.verifyRelPlan("SELECT * FROM f(u => TABLE t PARTITION BY name, i => 1)");
    }

    @Test
    void testEmptyArgs() {
        util.addTemporarySystemFunction("f", EmptyArgFunction.class);
        util.verifyRelPlan("SELECT * FROM f(uid => 'my-ptf')");
    }

    @Test
    void testPojoArgs() {
        util.addTemporarySystemFunction("f", PojoArgsFunction.class);
        util.addTemporarySystemFunction("pojoCreator", PojoCreatingFunction.class);
        util.verifyRelPlan(
                "SELECT * FROM f(input => TABLE t, scalar => pojoCreator('Bob', 12), uid => 'my-ptf')");
    }

    @Test
    void testTableAsSetPassThroughColumns() {
        util.addTemporarySystemFunction("f", TableAsSetPassThroughFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)");
    }

    @Test
    void testTableAsRowPassThroughColumns() {
        util.addTemporarySystemFunction("f", TableAsRowPassThroughFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t, i => 1)");
    }

    @Test
    void testUpdatingInput() {
        util.addTemporarySystemFunction("f", UpdatingArgFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t_updating PARTITION BY name, i => 1)");
    }

    @Test
    void testMissingUid() {
        // Function name contains special characters and can thus not be used as UID
        util.addTemporarySystemFunction("f*", ScalarArgsFunction.class);
        assertThatThrownBy(() -> util.verifyRelPlan("SELECT * FROM `f*`(42, true)"))
                .satisfies(
                        anyCauseMatches(
                                "Could not derive a unique identifier for process table function 'f*'. "
                                        + "The function's name does not qualify for a UID. "
                                        + "Please provide a custom identifier using the implicit `uid` argument. "
                                        + "For example: myFunction(..., uid => 'my-id')"));
    }

    @Test
    void testUidPipelineSplitIntoTwoFunctions() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);
        util.verifyExecPlan(
                util.tableEnv()
                        .createStatementSet()
                        .addInsertSql(
                                "INSERT INTO t_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'a')")
                        .addInsertSql(
                                "INSERT INTO t_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'b')"));
    }

    @Test
    void testUidPipelineMergeIntoOneFunction() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);
        util.verifyExecPlan(
                util.tableEnv()
                        .createStatementSet()
                        .addInsertSql(
                                "INSERT INTO t_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same')")
                        .addInsertSql(
                                "INSERT INTO t_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same')"));
    }

    @Test
    void testUidPipelineMergeWithFanOut() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);

        util.verifyExecPlan(
                util.tableEnv()
                        .createStatementSet()
                        .addInsertSql(
                                "INSERT INTO t_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same') WHERE name = 'Bob'")
                        .addInsertSql(
                                "INSERT INTO t_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same') WHERE name = 'Alice'"));
    }

    @ParameterizedTest
    @MethodSource("errorSpecs")
    void testErrorBehavior(ErrorSpec spec) {
        util.addTemporarySystemFunction("f", spec.functionClass);
        assertThatThrownBy(() -> util.verifyExecPlan(spec.sql))
                .satisfies(anyCauseMatches(spec.errorMessage));
    }

    private static Stream<ErrorSpec> errorSpecs() {
        return Stream.of(
                ErrorSpec.of(
                        "invalid uid",
                        ScalarArgsFunction.class,
                        "SELECT * FROM f(uid => '%', i => 1, b => true)",
                        "Invalid unique identifier for process table function. "
                                + "The `uid` argument must be a string literal that follows the pattern [a-zA-Z_][a-zA-Z-_0-9]*. "
                                + "But found: %"),
                ErrorSpec.of(
                        "typed table as row with invalid input",
                        TypedTableAsRowFunction.class,
                        // function expects <STRING name, INT score>
                        "SELECT * FROM f(u => TABLE t_type_diff, i => 1)",
                        "No match found for function signature "
                                + "f(<RecordType(CHAR(3) name, BOOLEAN isValid)>, <NUMERIC>, <CHARACTER>)"),
                ErrorSpec.of(
                        "table as set with missing partition by",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t, i => 1)",
                        "Table argument 'r' requires a PARTITION BY clause for parallel processing."),
                ErrorSpec.of(
                        "typed table as set with invalid input",
                        TypedTableAsSetFunction.class,
                        // function expects <STRING name, INT score>
                        "SELECT * FROM f(u => TABLE t_type_diff PARTITION BY name, i => 1)",
                        "No match found for function signature "
                                + "f(<RecordType(CHAR(3) name, BOOLEAN isValid)>, <NUMERIC>, <CHARACTER>)"),
                ErrorSpec.of(
                        "table function instead of process table function",
                        NoProcessTableFunction.class,
                        "SELECT * FROM f(r => TABLE t)",
                        "Only scalar arguments are supported at this location. "
                                + "But argument 'r' declared the following traits: [TABLE, TABLE_AS_ROW]"),
                ErrorSpec.of(
                        "reserved args",
                        ReservedArgFunction.class,
                        "SELECT * FROM f(uid => 'my-ptf')",
                        "Function signature must not declare system arguments. Reserved argument names are: [uid]"),
                ErrorSpec.of(
                        "multiple table args",
                        MultiTableFunction.class,
                        "SELECT * FROM f(r1 => TABLE t, r2 => TABLE t)",
                        "Currently, only signatures with at most one table argument are supported."),
                ErrorSpec.of(
                        "row instead of table",
                        TableAsRowFunction.class,
                        "SELECT * FROM f(r => ROW(42), i => 1)",
                        "Invalid argument value. Argument 'r' expects a table to be passed."),
                ErrorSpec.of(
                        "table as row partition by",
                        TableAsRowFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)",
                        "Only tables with set semantics may be partitioned. "
                                + "Invalid PARTITION BY clause in the 0-th operand of table function 'f'"),
                ErrorSpec.of(
                        "invalid partition by clause",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY invalid, i => 1)",
                        "Invalid column 'invalid' for PARTITION BY clause. Available columns are: [name, score]"),
                ErrorSpec.of(
                        "unsupported order by",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name ORDER BY score, i => 1)",
                        "ORDER BY clause is currently not supported."),
                ErrorSpec.of(
                        "updates into insert-only table arg",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t_updating PARTITION BY name, i => 1)",
                        "StreamPhysicalProcessTableFunction doesn't support consuming update changes"),
                ErrorSpec.of(
                        "updates into POJO table arg",
                        InvalidTypedUpdatingArgFunction.class,
                        "SELECT * FROM f(r => TABLE t_updating, i => 1)",
                        "Table arguments that support updates must use a row type."),
                ErrorSpec.of(
                        "uid conflict",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name, i => 42, uid => 'same') "
                                + "UNION ALL SELECT * FROM f(r => TABLE t PARTITION BY name, i => 999, uid => 'same')",
                        "Duplicate unique identifier 'same' detected among process table functions. "
                                + "Make sure that all PTF calls have an identifier defined that is globally unique."));
    }

    /** Testing function. */
    public static class ScalarArgsFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(Integer i, Boolean b) {}
    }

    /** Testing function. */
    public static class TableAsRowFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) Row r, Integer i) {}
    }

    /** Testing function. */
    public static class TypedTableAsRowFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint(ArgumentTrait.TABLE_AS_ROW) User u, Integer i) {}
    }

    /** Testing function. */
    public static class TableAsSetFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r, Integer i) {}
    }

    /** Testing function. */
    public static class UpdatingArgFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES}) Row r, Integer i) {}
    }

    /** Testing function. */
    public static class InvalidTypedUpdatingArgFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint({TABLE_AS_ROW, SUPPORT_UPDATES}) User u, Integer i) {}
    }

    /** Testing function. */
    public static class MultiTableFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r1,
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r2) {}
    }

    /** Testing function. */
    public static class TypedTableAsSetFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint(TABLE_AS_SET) User u, Integer i) {}
    }

    /** Testing function. */
    public static class TableAsSetOptionalPartitionFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r, Integer i) {}
    }

    /** Testing function. */
    public static class TableAsRowPassThroughFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint({TABLE_AS_ROW, PASS_COLUMNS_THROUGH}) Row r, Integer i) {}
    }

    /** Testing function. */
    public static class TableAsSetPassThroughFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH}) Row r, Integer i) {}
    }

    /** Testing function. */
    public static class NoProcessTableFunction extends TableFunction<String> {

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .staticArguments(
                            StaticArgument.table(
                                    "r",
                                    Row.class,
                                    false,
                                    EnumSet.of(StaticArgumentTrait.TABLE_AS_ROW)))
                    .outputTypeStrategy(callContext -> Optional.of(DataTypes.STRING()))
                    .build();
        }

        @SuppressWarnings("unused")
        public void eval(Row r) {}
    }

    /** Testing function. */
    public static class ReservedArgFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(String uid) {}
    }

    /** Testing function. */
    public static class EmptyArgFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval() {}
    }

    /** Testing function. */
    public static class PojoArgsFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint(TABLE_AS_ROW) User input, User scalar) {}
    }

    public static class PojoCreatingFunction extends ScalarFunction {
        public User eval(String s, Integer i) {
            return new User(s, i);
        }
    }

    /** POJO for typed tables. */
    public static class User {
        public String s;
        public Integer i;

        public User(String s, Integer i) {
            this.s = s;
            this.i = i;
        }
    }

    private static class ErrorSpec {
        private final String description;
        private final Class<? extends UserDefinedFunction> functionClass;
        private final String sql;
        private final String errorMessage;

        private ErrorSpec(
                String description,
                Class<? extends UserDefinedFunction> functionClass,
                String sql,
                String errorMessage) {
            this.description = description;
            this.functionClass = functionClass;
            this.sql = sql;
            this.errorMessage = errorMessage;
        }

        static ErrorSpec of(
                String description,
                Class<? extends UserDefinedFunction> functionClass,
                String sql,
                String errorMessage) {
            return new ErrorSpec(description, functionClass, sql, errorMessage);
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
