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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.AppendProcessTableFunctionBase;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.DescriptorFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.EmptyArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.InvalidUpdatingSemanticsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.MultiInputFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.RequiredTimeFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ScalarArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsSetFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.UpdatingUpsertFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.User;
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
                        "CREATE TABLE t_watermarked (name STRING, score INT, ts TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts) "
                                + "WITH ('connector' = 'datagen')");
        util.tableEnv()
                .executeSql("CREATE TABLE t_sink (`out` STRING) WITH ('connector' = 'blackhole')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t_keyed_sink (`name` STRING, `out` STRING) WITH ('connector' = 'blackhole')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t_full_delete_sink (`name` STRING PRIMARY KEY NOT ENFORCED, `name0` STRING, `count` BIGINT, `mode` STRING) "
                                + "WITH ('connector' = 'values')");
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
    void testTypedTableAsRowIgnoringColumnNames() {
        util.addTemporarySystemFunction("f", TypedTableAsRowFunction.class);
        // function expects <STRING name, INT score>
        // but table is <STRING name, INT different>
        util.verifyRelPlan("SELECT * FROM f(u => TABLE t_name_diff, i => 1)");
    }

    @Test
    void testDifferentPartitionKey() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);
        util.verifyRelPlan("SELECT * FROM f(r => TABLE t PARTITION BY score, i => 1)");
    }

    @Test
    void testEmptyArgs() {
        util.addTemporarySystemFunction("f", EmptyArgFunction.class);
        util.verifyRelPlan("SELECT * FROM f(uid => 'my-ptf')");
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
    void testDescriptors() {
        util.addTemporarySystemFunction("f", DescriptorFunction.class);
        util.verifyRelPlan(
                "SELECT * FROM f("
                        + "columnList1 => DESCRIPTOR(a), "
                        + "columnList2 => DESCRIPTOR(b, c), "
                        + "columnList3 => DESCRIPTOR())");
    }

    @Test
    void testOnTime() {
        util.addTemporarySystemFunction("f", RequiredTimeFunction.class);
        util.verifyRelPlan(
                "SELECT `out`, `rowtime` FROM f(r => TABLE t_watermarked, on_time => DESCRIPTOR(ts))");
    }

    @Test
    void testMissingUid() {
        // Function name contains special characters and can thus not be used as UID
        util.addTemporarySystemFunction("f*", TableAsSetFunction.class);
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT * FROM `f*`(r => TABLE t PARTITION BY name, i => 1)"))
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
                                "INSERT INTO t_keyed_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'a')")
                        .addInsertSql(
                                "INSERT INTO t_keyed_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'b')"));
    }

    @Test
    void testUidPipelineMergeIntoOneFunction() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);
        util.verifyExecPlan(
                util.tableEnv()
                        .createStatementSet()
                        .addInsertSql(
                                "INSERT INTO t_keyed_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same')")
                        .addInsertSql(
                                "INSERT INTO t_keyed_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same')"));
    }

    @Test
    void testUidPipelineMergeWithFanOut() {
        util.addTemporarySystemFunction("f", TableAsSetFunction.class);
        util.verifyExecPlan(
                util.tableEnv()
                        .createStatementSet()
                        .addInsertSql(
                                "INSERT INTO t_keyed_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same') WHERE name = 'Bob'")
                        .addInsertSql(
                                "INSERT INTO t_keyed_sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1, uid => 'same') WHERE name = 'Alice'"));
    }

    @Test
    void testTableAsRowOptionalUid() {
        util.addTemporarySystemFunction("f", TableAsRowFunction.class);
        util.verifyExecPlan(
                util.tableEnv()
                        .createStatementSet()
                        .addInsertSql("INSERT INTO t_sink SELECT * FROM f(r => TABLE t, i => 1)")
                        .addInsertSql("INSERT INTO t_sink SELECT * FROM f(r => TABLE t, i => 42)"));
    }

    @ParameterizedTest
    @MethodSource("errorSpecs")
    void testErrorBehavior(ErrorSpec spec) {
        util.addTemporarySystemFunction("f", spec.functionClass);
        assertThatThrownBy(
                        () -> {
                            if (spec.selectSql != null) {
                                util.verifyExecPlan(spec.selectSql);
                            } else {
                                util.verifyExecPlan(
                                        util.tableEnv()
                                                .createStatementSet()
                                                .addInsertSql(spec.insertIntoSql));
                            }
                        })
                .satisfies(anyCauseMatches(spec.errorMessage));
    }

    private static Stream<ErrorSpec> errorSpecs() {
        return Stream.of(
                ErrorSpec.ofSelect(
                        "invalid uid",
                        ScalarArgsFunction.class,
                        "SELECT * FROM f(uid => '%', i => 1, b => true)",
                        "Invalid unique identifier for process table function. "
                                + "The `uid` argument must be a string literal that follows the pattern [a-zA-Z_][a-zA-Z-_0-9]*. "
                                + "But found: %"),
                ErrorSpec.ofSelect(
                        "typed table as row with invalid input",
                        TypedTableAsRowFunction.class,
                        // function expects <STRING name, INT score>
                        "SELECT * FROM f(u => TABLE t_type_diff, i => 1)",
                        "No match found for function signature "
                                + "f(<RecordType(CHAR(3) name, BOOLEAN isValid)>, <NUMERIC>, <COLUMN_LIST>, <CHARACTER>)"),
                ErrorSpec.ofSelect(
                        "table as set with missing partition by",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t, i => 1)",
                        "Table argument 'r' requires a PARTITION BY clause for parallel processing."),
                ErrorSpec.ofSelect(
                        "typed table as set with invalid input",
                        TypedTableAsSetFunction.class,
                        // function expects <STRING name, INT score>
                        "SELECT * FROM f(u => TABLE t_type_diff PARTITION BY name, i => 1)",
                        "No match found for function signature "
                                + "f(<RecordType(CHAR(3) name, BOOLEAN isValid)>, <NUMERIC>, <COLUMN_LIST>, <CHARACTER>)"),
                ErrorSpec.ofSelect(
                        "table function instead of process table function",
                        NoProcessTableFunction.class,
                        "SELECT * FROM f(r => TABLE t)",
                        "Only scalar arguments are supported at this location. "
                                + "But argument 'r' declared the following traits: [TABLE, TABLE_AS_ROW]"),
                ErrorSpec.ofSelect(
                        "reserved args",
                        ReservedArgFunction.class,
                        "SELECT * FROM f(uid => 'my-ptf')",
                        "Function signature must not declare system arguments. Reserved argument names are: [on_time, uid]"),
                ErrorSpec.ofSelect(
                        "multiple table args",
                        InvalidMultiTableWithRowFunction.class,
                        "SELECT * FROM f(r1 => TABLE t, r2 => TABLE t)",
                        "All table arguments must use set semantics if multiple table arguments are declared."),
                ErrorSpec.ofSelect(
                        "row instead of table",
                        TableAsRowFunction.class,
                        "SELECT * FROM f(r => ROW(42), i => 1)",
                        "Invalid argument value. Argument 'r' expects a table to be passed."),
                ErrorSpec.ofSelect(
                        "table as row partition by",
                        TableAsRowFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)",
                        "Only tables with set semantics may be partitioned. "
                                + "Invalid PARTITION BY clause in the 0-th operand of table function 'f'"),
                ErrorSpec.ofSelect(
                        "invalid partition by clause",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY invalid, i => 1)",
                        "Invalid column 'invalid' for PARTITION BY clause. Available columns are: [name, score]"),
                ErrorSpec.ofSelect(
                        "unsupported order by",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name ORDER BY score, i => 1)",
                        "ORDER BY clause is currently not supported."),
                ErrorSpec.ofSelect(
                        "updates into insert-only table arg",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t_updating PARTITION BY name, i => 1)",
                        "StreamPhysicalProcessTableFunction doesn't support consuming update changes"),
                ErrorSpec.ofSelect(
                        "updates into POJO table arg",
                        InvalidTypedUpdatingArgFunction.class,
                        "SELECT * FROM f(r => TABLE t_updating, i => 1)",
                        "Table arguments that support updates must use a row type."),
                ErrorSpec.ofSelect(
                        "uid conflict",
                        TableAsSetFunction.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name, i => 42, uid => 'same') "
                                + "UNION ALL SELECT * FROM f(r => TABLE t PARTITION BY name, i => 999, uid => 'same')",
                        "Duplicate unique identifier 'same' detected among process table functions. "
                                + "Make sure that all PTF calls have an identifier defined that is globally unique."),
                ErrorSpec.ofSelect(
                        "no updates and pass through",
                        UpdatingPassThrough.class,
                        "SELECT * FROM f(r => TABLE t PARTITION BY name)",
                        "Signatures with updating inputs must not pass columns through."),
                ErrorSpec.ofSelect(
                        "invalid descriptor",
                        DescriptorFunction.class,
                        "SELECT * FROM f(columnList1 => NULL, columnList3 => DESCRIPTOR(b.INVALID))",
                        "column alias must be a simple identifier"),
                ErrorSpec.ofSelect(
                        "ambiguous on_time reference",
                        TableAsRowFunction.class,
                        "WITH duplicate_ts AS (SELECT ts AS ts1, ts AS ts2 FROM t_watermarked) "
                                + "SELECT * FROM f(r => TABLE duplicate_ts, i => 1, on_time => DESCRIPTOR(ts1, ts2))",
                        "Ambiguous time attribute found. The `on_time` argument must reference at "
                                + "most one column in a table argument. Currently, the columns in "
                                + "`on_time` point to both 'ts1' and 'ts2' in table argument 'r'."),
                ErrorSpec.ofSelect(
                        "invalid on_time data type",
                        RequiredTimeFunction.class,
                        "SELECT * FROM f(r => TABLE t_watermarked)",
                        "Table argument 'r' requires a time attribute. "
                                + "Please provide one using the implicit `on_time` argument. "
                                + "For example: myFunction(..., on_time => DESCRIPTOR(`my_timestamp`)"),
                ErrorSpec.ofSelect(
                        "invalid on_time column",
                        TableAsRowFunction.class,
                        "SELECT * FROM f(r => TABLE t_watermarked, i => 1, on_time => DESCRIPTOR(ts, INVALID))",
                        "Invalid time attribute declaration. Each column in the `on_time` argument must "
                                + "reference at least one column in one of the table arguments. "
                                + "Unknown references: [INVALID]"),
                ErrorSpec.ofSelect(
                        "invalid optional table argument",
                        OptionalUntypedTable.class,
                        "SELECT * FROM f()",
                        "Table arguments must not be optional."),
                ErrorSpec.ofSelect(
                        "no changelog support for tables with row semantics",
                        InvalidUpdatingSemanticsFunction.class,
                        "SELECT * FROM f(r => TABLE t)",
                        "PTFs that take table arguments with row semantics don't support updating output. "
                                + "Table argument 'r' of function 'f' must use set semantics."),
                ErrorSpec.ofSelect(
                        "on_time is not supported on updating output",
                        UpdatingUpsertFunction.class,
                        "SELECT * FROM f(r => TABLE t_watermarked PARTITION BY name, on_time => DESCRIPTOR(ts))",
                        "Time operations using the `on_time` argument are currently not supported for "
                                + "PTFs that consume or produce updates."),
                ErrorSpec.ofSelect(
                        "no pass-through for multiple table args",
                        InvalidPassThroughTables.class,
                        "SELECT * FROM f(r1 => TABLE t, r2 => TABLE t)",
                        "Pass-through columns are not supported if multiple table arguments are declared."),
                ErrorSpec.ofSelect(
                        "on_time must be declared for multiple table args",
                        MultiInputFunction.class,
                        "SELECT * FROM f(in1 => TABLE t PARTITION BY name, in2 => TABLE t_watermarked PARTITION BY name, "
                                + "on_time => DESCRIPTOR(ts))",
                        "Invalid time attribute declaration. If multiple tables are declared, "
                                + "the `on_time` argument must reference a time column for each table argument "
                                + "or none. Missing time attributes for: [in1]"),
                ErrorSpec.ofSelect(
                        "different partition keys by data type",
                        MultiInputFunction.class,
                        "SELECT * FROM f(in1 => TABLE t PARTITION BY score, in2 => TABLE t PARTITION BY name)",
                        "Invalid PARTITION BY columns. The number of columns and their data types must match across all "
                                + "involved table arguments. Given partition key sets: [INT NOT NULL], [VARCHAR(5) NOT NULL]"),
                ErrorSpec.ofSelect(
                        "different partition keys by column count",
                        MultiInputFunction.class,
                        "SELECT * FROM f(in1 => TABLE t PARTITION BY score, in2 => TABLE t)",
                        "Invalid PARTITION BY columns. The number of columns and their data types must match across all "
                                + "involved table arguments. Given partition key sets: [INT NOT NULL], []"),
                ErrorSpec.ofSelect(
                        "maximum table arguments reached",
                        HighMultiInputFunction.class,
                        "SELECT * FROM f("
                                + "in1 => TABLE t PARTITION BY score, "
                                + "in2 => TABLE t PARTITION BY score, "
                                + "in3 => TABLE t PARTITION BY score, "
                                + "in4 => TABLE t PARTITION BY score, "
                                + "in5 => TABLE t PARTITION BY score, "
                                + "in6 => TABLE t PARTITION BY score, "
                                + "in7 => TABLE t PARTITION BY score, "
                                + "in8 => TABLE t PARTITION BY score, "
                                + "in9 => TABLE t PARTITION BY score, "
                                + "in10 => TABLE t PARTITION BY score, "
                                + "in11 => TABLE t PARTITION BY score, "
                                + "in12 => TABLE t PARTITION BY score, "
                                + "in13 => TABLE t PARTITION BY score, "
                                + "in14 => TABLE t PARTITION BY score, "
                                + "in15 => TABLE t PARTITION BY score, "
                                + "in16 => TABLE t PARTITION BY score, "
                                + "in17 => TABLE t PARTITION BY score, "
                                + "in18 => TABLE t PARTITION BY score, "
                                + "in19 => TABLE t PARTITION BY score, "
                                + "in20 => TABLE t PARTITION BY score, "
                                + "in21 => TABLE t PARTITION BY score"
                                + ")",
                        "Unsupported table argument count. Currently, the number of input tables is limited to 20."));
    }

    /** Testing function. */
    public static class InvalidTypedUpdatingArgFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint({TABLE_AS_ROW, SUPPORT_UPDATES}) User u, Integer i) {}
    }

    /** Testing function. */
    public static class InvalidMultiTableWithRowFunction extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(
                @ArgumentHint({TABLE_AS_SET, OPTIONAL_PARTITION_BY}) Row r1,
                @ArgumentHint(TABLE_AS_ROW) Row r2) {}
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
    public static class UpdatingPassThrough extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(
                @ArgumentHint({TABLE_AS_SET, SUPPORT_UPDATES, PASS_COLUMNS_THROUGH}) Row r) {}
    }

    /** Testing function. */
    public static class OptionalUntypedTable extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(@ArgumentHint(value = TABLE_AS_ROW, isOptional = true) Row r) {}
    }

    /** Testing function. */
    public static class InvalidPassThroughTables extends ProcessTableFunction<String> {
        @SuppressWarnings("unused")
        public void eval(
                @ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH}) Row r1,
                @ArgumentHint({TABLE_AS_SET, PASS_COLUMNS_THROUGH}) Row r2) {}
    }

    /** Testing function. */
    public static class HighMultiInputFunction extends AppendProcessTableFunctionBase {
        @SuppressWarnings("unused")
        public void eval(
                @ArgumentHint(TABLE_AS_SET) Row in1,
                @ArgumentHint(TABLE_AS_SET) Row in2,
                @ArgumentHint(TABLE_AS_SET) Row in3,
                @ArgumentHint(TABLE_AS_SET) Row in4,
                @ArgumentHint(TABLE_AS_SET) Row in5,
                @ArgumentHint(TABLE_AS_SET) Row in6,
                @ArgumentHint(TABLE_AS_SET) Row in7,
                @ArgumentHint(TABLE_AS_SET) Row in8,
                @ArgumentHint(TABLE_AS_SET) Row in9,
                @ArgumentHint(TABLE_AS_SET) Row in10,
                @ArgumentHint(TABLE_AS_SET) Row in11,
                @ArgumentHint(TABLE_AS_SET) Row in12,
                @ArgumentHint(TABLE_AS_SET) Row in13,
                @ArgumentHint(TABLE_AS_SET) Row in14,
                @ArgumentHint(TABLE_AS_SET) Row in15,
                @ArgumentHint(TABLE_AS_SET) Row in16,
                @ArgumentHint(TABLE_AS_SET) Row in17,
                @ArgumentHint(TABLE_AS_SET) Row in18,
                @ArgumentHint(TABLE_AS_SET) Row in19,
                @ArgumentHint(TABLE_AS_SET) Row in20,
                @ArgumentHint(TABLE_AS_SET) Row in21)
                throws Exception {}
    }

    private static class ErrorSpec {
        private final String description;
        private final Class<? extends UserDefinedFunction> functionClass;
        private final String selectSql;
        private final String insertIntoSql;
        private final String errorMessage;

        private ErrorSpec(
                String description,
                Class<? extends UserDefinedFunction> functionClass,
                String selectSql,
                String insertIntoSql,
                String errorMessage) {
            this.description = description;
            this.functionClass = functionClass;
            this.selectSql = selectSql;
            this.insertIntoSql = insertIntoSql;
            this.errorMessage = errorMessage;
        }

        static ErrorSpec ofSelect(
                String description,
                Class<? extends UserDefinedFunction> functionClass,
                String selectSql,
                String errorMessage) {
            return new ErrorSpec(description, functionClass, selectSql, null, errorMessage);
        }

        static ErrorSpec ofInsertInto(
                String description,
                Class<? extends UserDefinedFunction> functionClass,
                String insertIntoSql,
                String errorMessage) {
            return new ErrorSpec(description, functionClass, null, insertIntoSql, errorMessage);
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
