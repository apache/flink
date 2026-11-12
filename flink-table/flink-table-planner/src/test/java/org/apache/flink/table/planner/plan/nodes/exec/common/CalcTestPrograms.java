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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc1;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc2;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.UdfWithOpen;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;

/** {@link TableTestProgram}s for testing {@link StreamExecCalc} and {@link BatchExecCalc}. */
public class CalcTestPrograms {

    private static final VariantBuilder VARIANT_BUILDER = Variant.newBuilder();

    // --------------------------------------------------------------------------------------------
    // With restore data
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram SIMPLE_CALC =
            TableTestProgram.of("calc-simple", "validates basic calc node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .producedBeforeRestore(Row.of(420L, 42.0))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .consumedBeforeRestore(Row.of(421L, 42.0))
                                    .consumedAfterRestore(Row.of(422L, 42.1))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a + 1, b FROM t")
                    .build();

    public static final TableTestProgram CALC_PARTIAL_DELETE_WITH_EXPRESSION_AND_FILTER =
            TableTestProgram.of(
                            "calc-partial-delete-with-expression-and-filter",
                            "validates that a calc forwarding partial deletes skips evaluating a"
                                    + " non-key row constructor expression for a delete-by-key row"
                                    + " after being restored from a compiled plan, with a key-safe"
                                    + " filter present")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "arr ARRAY<INT> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("source.produces-delete-by-key", "true")
                                    .producedBeforeRestore(
                                            // Filtered out by the WHERE clause below
                                            Row.ofKind(RowKind.INSERT, 0, new Integer[] {99}),
                                            Row.ofKind(RowKind.INSERT, 1, new Integer[] {1, 2}),
                                            Row.ofKind(RowKind.INSERT, 2, new Integer[] {3}))
                                    .producedAfterRestore(
                                            // Delete by key: NOT NULL array column is null
                                            Row.ofKind(RowKind.DELETE, 1, null),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, 2, new Integer[] {3, 4}))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "id INT PRIMARY KEY NOT ENFORCED",
                                            "r ROW<a INT, b ARRAY<INT>> NOT NULL")
                                    .addOption("changelog-mode", "I,UA,D")
                                    .addOption("sink.supports-delete-by-key", "true")
                                    .consumedBeforeRestore(
                                            "+I[1, +I[1, [1, 2]]]", "+I[2, +I[2, [3]]]")
                                    .consumedAfterRestore("-D[1, null]", "+U[2, +I[2, [3, 4]]]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT id, ROW(id, arr) FROM source_t WHERE id > 0")
                    .build();

    public static final TableTestProgram CALC_PROJECT_PUSHDOWN =
            TableTestProgram.of(
                            "calc-project-pushdown", "validates calc node with project pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(421L, 42.1))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "a1 VARCHAR")
                                    .consumedBeforeRestore(Row.of(421L, "421"))
                                    .consumedAfterRestore(Row.of(421L, "421"))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, CAST(a AS VARCHAR) FROM source_t WHERE a > CAST(1 AS BIGINT)")
                    .build();

    public static final TableTestProgram CALC_FILTER =
            TableTestProgram.of("calc-filter", "validates calc node with filter")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b INT", "c DOUBLE", "d VARCHAR")
                                    .producedBeforeRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .producedAfterRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b INT", "c DOUBLE", "d VARCHAR")
                                    .consumedBeforeRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .consumedAfterRestore(Row.of(420L, 1, 42.0, "hello"))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * FROM source_t WHERE b > 0")
                    .build();

    public static final TableTestProgram CALC_FILTER_PUSHDOWN =
            TableTestProgram.of("calc-filter-pushdown", "validates calc node with filter pushdown")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(421L, 42.1))
                                    .producedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT", "b DOUBLE")
                                    .consumedBeforeRestore(Row.of(421L, 42.1))
                                    .consumedAfterRestore(Row.of(421L, 42.1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a, b FROM source_t WHERE a > CAST(420 AS BIGINT)")
                    .build();

    public static final TableTestProgram CALC_SARG =
            TableTestProgram.of("calc-sarg", "validates calc node with Sarg")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .addOption("filterable-fields", "a")
                                    .producedBeforeRestore(Row.of(1))
                                    .producedAfterRestore(Row.of(1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT")
                                    .consumedBeforeRestore(Row.of(1))
                                    .consumedAfterRestore(Row.of(1))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a FROM source_t WHERE a = 1 or a = 2 or a is null")
                    .build();

    public static final TableTestProgram CALC_UDF_SIMPLE =
            TableTestProgram.of("calc-udf-simple", "validates calc node with simple UDF")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT")
                                    .producedBeforeRestore(Row.of(5))
                                    .producedAfterRestore(Row.of(5))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "a1 BIGINT")
                                    .consumedBeforeRestore(Row.of(5, 6L))
                                    .consumedAfterRestore(Row.of(5, 6L))
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT a, udf1(a) FROM source_t")
                    .build();

    public static final TableTestProgram CALC_UDF_COMPLEX =
            TableTestProgram.of("calc-udf-complex", "validates calc node with complex UDFs")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .setupTemporaryCatalogFunction("udf2", JavaFunc1.class)
                    .setupTemporarySystemFunction("udf3", JavaFunc2.class)
                    .setupTemporarySystemFunction("udf4", UdfWithOpen.class)
                    .setupCatalogFunction("udf5", JavaFunc5.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a BIGINT, b INT NOT NULL, c VARCHAR, d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    5L,
                                                    11,
                                                    "hello world",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 1, 123)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    5L,
                                                    11,
                                                    "hello world",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 1, 123)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "a BIGINT",
                                            "a1 VARCHAR",
                                            "b INT NOT NULL",
                                            "b1 VARCHAR",
                                            "c1 VARCHAR",
                                            "c2 VARCHAR",
                                            "d1 TIMESTAMP(3)")
                                    .consumedBeforeRestore(
                                            Row.of(
                                                    5L,
                                                    "5",
                                                    11,
                                                    "11 and 11 and 1702688461000",
                                                    "hello world11",
                                                    "$hello",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 0, 0)))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    5L,
                                                    "5",
                                                    11,
                                                    "11 and 11 and 1702688461000",
                                                    "hello world11",
                                                    "$hello",
                                                    LocalDateTime.of(2023, 12, 16, 1, 1, 0, 0)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "a, "
                                    + "cast(a as VARCHAR) as a1, "
                                    + "b, "
                                    + "udf2(b, b, d) as b1, "
                                    + "udf3(c, b) as c1, "
                                    + "udf4(substring(c, 1, 5)) as c2, "
                                    + "udf5(d, 1000) as d1 "
                                    + "from source_t where "
                                    + "(udf1(a) > 0 or (a * b) < 100) and b > 10")
                    .build();

    public static final TableTestProgram CALC_CURRENT_TIMESTAMP =
            TableTestProgram.of(
                            "calc-current-timestamp", "validates basic calc with current timestamp")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("a BIGINT")
                                    .producedBeforeRestore(Row.of(100L))
                                    .producedAfterRestore(Row.of(10000L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a BIGINT")
                                    .consumedBeforeRestore(Row.of(20L))
                                    .consumedAfterRestore(Row.of(0L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT extract(year from current_timestamp) / a FROM t")
                    .build();

    public static final TableTestProgram CALC_VARIANT =
            TableTestProgram.of("calc-variant", "validates calc node with VARIANT type")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("s STRING", "v VARIANT")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "{\"a\":1}",
                                                    VARIANT_BUILDER
                                                            .object()
                                                            .add("k", VARIANT_BUILDER.of(1))
                                                            .build()))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "{\"a\":2}",
                                                    VARIANT_BUILDER
                                                            .object()
                                                            .add("k", VARIANT_BUILDER.of(2))
                                                            .build()))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "parsed VARIANT", "try_parsed VARIANT", "field VARIANT")
                                    .consumedBeforeRestore("+I[{\"a\":1}, {\"a\":1}, 1]")
                                    .consumedAfterRestore("+I[{\"a\":2}, {\"a\":2}, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT PARSE_JSON(s), TRY_PARSE_JSON(s), v['k'] FROM t")
                    .build();

    // --------------------------------------------------------------------------------------------
    // Without restore data
    // --------------------------------------------------------------------------------------------

    public static final TableTestProgram CURRENT_WATERMARK =
            TableTestProgram.of(
                            "calc-current-watermark", "validates the CURRENT_WATERMARK function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "WATERMARK FOR ts AS ts")
                                    .producedValues(
                                            Row.of("Bob", Instant.ofEpochMilli(0)),
                                            Row.of("Bob", Instant.ofEpochMilli(1)),
                                            Row.of("Alice", Instant.ofEpochMilli(2)),
                                            Row.of("Bob", Instant.ofEpochMilli(3)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "name STRING",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "w TIMESTAMP_LTZ(3)")
                                    .consumedValues(
                                            "+I[Bob, 1970-01-01T00:00:00Z, null]",
                                            "+I[Bob, 1970-01-01T00:00:00.001Z, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, 1970-01-01T00:00:00.002Z, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, 1970-01-01T00:00:00.003Z, 1970-01-01T00:00:00.002Z]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT name, ts, CURRENT_WATERMARK(ts) AS w FROM t")
                    .build();

    public static final TableTestProgram COALESCE_NESTED_ROW_LEFT_JOIN =
            TableTestProgram.of(
                            "calc-coalesce-nested-row-left-join",
                            "validates coalesce on nested ROW field from LEFT JOIN")
                    .setupTableSource(
                            SourceTestStep.newBuilder("orders")
                                    .addSchema(
                                            "`order_id` BIGINT NOT NULL",
                                            "`amount` DOUBLE",
                                            "PRIMARY KEY (`order_id`) NOT ENFORCED")
                                    .producedValues(Row.of(1L, 10.0), Row.of(2L, 20.0))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("order_details_row")
                                    .addSchema(
                                            "`r` ROW<`order_id` BIGINT NOT NULL, `name` STRING NOT NULL> NOT NULL",
                                            "`detail` STRING",
                                            "PRIMARY KEY (`r`) NOT ENFORCED")
                                    .producedValues(Row.of(Row.of(1L, "first"), "d1"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("coalesce_sink")
                                    .addSchema("order_id_str STRING")
                                    .testMaterializedData()
                                    .consumedValues("+I[1]", "+I[2]")
                                    .build())
                    .runSql(
                            "INSERT INTO coalesce_sink "
                                    + "SELECT CAST(COALESCE(b.r.order_id, a.order_id) AS STRING) AS order_id_str "
                                    + "FROM orders a LEFT JOIN order_details_row b "
                                    + "ON a.order_id = b.r.order_id")
                    .build();

    // fails for '{}', "1" for '{"x":"1"}'
    private static final String JSON_X =
            "JSON_VALUE(REGEXP_REPLACE(body, 'p', 'q'), '$.x' ERROR ON ERROR)";

    private static SourceTestStep jsonBodySource(final String... bodies) {
        return SourceTestStep.newBuilder("t")
                .addSchema("body STRING")
                .producedValues(Arrays.stream(bodies).map(Row::of).toArray(Row[]::new))
                .build();
    }

    public static final TableTestProgram JSON_VALUE_LAZY_IN_CASE =
            TableTestProgram.of(
                            "calc-json-value-lazy-in-case",
                            "validates that a value in one CASE branch is not computed for other rows")
                    .setupTableSource(jsonBodySource("{}", "{\"x\":\"1\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r STRING")
                                    .consumedValues("+I[null]", "+I[1]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CASE WHEN a THEN v END FROM (SELECT "
                                    + JSON_X
                                    + " AS v, body LIKE '%x%' AS a FROM t)")
                    .build();

    public static final TableTestProgram JSON_VALUE_LAZY_IN_AND =
            TableTestProgram.of(
                            "calc-json-value-lazy-in-and",
                            "validates that a value in a later AND operand is not computed for other rows")
                    .setupTableSource(jsonBodySource("{}", "{\"x\":\"1\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("body STRING")
                                    .consumedValues("+I[{\"x\":\"1\"}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT body FROM (SELECT "
                                    + JSON_X
                                    + " AS v, body LIKE '%x%' AS a, body FROM t) "
                                    + "WHERE a AND v = '1'")
                    .build();

    public static final TableTestProgram JSON_VALUE_ACROSS_CASES =
            TableTestProgram.of(
                            "calc-json-value-across-cases",
                            "validates a value shared by two CASE expressions")
                    .setupTableSource(jsonBodySource("{}", "{\"x\":\"1\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 STRING", "r2 STRING")
                                    .consumedValues("+I[null, null]", "+I[1, 1x]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CASE WHEN a THEN v END, "
                                    + "CASE WHEN a THEN CONCAT(v, 'x') END FROM (SELECT "
                                    + JSON_X
                                    + " AS v, body LIKE '%x%' AS a FROM t)")
                    .build();

    public static final TableTestProgram JSON_VALUE_NULL_ACROSS_CASES =
            TableTestProgram.of(
                            "calc-json-value-null-across-cases",
                            "validates a shared value that is NULL for some records")
                    .setupTableSource(jsonBodySource("{\"x\":\"1\"}", "{\"x\":\"1\",\"y\":\"2\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 STRING", "r2 STRING")
                                    .consumedValues("+I[null, none]", "+I[2, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CASE WHEN a THEN v END, "
                                    + "CASE WHEN a THEN COALESCE(v, 'none') END FROM (SELECT "
                                    + "JSON_VALUE(REGEXP_REPLACE(body, 'p', 'q'), '$.y') AS v, "
                                    + "body LIKE '%x%' AS a FROM t)")
                    .build();

    public static final TableTestProgram JSON_VALUE_INT_ACROSS_CASES =
            TableTestProgram.of(
                            "calc-json-value-int-across-cases",
                            "validates a shared primitive value, also when it is NULL")
                    .setupTableSource(jsonBodySource("{\"n\":1}", "{\"n\":2}", "{}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 INT", "r2 INT")
                                    .consumedValues("+I[1, 11]", "+I[2, 12]", "+I[null, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CASE WHEN a THEN v END, "
                                    + "CASE WHEN a THEN v + 10 END FROM (SELECT "
                                    + "JSON_VALUE(REGEXP_REPLACE(body, 'p', 'q'), '$.n' RETURNING INT) "
                                    + "AS v, body LIKE '%{%' AS a FROM t)")
                    .build();

    public static final TableTestProgram JSON_VALUE_IN_OR_AND_IFNULL =
            TableTestProgram.of(
                            "calc-json-value-in-or-and-ifnull",
                            "validates a value shared by a later OR operand and an IFNULL fallback")
                    .setupTableSource(jsonBodySource("{}", "{\"x\":\"1\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 BOOLEAN", "r2 STRING")
                                    .consumedValues("+I[true, c]", "+I[true, 1]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a OR v = '1', IFNULL(c, v) FROM (SELECT "
                                    + JSON_X
                                    + " AS v, body = '{}' AS a, "
                                    + "CASE WHEN body = '{}' THEN 'c' END AS c FROM t)")
                    .build();

    public static final TableTestProgram JSON_VALUE_IN_NESTED_CASE =
            TableTestProgram.of(
                            "calc-json-value-in-nested-case",
                            "validates a shared value used in a CASE nested in another branch")
                    .setupTableSource(jsonBodySource("{}", "{\"x\":\"1\"}", "{\"x\":\"2\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 STRING", "r2 STRING")
                                    .consumedValues("+I[o, null]", "+I[1, 1x]", "+I[n, 2x]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "CASE WHEN a THEN CASE WHEN b THEN v ELSE 'n' END ELSE 'o' END, "
                                    + "CASE WHEN a THEN CONCAT(v, 'x') END FROM (SELECT "
                                    + JSON_X
                                    + " AS v, body LIKE '%x%' AS a, body LIKE '%1%' AS b FROM t)")
                    .build();

    public static final TableTestProgram JSON_VALUE_IN_FILTER_OR =
            TableTestProgram.of(
                            "calc-json-value-in-filter-or",
                            "validates a value shared by two later OR operands of a filter")
                    .setupTableSource(
                            jsonBodySource("{}", "{\"x\":\"1\"}", "{\"x\":\"3\"}", "{\"x\":\"4\"}"))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("body STRING")
                                    .consumedValues(
                                            "+I[{}]", "+I[{\"x\":\"1\"}]", "+I[{\"x\":\"3\"}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT body FROM (SELECT "
                                    + JSON_X
                                    + " AS v, body = '{}' AS a, body FROM t) "
                                    + "WHERE a OR v = '1' OR CONCAT(v, 'y') = '3y'")
                    .build();

    public static final TableTestProgram JSON_VALUE_IN_LOOKUP_JOIN_FILTER =
            TableTestProgram.of(
                            "calc-json-value-in-lookup-join-filter",
                            "validates a value shared by two branches of a filter on the lookup side")
                    .setupTableSource(
                            SourceTestStep.newBuilder("src_t")
                                    .addSchema("id INT", "proc_time AS PROCTIME()")
                                    .producedValues(Row.of(1), Row.of(2), Row.of(3), Row.of(4))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("dim_t")
                                    .addOption("disable-lookup", "false")
                                    .addSchema("id INT", "j STRING")
                                    .producedValues(
                                            Row.of(1, "{\"b\":\"q\"}"),
                                            Row.of(2, "{\"b\":\"q\"}"),
                                            Row.of(3, "{\"b\":\"r\"}"),
                                            Row.of(4, "{\"b\":\"x\"}"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("id INT", "j STRING")
                                    .consumedValues("+I[2, {\"b\":\"q\"}]", "+I[3, {\"b\":\"r\"}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT S.id, D.j FROM src_t AS S "
                                    + "JOIN dim_t FOR SYSTEM_TIME AS OF S.proc_time AS D "
                                    + "ON S.id = D.id AND CASE "
                                    + "WHEN D.id = 2 THEN UPPER(JSON_VALUE(D.j, '$.b')) = 'Q' "
                                    + "WHEN D.id = 3 THEN UPPER(JSON_VALUE(D.j, '$.b')) = 'R' "
                                    + "ELSE FALSE END")
                    .build();

    public static final TableTestProgram MEMOIZED_VALUE_BELOW_NON_DETERMINISTIC_CALL =
            TableTestProgram.of(
                            "calc-memoized-value-below-non-deterministic-call",
                            "validates a memoized value first called under UUID() that reads the shared TRIM(j)")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("id INT", "j STRING")
                                    .producedValues(
                                            Row.of(1, " p "), Row.of(2, " q "), Row.of(3, " r "))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 INT", "r2 STRING", "r3 STRING")
                                    .consumedValues(
                                            "+I[null, null, p]", "+I[37, null, q]", "+I[37, R!, r]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "CHAR_LENGTH(CONCAT(CASE WHEN id > 1 THEN UPPER(TRIM(j)) END, UUID())), "
                                    + "CASE WHEN id > 2 THEN CONCAT(UPPER(TRIM(j)), '!') END, "
                                    + "TRIM(j) FROM t")
                    .build();

    public static final TableTestProgram SEARCH_ON_PROJECTED_VALUE_STAYS_LAZY =
            TableTestProgram.of(
                            "calc-search-on-projected-value-stays-lazy",
                            "validates that a filter SEARCH on a projected value does not compute "
                                    + "projection values for rows the filter drops")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("n INT", "v STRING")
                                    .producedValues(Row.of(null, "2"), Row.of(5, "x"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 INT", "r2 INT")
                                    .consumedValues("+I[2, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CAST(v AS INT), COALESCE(n, CAST(v AS INT)) "
                                    + "FROM t WHERE COALESCE(n, CAST(v AS INT)) IN (1, 2)")
                    .build();

    public static final TableTestProgram TIME_ATTRIBUTES_IN_TWO_BRANCHES =
            TableTestProgram.of(
                            "calc-time-attributes-in-two-branches",
                            "validates that a rowtime value is shared by two branches, while "
                                    + "PROCTIME() reads the context and is not deterministic")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "id INT",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "WATERMARK FOR ts AS ts")
                                    .producedValues(
                                            Row.of(1, Instant.ofEpochMilli(1000L)),
                                            Row.of(2, Instant.ofEpochMilli(2000L)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 INT", "r2 INT", "r3 BOOLEAN")
                                    .consumedValues("+I[null, null, true]", "+I[2, 3, true]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "CASE WHEN id > 1 THEN TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(0, 3), ts) END, "
                                    + "CASE WHEN id > 1 THEN TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(0, 3), ts) + 1 END, "
                                    + "CASE WHEN id > 1 THEN CAST(PROCTIME() AS STRING) "
                                    + "ELSE CAST(PROCTIME() AS STRING) END IS NOT NULL FROM t")
                    .build();

    public static final TableTestProgram FIELD_ACCESS_IN_FILTER_MEMOIZED_VALUE =
            TableTestProgram.of(
                            "calc-field-access-in-filter-memoized-value",
                            "validates a column read by the filter's memoized value only through "
                                    + "a field access")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("x INT", "r ROW<a STRING>")
                                    .producedValues(
                                            Row.of(1, Row.of("a")),
                                            Row.of(2, Row.of("a")),
                                            Row.of(1, Row.of("b")),
                                            Row.of(-1, Row.of("b")))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("r1 STRING")
                                    .consumedValues("+I[(a)]", "+I[(a)!]", "+I[null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT CASE WHEN x = 1 THEN CAST(r AS STRING) "
                                    + "WHEN x = 2 THEN CONCAT(CAST(r AS STRING), '!') END FROM t "
                                    + "WHERE (x > 0 AND UPPER(r.a) = 'A') "
                                    + "OR (x < 0 AND UPPER(r.a) = 'B')")
                    .build();

    public static final TableTestProgram COALESCE =
            TableTestProgram.of("calc-coalesce", "validates coalesce node")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "a DECIMAL(2, 1)",
                                            "b DECIMAL(4, 2)",
                                            "c TIMESTAMP(0)",
                                            "d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    null,
                                                    new BigDecimal("11.22"),
                                                    null,
                                                    LocalDateTime.of(
                                                            1970, 1, 1, 0, 0, 0, 123_000_000)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    new BigDecimal("5.3"),
                                                    null,
                                                    LocalDateTime.of(2000, 2, 2, 2, 2, 2),
                                                    null))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("x DECIMAL(4, 2)", "y TIMESTAMP(3)")
                                    .consumedBeforeRestore(
                                            Row.of(
                                                    new BigDecimal("11.22"),
                                                    LocalDateTime.of(
                                                            1970, 1, 1, 0, 0, 0, 123_000_000)))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    new BigDecimal("5.30"),
                                                    LocalDateTime.of(2000, 2, 2, 2, 2, 2)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT COALESCE(a, b) AS x, COALESCE(c, d) AS y FROM t")
                    .build();
}
