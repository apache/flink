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

package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.ifThenElse;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.row;

/**
 * Collection of {@link TableTestProgram TableTestPrograms} for basic {@link QueryOperation
 * QueryOperations}.
 */
@Internal
public class QueryOperationTestPrograms {
    static final TableTestProgram SOURCE_QUERY_OPERATION =
            TableTestProgram.of("source-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(Row.of(1L, "abc"), Row.of(2L, "cde"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(1L, "abc"), Row.of(2L, "cde"))
                                    .build())
                    .runTableApi(t -> t.from("s"), "sink")
                    .runSql("SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`")
                    .build();

    static final TableTestProgram VALUES_QUERY_OPERATION =
            TableTestProgram.of("values-query-operation", "verifies sql serialization")
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(1L, "abc"), Row.of(2L, "cde"))
                                    .build())
                    .runTableApi(t -> t.fromValues(row(1L, "abc"), row(2L, "cde")), "sink")
                    .runSql(
                            "SELECT `f0`, `f1` FROM (VALUES \n"
                                    + "    (CAST(1 AS BIGINT), 'abc'),\n"
                                    + "    (CAST(2 AS BIGINT), 'cde')\n"
                                    + ") VAL$0(`f0`, `f1`)")
                    .build();

    static final TableTestProgram FILTER_QUERY_OPERATION =
            TableTestProgram.of("filter-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(Row.of(10L, "abc"), Row.of(20L, "cde"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(20L, "cde"))
                                    .build())
                    .runTableApi(t -> t.from("s").where($("a").isGreaterOrEqual(15)), "sink")
                    .runSql(
                            "SELECT `a`, `b` FROM (\n"
                                    + "    SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`\n"
                                    + ") WHERE `a` >= 15")
                    .build();

    static final TableTestProgram DISTINCT_QUERY_OPERATION =
            TableTestProgram.of("distinct-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(
                                            Row.of(20L, "apple"),
                                            Row.of(20L, "apple"),
                                            Row.of(5L, "pear"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(20L, "apple"))
                                    .build())
                    .runTableApi(
                            t -> t.from("s").where($("a").isGreaterOrEqual(15)).distinct(), "sink")
                    .runSql(
                            "SELECT DISTINCT `a`, `b` FROM (\n"
                                    + "    SELECT `a`, `b` FROM (\n"
                                    + "        SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`\n"
                                    + "    ) WHERE `a` >= 15\n"
                                    + ")")
                    .build();

    static final TableTestProgram AGGREGATE_QUERY_OPERATION =
            TableTestProgram.of("aggregate-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(
                                            Row.of(10L, "apple"),
                                            Row.of(20L, "apple"),
                                            Row.of(5L, "pear"),
                                            Row.of(15L, "pear"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a string", "b bigint")
                                    .consumedValues(Row.of("apple", 30L), Row.of("pear", 20L))
                                    .build())
                    .runTableApi(
                            t -> t.from("s").groupBy($("b")).select($("b"), $("a").sum()), "sink")
                    .runSql(
                            "SELECT `b`, `EXPR$0` FROM (\n"
                                    + "    SELECT `b`, (SUM(`a`)) AS `EXPR$0` FROM (\n"
                                    + "        SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`\n"
                                    + "    )\n"
                                    + "    GROUP BY `b`\n"
                                    + ")")
                    .build();

    static final TableTestProgram WINDOW_AGGREGATE_QUERY_OPERATION =
            TableTestProgram.of("window-aggregate-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema(
                                            "a bigint",
                                            "b string",
                                            "ts TIMESTAMP_LTZ(3)",
                                            "WATERMARK FOR ts AS ts - INTERVAL '1' SECOND")
                                    .producedValues(
                                            Row.of(2L, "apple", dayOfSeconds(0)),
                                            Row.of(3L, "apple", dayOfSeconds(4)),
                                            Row.of(1L, "apple", dayOfSeconds(7)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a string", "ts TIMESTAMP_LTZ(3)", "b bigint")
                                    .consumedValues(
                                            Row.of("apple", dayOfSeconds(0), 5L),
                                            Row.of("apple", dayOfSeconds(5), 1L))
                                    .build())
                    .runTableApi(
                            t ->
                                    t.from("s")
                                            .window(
                                                    Tumble.over(lit(5).seconds())
                                                            .on($("ts"))
                                                            .as("w"))
                                            .groupBy($("w"), $("b"))
                                            .select($("b"), $("w").start(), $("a").sum()),
                            "sink")
                    .runSql(
                            "SELECT `b`, `EXPR$0`, `EXPR$1` FROM (\n"
                                    + "    SELECT `b`, (SUM(`a`)) AS `EXPR$1`, (window_start) AS `EXPR$0` FROM TABLE(\n"
                                    + "        TUMBLE((\n"
                                    + "            SELECT `a`, `b`, `ts` FROM `default_catalog`.`default_database`.`s`\n"
                                    + "        ), DESCRIPTOR(`ts`), INTERVAL '0 00:00:05.0' DAY TO SECOND(3))\n"
                                    + "    ) GROUP BY window_start, window_end, `b`\n"
                                    + ")")
                    .build();

    private static Instant dayOfSeconds(int second) {
        return LocalDateTime.of(2024, 1, 1, 0, 0, second).atZone(ZoneId.of("UTC")).toInstant();
    }

    static final TableTestProgram JOIN_QUERY_OPERATION =
            TableTestProgram.of("join-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("d")
                                    .addSchema("dept_id bigint", "d_name string")
                                    .producedValues(
                                            Row.of(1L, "Research"), Row.of(2L, "Accounting"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("e")
                                    .addSchema(
                                            "emp_id bigint",
                                            "e_dept_id bigint",
                                            "name string",
                                            "age int")
                                    .producedValues(
                                            Row.of(1L, 2L, "Steve", 18),
                                            Row.of(2L, 1L, "Helena", 22),
                                            Row.of(3L, 2L, "Charlie", 25),
                                            Row.of(4L, 1L, "Anna", 18))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("name string", "dept_name string", "age bigint")
                                    .consumedValues(
                                            Row.of("Helena", "Research", 22L),
                                            Row.of("Charlie", "Accounting", 25L))
                                    .build())
                    .runTableApi(
                            t ->
                                    t.from("e")
                                            .join(
                                                    t.from("d"),
                                                    $("e_dept_id")
                                                            .isEqual($("dept_id"))
                                                            .and($("age").isGreaterOrEqual(21)))
                                            .select($("name"), $("d_name"), $("age")),
                            "sink")
                    .runSql(
                            "SELECT `name`, `d_name`, `age` FROM (\n"
                                    + "    SELECT `emp_id`, `e_dept_id`, `name`, `age`, `dept_id`, `d_name` FROM (\n"
                                    + "        SELECT `emp_id`, `e_dept_id`, `name`, `age` FROM `default_catalog`.`default_database`.`e`\n"
                                    + "    ) INNER JOIN (\n"
                                    + "        SELECT `dept_id`, `d_name` FROM `default_catalog`.`default_database`.`d`\n"
                                    + "    ) ON (`e_dept_id` = `dept_id`) AND (`age` >= 21)\n"
                                    + ")")
                    .build();

    static final TableTestProgram LATERAL_JOIN_QUERY_OPERATION =
            TableTestProgram.of("lateral-join-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("e")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(Row.of(1L, "abc"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string", "f0 int")
                                    .consumedValues(Row.of(1L, "abc", 3))
                                    .build())
                    .setupCatalogFunction(
                            "udtf", JavaUserDefinedTableFunctions.JavaTableFunc1.class)
                    .runTableApi(
                            t -> t.from("e").joinLateral(call("udtf", $("b")).as("f0")), "sink")
                    .runSql(
                            "SELECT `a`, `b`, `f0` FROM (\n"
                                    + "    SELECT `a`, `b` FROM `default_catalog`.`default_database`.`e`\n"
                                    + ") INNER JOIN \n"
                                    + "    LATERAL TABLE(`default_catalog`.`default_database`.`udtf`(`b`)) T$0(`f0`) ON TRUE")
                    .build();

    static final TableTestProgram UNION_ALL_QUERY_OPERATION =
            TableTestProgram.of("union-all-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(Row.of(1L, "abc"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(Row.of(2L, "cde"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(1L, "abc"), Row.of(2L, "cde"))
                                    .build())
                    .runTableApi(t -> t.from("s").unionAll(t.from("t")), "sink")
                    .runSql(
                            "SELECT `a`, `b` FROM (\n"
                                    + "    SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`\n"
                                    + ") UNION ALL (\n"
                                    + "    SELECT `a`, `b` FROM `default_catalog`.`default_database`.`t`\n"
                                    + ")")
                    .build();

    static final TableTestProgram ORDER_BY_QUERY_OPERATION =
            TableTestProgram.of("order-by-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(
                                            Row.of(1L, "a"),
                                            Row.of(2L, "b"),
                                            Row.of(3L, "c"),
                                            Row.of(4L, "d"),
                                            Row.of(5L, "e"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(2L, "b"), Row.of(3L, "c"))
                                    .build())
                    .runTableApi(
                            t -> t.from("s").orderBy($("a"), $("b").desc()).offset(1).fetch(2),
                            "sink")
                    .runSql(
                            "SELECT `a`, `b` FROM (\n"
                                    + "    SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`\n"
                                    + ") ORDER BY `a` ASC, `b` DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")
                    .build();

    static final TableTestProgram SQL_QUERY_OPERATION =
            TableTestProgram.of("sql-query-operation", "verifies sql serialization")
                    .setupTableSource(
                            SourceTestStep.newBuilder("s")
                                    .addSchema("a bigint", "b string")
                                    .producedValues(Row.of(1L, "abc"), Row.of(2L, "cde"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("a bigint", "b string")
                                    .consumedValues(Row.of(3L, "bc"), Row.of(4L, "de"))
                                    .build())
                    .runTableApi(
                            t ->
                                    t.sqlQuery("SELECT a, b FROM s")
                                            .select($("a").plus(2), $("b").substr(2, 3)),
                            "sink")
                    .runSql(
                            "SELECT (`a` + 2) AS `_c0`, (SUBSTR(`b`, 2, 3)) AS `_c1` FROM (\n"
                                    + "    SELECT `s`.`a`, `s`.`b`\n"
                                    + "    FROM `default_catalog`.`default_database`.`s` AS `s`\n"
                                    + ")")
                    .build();

    static final TableTestProgram GROUP_HOP_WINDOW_EVENT_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-hop-event-time",
                            "validates group by using hopping window with event time")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "ts STRING",
                                            "a_int INT",
                                            "b_double DOUBLE",
                                            "c_float FLOAT",
                                            "d_bigdec DECIMAL(10, 2)",
                                            "`comment` STRING",
                                            "name STRING",
                                            "`rowtime` AS TO_TIMESTAMP(`ts`)",
                                            "`proctime` AS PROCTIME()",
                                            "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "2020-10-10 00:00:01",
                                                    1,
                                                    1d,
                                                    1f,
                                                    new BigDecimal("1.11"),
                                                    "Hi",
                                                    "a"),
                                            Row.of(
                                                    "2020-10-10 00:00:02",
                                                    2,
                                                    2d,
                                                    2f,
                                                    new BigDecimal("2.22"),
                                                    "Comment#1",
                                                    "a"),
                                            Row.of(
                                                    "2020-10-10 00:00:03",
                                                    2,
                                                    2d,
                                                    2f,
                                                    new BigDecimal("2.22"),
                                                    "Comment#1",
                                                    "a"),
                                            Row.of(
                                                    "2020-10-10 00:00:04",
                                                    5,
                                                    5d,
                                                    5f,
                                                    new BigDecimal("5.55"),
                                                    null,
                                                    "a"),
                                            Row.of(
                                                    "2020-10-10 00:00:07",
                                                    3,
                                                    3d,
                                                    3f,
                                                    null,
                                                    "Hello",
                                                    "b"),
                                            // out of order
                                            Row.of(
                                                    "2020-10-10 00:00:06",
                                                    6,
                                                    6d,
                                                    6f,
                                                    new BigDecimal("6.66"),
                                                    "Hi",
                                                    "b"),
                                            Row.of(
                                                    "2020-10-10 00:00:08",
                                                    3,
                                                    null,
                                                    3f,
                                                    new BigDecimal("3.33"),
                                                    "Comment#2",
                                                    "a"),
                                            // late event
                                            Row.of(
                                                    "2020-10-10 00:00:04",
                                                    5,
                                                    5d,
                                                    null,
                                                    new BigDecimal("5.55"),
                                                    "Hi",
                                                    "a"),
                                            Row.of(
                                                    "2020-10-10 00:00:16",
                                                    4,
                                                    4d,
                                                    4f,
                                                    new BigDecimal("4.44"),
                                                    "Hi",
                                                    "b"),
                                            Row.of(
                                                    "2020-10-10 00:00:32",
                                                    7,
                                                    7d,
                                                    7f,
                                                    new BigDecimal("7.77"),
                                                    null,
                                                    null),
                                            Row.of(
                                                    "2020-10-10 00:00:34",
                                                    1,
                                                    3d,
                                                    3f,
                                                    new BigDecimal("3.33"),
                                                    "Comment#3",
                                                    "b"))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "2020-10-10 00:00:41",
                                                    10,
                                                    3d,
                                                    3f,
                                                    new BigDecimal("4.44"),
                                                    "Comment#4",
                                                    "a"),
                                            Row.of(
                                                    "2020-10-10 00:00:42",
                                                    11,
                                                    4d,
                                                    4f,
                                                    new BigDecimal("5.44"),
                                                    "Comment#5",
                                                    "d"),
                                            Row.of(
                                                    "2020-10-10 00:00:43",
                                                    12,
                                                    5d,
                                                    5f,
                                                    new BigDecimal("6.44"),
                                                    "Comment#6",
                                                    "c"),
                                            Row.of(
                                                    "2020-10-10 00:00:44",
                                                    13,
                                                    6d,
                                                    6f,
                                                    new BigDecimal("7.44"),
                                                    "Comment#7",
                                                    "d"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "cnt BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[a, 4]",
                                            "+I[b, 2]",
                                            "+I[a, 6]",
                                            "+I[a, 1]",
                                            "+I[b, 2]",
                                            "+I[b, 1]",
                                            "+I[b, 1]")
                                    .consumedAfterRestore(
                                            "+I[b, 1]",
                                            "+I[null, 1]",
                                            "+I[b, 1]",
                                            "+I[null, 1]",
                                            "+I[a, 1]",
                                            "+I[d, 2]",
                                            "+I[c, 1]",
                                            "+I[a, 1]",
                                            "+I[c, 1]",
                                            "+I[d, 2]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("source_t")
                                            .window(
                                                    Slide.over(lit(10).seconds())
                                                            .every(lit(5).seconds())
                                                            .on($("rowtime"))
                                                            .as("w"))
                                            .groupBy($("name"), $("w"))
                                            .select($("name"), lit(1).count()),
                            "sink_t")
                    .build();

    static final TableTestProgram SORT_LIMIT_DESC =
            TableTestProgram.of(
                            "sort-limit-desc",
                            "validates sort limit node by sorting integers in desc mode")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .producedBeforeRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .producedAfterRestore(
                                            // ignored since smaller than the least max (4, b, 8)
                                            Row.of(2, "a", 6),
                                            // replaces (4, b, 8) from beforeRestore
                                            Row.of(6, "c", 10),
                                            // ignored since not larger than the least max (5, c, 9)
                                            Row.of(5, "c", 9))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b VARCHAR", "c BIGINT")
                                    // heap state
                                    //      [4, b, 8]
                                    // [5, c, 9]  [6, c, 10]
                                    .consumedBeforeRestore(
                                            "+I[2, a, 6]",
                                            "+I[4, b, 8]",
                                            "+I[6, c, 10]",
                                            "-D[2, a, 6]",
                                            "+I[3, b, 7]",
                                            "-D[3, b, 7]",
                                            "+I[5, c, 9]")
                                    // heap state
                                    //       [5, c, 9]
                                    // [6, c, 10]  [6, c, 10]
                                    .consumedAfterRestore("-D[4, b, 8]", "+I[6, c, 10]")
                                    .build())
                    .runTableApi(
                            env -> env.from("source_t").orderBy($("a").desc()).limit(3), "sink_t")
                    .build();

    static final TableTestProgram GROUP_BY_UDF_WITH_MERGE =
            TableTestProgram.of(
                            "group-aggregate-udf-with-merge",
                            "validates udfs with merging using group by")
                    .setupCatalogFunction(
                            "my_avg", JavaUserDefinedAggFunctions.WeightedAvgWithMerge.class)
                    .setupTemporarySystemFunction(
                            "my_concat",
                            JavaUserDefinedAggFunctions.ConcatDistinctAggFunction.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(
                                            "a INT", "b BIGINT", "c INT", "d VARCHAR", "e BIGINT")
                                    .producedBeforeRestore(
                                            Row.of(2, 3L, 2, "Hello World Like", 1L),
                                            Row.of(3, 4L, 3, "Hello World Its nice", 2L),
                                            Row.of(2, 2L, 1, "Hello World", 2L),
                                            Row.of(1, 1L, 0, "Hello", 1L),
                                            Row.of(5, 11L, 10, "GHI", 1L),
                                            Row.of(3, 5L, 4, "ABC", 2L),
                                            Row.of(4, 10L, 9, "FGH", 2L),
                                            Row.of(4, 7L, 6, "CDE", 2L),
                                            Row.of(5, 14L, 13, "JKL", 2L),
                                            Row.of(4, 9L, 8, "EFG", 1L),
                                            Row.of(5, 15L, 14, "KLM", 2L),
                                            Row.of(5, 12L, 11, "HIJ", 3L),
                                            Row.of(4, 8L, 7, "DEF", 1L),
                                            Row.of(5, 13L, 12, "IJK", 3L),
                                            Row.of(3, 6L, 5, "BCD", 3L))
                                    .producedAfterRestore(
                                            Row.of(1, 1L, 0, "Hello", 1L),
                                            Row.of(3, 5L, 4, "ABC", 2L),
                                            Row.of(4, 10L, 9, "FGH", 2L),
                                            Row.of(4, 7L, 6, "CDE", 2L),
                                            Row.of(7, 7L, 7, "MNO", 7L),
                                            Row.of(3, 6L, 5, "BCD", 3L),
                                            Row.of(7, 7L, 7, "XYZ", 7L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "d BIGINT",
                                            "s1 BIGINT",
                                            "c1 VARCHAR",
                                            "PRIMARY KEY (d) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, Hello World Like]",
                                            "+I[2, 2, Hello World Its nice]",
                                            "+U[2, 2, Hello World Its nice|Hello World]",
                                            "+U[1, 1, Hello World Like|Hello]",
                                            "+U[1, 1, Hello World Like|Hello|GHI]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE|JKL]",
                                            "+U[1, 1, Hello World Like|Hello|GHI|EFG]",
                                            "+U[2, 2, Hello World Its nice|Hello World|ABC|FGH|CDE|JKL|KLM]",
                                            "+I[3, 3, HIJ]",
                                            "+U[1, 1, Hello World Like|Hello|GHI|EFG|DEF]",
                                            "+U[3, 3, HIJ|IJK]",
                                            "+U[3, 3, HIJ|IJK|BCD]")
                                    .consumedAfterRestore("+I[7, 7, MNO]", "+U[7, 7, MNO|XYZ]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("source_t")
                                            .groupBy($("e"))
                                            .select(
                                                    $("e"),
                                                    call("my_avg", $("e"), $("a")).as("s1"),
                                                    call("my_concat", $("d")).as("c1")),
                            "sink_t")
                    .build();

    static final TableTestProgram NON_WINDOW_INNER_JOIN =
            TableTestProgram.of("join-non-window-inner-join", "test non-window inner join")
                    .setupTableSource(
                            SourceTestStep.newBuilder("T1")
                                    .addSchema("a int", "b bigint", "c varchar")
                                    .producedBeforeRestore(
                                            Row.of(1, 1L, "Baker1"),
                                            Row.of(1, 2L, "Baker2"),
                                            Row.of(1, 2L, "Baker2"),
                                            Row.of(1, 5L, "Baker3"),
                                            Row.of(2, 7L, "Baker5"),
                                            Row.of(1, 9L, "Baker6"),
                                            Row.of(1, 8L, "Baker8"),
                                            Row.of(3, 8L, "Baker9"))
                                    .producedAfterRestore(Row.of(1, 1L, "PostRestore"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("T2")
                                    .addSchema("a int", "b bigint", "c varchar")
                                    .producedBeforeRestore(
                                            Row.of(1, 1L, "BakerBaker"),
                                            Row.of(2, 2L, "HeHe"),
                                            Row.of(3, 2L, "HeHe"))
                                    .producedAfterRestore(Row.of(2, 1L, "PostRestoreRight"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema("a int", "c1 varchar", "c2 varchar")
                                    .consumedBeforeRestore(
                                            Row.of(1, "BakerBaker", "Baker2"),
                                            Row.of(1, "BakerBaker", "Baker2"),
                                            Row.of(1, "BakerBaker", "Baker3"),
                                            Row.of(2, "HeHe", "Baker5"),
                                            Row.of(1, "BakerBaker", "Baker6"),
                                            Row.of(1, "BakerBaker", "Baker8"))
                                    .consumedAfterRestore(Row.of(2, "PostRestoreRight", "Baker5"))
                                    .build())
                    .runSql(
                            "insert into MySink "
                                    + "SELECT t2.a, t2.c, t1.c\n"
                                    + "FROM (\n"
                                    + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T1\n"
                                    + ") as t1\n"
                                    + "JOIN (\n"
                                    + " SELECT if(a = 3, cast(null as int), a) as a, b, c FROM T2\n"
                                    + ") as t2\n"
                                    + "ON t1.a = t2.a AND t1.b > t2.b")
                    .runTableApi(
                            env -> {
                                final Table t1 =
                                        env.from("T1")
                                                .select(
                                                        ifThenElse(
                                                                        $("a").isEqual(3),
                                                                        nullOf(DataTypes.INT()),
                                                                        $("a"))
                                                                .as("a1"),
                                                        $("b").as("b1"),
                                                        $("c").as("c1"));
                                final Table t2 =
                                        env.from("T2")
                                                .select(
                                                        ifThenElse(
                                                                        $("a").isEqual(3),
                                                                        nullOf(DataTypes.INT()),
                                                                        $("a"))
                                                                .as("a2"),
                                                        $("b").as("b2"),
                                                        $("c").as("c2"));
                                return t1.join(
                                                t2,
                                                $("a1").isEqual($("a2"))
                                                        .and($("b1").isGreater($("b2"))))
                                        .select($("a2"), $("c2"), $("c1"));
                            },
                            "MySink")
                    .build();
}
