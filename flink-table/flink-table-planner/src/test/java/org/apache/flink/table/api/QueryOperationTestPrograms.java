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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.lit;
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
                            "SELECT * FROM (VALUES \n"
                                    + "    (CAST(1 AS BIGINT), 'abc'),\n"
                                    + "    (CAST(2 AS BIGINT), 'cde')\n"
                                    + ") $VAL0(`f0`, `f1`)")
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
                            "SELECT * FROM (\n"
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
                            "SELECT DISTINCT * FROM (\n"
                                    + "    SELECT * FROM (\n"
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
                                    .consumedValues(
                                            Row.ofKind(RowKind.INSERT, "apple", 10L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "apple", 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "apple", 30L),
                                            Row.ofKind(RowKind.INSERT, "pear", 5L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "pear", 5L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "pear", 20L))
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
                                    + "        TUMBLE((SELECT `a`, `b`, `ts` FROM `default_catalog`.`default_database`.`s`), DESCRIPTOR(`ts`), INTERVAL '0 00:00:05.0' DAY TO SECOND(3))\n"
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
                                    + "    LATERAL TABLE(`default_catalog`.`default_database`.`udtf`(`b`)) $T(`f0`) ON TRUE")
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
                            "SELECT * FROM (\n"
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
                            "SELECT * FROM (\n"
                                    + "    SELECT `a`, `b` FROM `default_catalog`.`default_database`.`s`\n"
                                    + ") ORDER BY `a` ASC, `b` DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")
                    .build();
}
