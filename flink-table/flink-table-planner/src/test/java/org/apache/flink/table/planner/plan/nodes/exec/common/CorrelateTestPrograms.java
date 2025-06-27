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

import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit;
import org.apache.flink.table.planner.utils.TableFunc1;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecCorrelate}. */
public class CorrelateTestPrograms {

    static final Row[] BEFORE_DATA = {Row.of(1L, 1, "hi#there"), Row.of(2L, 2, "hello#world")};

    static final Row[] AFTER_DATA = {
        Row.of(4L, 4, "foo#bar"), Row.of(3L, 3, "bar#fiz"),
    };

    static final String[] SOURCE_SCHEMA = {"a BIGINT", "b INT NOT NULL", "c VARCHAR"};

    public static final TableTestProgram CORRELATE_CATALOG_FUNC =
            TableTestProgram.of(
                            "correlate-catalog-func",
                            "validate correlate with temporary catalog function")
                    .setupTemporaryCatalogFunction("func1", TableFunc1.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, $hi]",
                                            "+I[hi#there, $there]",
                                            "+I[hello#world, $hello]",
                                            "+I[hello#world, $world]")
                                    .consumedAfterRestore(
                                            "+I[foo#bar, $foo]",
                                            "+I[foo#bar, $bar]",
                                            "+I[bar#fiz, $bar]",
                                            "+I[bar#fiz, $fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t, LATERAL TABLE(func1(c, '$')) AS T(s)")
                    .build();

    public static final TableTestProgram CORRELATE_SYSTEM_FUNC =
            TableTestProgram.of(
                            "correlate-system-func",
                            "validate correlate with temporary system function")
                    .setupTemporarySystemFunction("STRING_SPLIT", StringSplit.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, hi]",
                                            "+I[hi#there, there]",
                                            "+I[hello#world, hello]",
                                            "+I[hello#world, world]")
                                    .consumedAfterRestore(
                                            "+I[foo#bar, foo]",
                                            "+I[foo#bar, bar]",
                                            "+I[bar#fiz, bar]",
                                            "+I[bar#fiz, fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t, LATERAL TABLE(STRING_SPLIT(c, '#')) AS T(s)")
                    .build();

    public static final TableTestProgram CORRELATE_JOIN_FILTER =
            TableTestProgram.of("correlate-join-filter", "validate correlate with join and filter")
                    .setupTemporaryCatalogFunction("func1", TableFunc1.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hello#world, hello]", "+I[hello#world, world]")
                                    .consumedAfterRestore("+I[bar#fiz, bar]", "+I[bar#fiz, fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT * FROM (SELECT c, s FROM source_t, LATERAL TABLE(func1(c)) AS T(s)) AS T2 WHERE c LIKE '%hello%' OR c LIKE '%fiz%'")
                    .build();

    public static final TableTestProgram CORRELATE_LEFT_JOIN =
            TableTestProgram.of("correlate-left-join", "validate correlate with left join")
                    .setupTemporaryCatalogFunction("func1", TableFunc1.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a VARCHAR", "b VARCHAR")
                                    .consumedBeforeRestore(
                                            "+I[hi#there, hi]",
                                            "+I[hi#there, there]",
                                            "+I[hello#world, hello]",
                                            "+I[hello#world, world]")
                                    .consumedAfterRestore(
                                            "+I[foo#bar, foo]",
                                            "+I[foo#bar, bar]",
                                            "+I[bar#fiz, bar]",
                                            "+I[bar#fiz, fiz]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT c, s FROM source_t LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON TRUE")
                    .build();

    public static final TableTestProgram CORRELATE_CROSS_JOIN_UNNEST =
            TableTestProgram.of(
                            "correlate-cross-join-unnest",
                            "validate correlate with cross join and unnest")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("name STRING", "arr ARRAY<ROW<nested STRING>>")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "Bob",
                                                    new Row[] {
                                                        Row.of("1"), Row.of("2"), Row.of("3")
                                                    }))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "Alice",
                                                    new Row[] {
                                                        Row.of("4"), Row.of("5"), Row.of("6")
                                                    }))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "nested STRING")
                                    .consumedBeforeRestore("+I[Bob, 1]", "+I[Bob, 2]", "+I[Bob, 3]")
                                    .consumedAfterRestore(
                                            "+I[Alice, 4]", "+I[Alice, 5]", "+I[Alice, 6]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT name, nested FROM source_t CROSS JOIN UNNEST(arr) AS T(nested)")
                    .build();

    public static final TableTestProgram CORRELATE_CROSS_JOIN_UNNEST_2 =
            TableTestProgram.of(
                            "correlate-cross-join-unnest",
                            "validate correlate with cross join and unnest")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("name STRING", "arr ARRAY<ROW<nested STRING>>")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "Bob",
                                                    new Row[] {
                                                        Row.of("1"), Row.of("2"), Row.of("3")
                                                    }))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "Alice",
                                                    new Row[] {
                                                        Row.of("4"), Row.of("5"), Row.of("6")
                                                    }))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "nested STRING")
                                    .consumedBeforeRestore("+I[Bob, 1]", "+I[Bob, 2]", "+I[Bob, 3]")
                                    .consumedAfterRestore(
                                            "+I[Alice, 4]", "+I[Alice, 5]", "+I[Alice, 6]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT (SELECT name, nested FROM source_t, UNNEST(arr) AS T(nested)) FROM source_t")
                    .build();

    public static final TableTestProgram CORRELATE_WITH_LITERAL_AGG =
            TableTestProgram.of(
                            "correlate-with-literal-agg",
                            "validate correlate with literal aggregate function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t1")
                                    .addSchema("a INTEGER", "b BIGINT", "c STRING")
                                    .producedBeforeRestore(Row.of(1, 2L, "3"), Row.of(2, 3L, "4"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t2")
                                    .addSchema("d INTEGER", "e BIGINT", "f STRING")
                                    .producedBeforeRestore(Row.of(1, 2L, "3"), Row.of(2, 3L, "4"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t3")
                                    .addSchema("i INTEGER", "j BIGINT", "k STRING")
                                    .producedBeforeRestore(Row.of(1, 2L, "3"), Row.of(2, 3L, "4"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("b BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[2]", "+I[3]", "-D[2]", "-D[3]", "+I[2]", "+I[3]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT b FROM source_t1 "
                                    + " WHERE (CASE WHEN a IN (SELECT 1 FROM source_t3) THEN 1 ELSE 2 END) "
                                    + " IN (SELECT d FROM source_t2 WHERE source_t1.c = source_t2.f)")
                    .build();
}
