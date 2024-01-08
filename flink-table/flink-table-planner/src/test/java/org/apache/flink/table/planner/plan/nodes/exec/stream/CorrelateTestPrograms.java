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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

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

    static final TableTestProgram CORRELATE_CATALOG_FUNC =
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

    static final TableTestProgram CORRELATE_SYSTEM_FUNC =
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

    static final TableTestProgram CORRELATE_JOIN_FILTER =
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

    static final TableTestProgram CORRELATE_LEFT_JOIN =
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

    static final TableTestProgram CORRELATE_CROSS_JOIN_UNNEST =
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
}
