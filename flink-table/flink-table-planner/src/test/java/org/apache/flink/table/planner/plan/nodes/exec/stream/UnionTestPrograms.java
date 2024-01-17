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

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/** {@link TableTestProgram} definitions for testing {@link StreamExecUnion}. */
public class UnionTestPrograms {

    static final TableTestProgram UNION_TWO_SOURCES =
            TableTestProgram.of("union-two-sources", "validates union of 2 tables")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t1")
                                    .addSchema(
                                            "a BIGINT",
                                            "b INT NOT NULL",
                                            "c VARCHAR",
                                            "d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    420L,
                                                    1,
                                                    "hello",
                                                    LocalDateTime.of(
                                                            2023, 12, 16, 01, 01, 01, 123)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    420L,
                                                    1,
                                                    "hello",
                                                    LocalDateTime.of(
                                                            2023, 12, 16, 01, 01, 01, 123)),
                                            Row.of(
                                                    600L,
                                                    6,
                                                    "hello there",
                                                    LocalDateTime.of(
                                                            2023, 12, 19, 01, 01, 01, 123)))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t2")
                                    .addSchema("d BIGINT", "e INT NOT NULL")
                                    .producedBeforeRestore(Row.of(420L, 1), Row.of(421L, 2))
                                    .producedAfterRestore(Row.of(500L, 3), Row.of(420L, 1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t1_union_t2")
                                    .addSchema("a BIGINT", "b INT")
                                    .consumedBeforeRestore(Row.of(420L, 1), Row.of(421L, 2))
                                    .consumedAfterRestore(Row.of(600L, 6), Row.of(500L, 3))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t1_union_t2 SELECT * FROM (SELECT a, b FROM source_t1) UNION (SELECT d, e FROM source_t2)")
                    .build();

    static final TableTestProgram UNION_ALL_TWO_SOURCES =
            TableTestProgram.of("union-all-two-sources", "validates union all of 2 tables")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t1")
                                    .addSchema(
                                            "a BIGINT",
                                            "b INT NOT NULL",
                                            "c VARCHAR",
                                            "d TIMESTAMP(3)")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    420L,
                                                    1,
                                                    "hello",
                                                    LocalDateTime.of(
                                                            2023, 12, 16, 01, 01, 01, 123)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    600L,
                                                    6,
                                                    "hello there",
                                                    LocalDateTime.of(
                                                            2023, 12, 19, 01, 01, 01, 123)))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t2")
                                    .addSchema("d BIGINT", "e INT NOT NULL")
                                    .producedBeforeRestore(Row.of(420L, 1), Row.of(421L, 2))
                                    .producedAfterRestore(Row.of(500L, 3), Row.of(421L, 2))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t1_union_all_t2")
                                    .addSchema("a BIGINT", "b INT")
                                    .consumedBeforeRestore(
                                            Row.of(420L, 1), Row.of(420L, 1), Row.of(421L, 2))
                                    .consumedAfterRestore(
                                            Row.of(600L, 6), Row.of(500L, 3), Row.of(421L, 2))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t1_union_all_t2 SELECT * FROM (SELECT a, b FROM source_t1) UNION ALL (SELECT d, e FROM source_t2)")
                    .build();

    static final TableTestProgram UNION_ALL_WITH_FILTER =
            TableTestProgram.of(
                            "union-all-with-filter", "validates union all of 2 tables with filters")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t1")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .producedBeforeRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10))
                                    .producedAfterRestore(
                                            Row.of(1, "a", 5), Row.of(3, "b", 7), Row.of(5, "c", 9))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t2")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .producedBeforeRestore(
                                            Row.of(0, "a", 6),
                                            Row.of(7, "b", 8),
                                            Row.of(8, "c", 10))
                                    .producedAfterRestore(
                                            Row.of(1, "a", 5),
                                            Row.of(13, "b", 7),
                                            Row.of(50, "c", 9))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t1_union_all_t2")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .consumedBeforeRestore(
                                            Row.of(0, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10))
                                    .consumedAfterRestore(
                                            Row.of(1, "a", 5), Row.of(3, "b", 7), Row.of(5, "c", 9))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t1_union_all_t2 (SELECT * FROM source_t1 where a >=3) UNION ALL (select * from source_t2 where a <= 3)")
                    .build();
}
