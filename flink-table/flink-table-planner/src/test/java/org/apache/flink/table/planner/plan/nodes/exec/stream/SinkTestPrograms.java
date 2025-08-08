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
import org.apache.flink.types.RowKind;

/** Tests for verifying sink semantics. */
public class SinkTestPrograms {

    public static final TableTestProgram INSERT_RETRACT_WITHOUT_PK =
            TableTestProgram.of(
                            "insert-retract-without-pk",
                            "The sink accepts retract input. Retract is directly passed through.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("name STRING", "score INT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 3),
                                            Row.ofKind(RowKind.INSERT, "Bob", 5),
                                            Row.ofKind(RowKind.INSERT, "Bob", 6),
                                            Row.ofKind(RowKind.INSERT, "Charly", 33))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "score BIGINT")
                                    .addOption("sink-changelog-mode-enforced", "I,UB,UA,D")
                                    .consumedValues(
                                            "+I[Alice, 3]",
                                            "+I[Bob, 5]",
                                            "-U[Bob, 5]",
                                            "+U[Bob, 11]",
                                            "+I[Charly, 33]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT name, SUM(score) FROM source_t GROUP BY name")
                    .build();

    public static final TableTestProgram INSERT_RETRACT_WITH_PK =
            TableTestProgram.of(
                            "insert-retract-with-pk",
                            "The sink accepts retract input. Although upsert keys (name) and primary keys (UPPER(name))"
                                    + "don't match, the retract changelog is passed through.")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("name STRING", "score INT")
                                    .addOption("changelog-mode", "I")
                                    .producedValues(
                                            Row.ofKind(RowKind.INSERT, "Alice", 3),
                                            Row.ofKind(RowKind.INSERT, "Bob", 5),
                                            Row.ofKind(RowKind.INSERT, "Bob", 6),
                                            Row.ofKind(RowKind.INSERT, "Charly", 33))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "name STRING PRIMARY KEY NOT ENFORCED", "score BIGINT")
                                    .addOption("sink-changelog-mode-enforced", "I,UB,UA,D")
                                    .consumedValues(
                                            "+I[ALICE, 3]",
                                            "+I[BOB, 5]",
                                            "-U[BOB, 5]",
                                            "+U[BOB, 11]",
                                            "+I[CHARLY, 33]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT UPPER(name), SUM(score) FROM source_t GROUP BY name")
                    .build();
}
