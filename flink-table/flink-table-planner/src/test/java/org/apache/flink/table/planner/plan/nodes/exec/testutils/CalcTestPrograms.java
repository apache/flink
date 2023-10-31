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

package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecCalc}. */
public class CalcTestPrograms {

    static final TableTestProgram SIMPLE_CALC =
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
}
