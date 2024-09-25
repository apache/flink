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

import java.util.Collections;

/** Programs for verifying {@link StreamExecOverAggregate}. */
public class OverWindowTestPrograms {
    static final TableTestProgram LAG_OVER_FUNCTION =
            TableTestProgram.of("over-aggregate-lag", "validates restoring a lag function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "ts STRING",
                                            "b MAP<DOUBLE, DOUBLE>",
                                            "`r_time` AS TO_TIMESTAMP(`ts`)",
                                            "WATERMARK for `r_time` AS `r_time`")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "2020-04-15 08:00:05",
                                                    Collections.singletonMap(42.0, 42.0)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "2020-04-15 08:00:06",
                                                    Collections.singletonMap(42.1, 42.1)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("ts STRING", "b MAP<DOUBLE, DOUBLE>")
                                    .consumedBeforeRestore(Row.of("2020-04-15 08:00:05", null))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    "2020-04-15 08:00:06",
                                                    Collections.singletonMap(42.0, 42.0)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT ts, LAG(b, 1) over (order by r_time) AS "
                                    + "bLag FROM t")
                    .build();
}
