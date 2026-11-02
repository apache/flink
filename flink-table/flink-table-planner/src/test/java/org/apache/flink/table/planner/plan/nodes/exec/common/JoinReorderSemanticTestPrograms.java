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

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/**
 * {@link TableTestProgram} definitions for semantic testing join reorder, see {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_JOIN_REORDER_ENABLED}.
 */
public class JoinReorderSemanticTestPrograms {

    public static final TableTestProgram FILTER_ON_NULL_GENERATING_SIDE_OF_LEFT_JOIN =
            TableTestProgram.of(
                            "join-reorder-filter-on-null-generating-side-of-left-join",
                            "validates that the filter on the null generating side of a left join"
                                    + " keeps the rows of the left side without a match")
                    .setupConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t1")
                                    .addSchema("a1 INT")
                                    .producedValues(Row.of(1), Row.of(2))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("t2")
                                    .addSchema("a2 INT", "b2 INT")
                                    .producedValues(Row.of(1, 5), Row.of(2, 5))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("t3")
                                    .addSchema("a3 INT", "b3 INT")
                                    .producedValues(Row.of(2, 1))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a1 INT", "a2 INT", "b2 INT", "b3 INT")
                                    .testMaterializedData()
                                    // The filter removes the row of t2 with a2 = 2, so the row of
                                    // t1 with a1 = 2 has no match.
                                    .consumedValues("+I[1, 1, 5, null]", "+I[2, null, null, null]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT a1, a2, b2, b3 FROM t1 LEFT JOIN "
                                    + "(SELECT * FROM t2 LEFT JOIN t3 ON a2 = a3 "
                                    + "WHERE COALESCE(b3, b2) > 3) tab "
                                    + "ON a1 = a2")
                    .build();
}
