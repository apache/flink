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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link BatchExecSortAggregate}. */
class SortAggregateTestPrograms {

    static final TableTestProgram GROUP_BY_VARIANCE =
            TableTestProgram.of(
                            "sort-aggregate-variance",
                            "validates variance related aggregations (VAR_POP, VAR_SAMP) that are "
                                    + "reduced to the internal $WELFORD_M2$1 aggregate function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT")
                                    .producedBeforeRestore(
                                            Row.of(2, 1L),
                                            Row.of(4, 1L),
                                            Row.of(10, 2L),
                                            Row.of(14, 2L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "b BIGINT",
                                            "cnt BIGINT",
                                            "var_pop_a DOUBLE",
                                            "var_samp_a DOUBLE",
                                            "PRIMARY KEY (b) NOT ENFORCED")
                                    .consumedBeforeRestore(
                                            "+I[1, 2, 1.0, 2.0]", "+I[2, 2, 4.0, 8.0]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "b, "
                                    + "COUNT(*) AS cnt, "
                                    + "VAR_POP(a) AS var_pop_a, "
                                    + "VAR_SAMP(a) AS var_samp_a "
                                    + "FROM source_t GROUP BY b")
                    .build();
}
