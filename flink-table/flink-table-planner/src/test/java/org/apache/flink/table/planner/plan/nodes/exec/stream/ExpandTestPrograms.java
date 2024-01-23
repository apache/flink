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

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** {@link TableTestProgram} definitions for testing {@link StreamExecExpand}. */
public class ExpandTestPrograms {

    static final TableTestProgram EXPAND =
            TableTestProgram.of("expand", "validates expand node")
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                            AggregatePhaseStrategy.ONE_PHASE.name())
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
                    .setupConfig(
                            IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED(),
                            false)
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema("a int", "b bigint", "c varchar")
                                    .producedBeforeRestore(
                                            Row.of(1, 1L, "Hi"),
                                            Row.of(2, 2L, "Hello"),
                                            Row.of(2, 3L, "Hello world"))
                                    .producedAfterRestore(Row.of(5, 6L, "Hello there"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema(
                                            "b bigint",
                                            "a bigint",
                                            "c varchar",
                                            "primary key (b) not enforced")
                                    .consumedBeforeRestore(
                                            Row.of(1, 1L, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 1, 1L, "Hi"),
                                            Row.of(2, 1L, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 1L, "Hello"),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 2, 2L, "Hello"))
                                    .consumedAfterRestore(
                                            Row.of(5, 1L, null),
                                            Row.ofKind(RowKind.UPDATE_AFTER, 5, 1L, "Hello there"))
                                    .build())
                    .runSql(
                            "insert into MySink select a, "
                                    + "count(distinct b) as b, "
                                    + "first_value(c) c "
                                    + "from MyTable group by a")
                    .build();
}
