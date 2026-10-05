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

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.test.program.TableTestProgram;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.descriptor;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.FLIP_TRIGGER_TS;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.appendBuild;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.defaultBuild;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.defaultProbe;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.keyValueSink;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.throttledProbe;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.ts;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.LateralSnapshotJoinSemanticTestPrograms.withFlipTrigger;

/**
 * Table API counterparts of {@link LateralSnapshotJoinSemanticTestPrograms}, reusing the same
 * sources, sinks, and flip-trigger determinism tricks but expressing the {@code LATERAL SNAPSHOT}
 * join through {@link Table#joinLateral} instead of SQL.
 */
public class LateralSnapshotJoinTableApiSemanticTestPrograms {

    public static final TableTestProgram INNER_JOIN_TABLE_API =
            TableTestProgram.of(
                            "lateral-snapshot-inner-join-table-api",
                            "LATERAL SNAPSHOT inner join expressed through the Table API")
                    .setupTableSource(throttledProbe(defaultProbe(), 40L))
                    .setupTableSource(appendBuild(withFlipTrigger(defaultBuild())))
                    .setupTableSink(
                            keyValueSink()
                                    .consumedValues(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("probe")
                                            .joinLateral(
                                                    call(
                                                            "SNAPSHOT",
                                                            env.from("b").asArgument("input"),
                                                            descriptor("bts").asArgument("on_time"),
                                                            loadCompletedTime(FLIP_TRIGGER_TS)
                                                                    .asArgument(
                                                                            "load_completed_time")),
                                                    $("pk").isEqual($("bk")))
                                            .select($("pk"), $("pv"), $("bk"), $("bv")),
                            "sink")
                    .build();

    /**
     * Builds the {@code load_completed_time} argument as a Table API expression equivalent to the
     * SQL {@code CAST(TIMESTAMP '2020-01-01 <time>' AS TIMESTAMP_LTZ(3))} used by the SQL programs
     * for the same flip timestamp.
     */
    private static ApiExpression loadCompletedTime(String time) {
        return lit(ts(time)).cast(DataTypes.TIMESTAMP_LTZ(3));
    }

    private LateralSnapshotJoinTableApiSemanticTestPrograms() {}
}
