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

/**
 * {@link TableTestProgram} definitions for semantically testing {@code LATERAL SNAPSHOT} joins in
 * batch mode, where the join degenerates to a regular join of the probe side against the (final)
 * build side.
 */
public class LateralSnapshotJoinTestPrograms {

    private static SourceTestStep probe() {
        return SourceTestStep.newBuilder("probe")
                .addSchema("pk STRING", "pv INT")
                .producedValues(Row.of("a", 1), Row.of("b", 2), Row.of("c", 3))
                .build();
    }

    private static SourceTestStep build() {
        // The key "a" appears twice: the batch join runs against the final (complete) build side,
        // so both "a" rows participate.
        return SourceTestStep.newBuilder("b")
                .addSchema("bk STRING", "bv INT")
                .producedValues(Row.of("a", 10), Row.of("a", 11), Row.of("b", 20))
                .build();
    }

    public static final TableTestProgram INNER_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-join-inner",
                            "batch LATERAL SNAPSHOT inner join degrades to a regular join")
                    .setupTableSource(probe())
                    .setupTableSource(build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "pv INT", "bk STRING", "bv INT")
                                    .consumedValues(
                                            Row.of("a", 1, "a", 10),
                                            Row.of("a", 1, "a", 11),
                                            Row.of("b", 2, "b", 20))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT pk, pv, bk, bv FROM probe JOIN LATERAL "
                                    + "TABLE(SNAPSHOT(input => TABLE b)) AS s ON probe.pk = s.bk")
                    .build();

    public static final TableTestProgram LEFT_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-join-left",
                            "batch LATERAL SNAPSHOT left join null-pads unmatched probe rows")
                    .setupTableSource(probe())
                    .setupTableSource(build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "pv INT", "bk STRING", "bv INT")
                                    .consumedValues(
                                            Row.of("a", 1, "a", 10),
                                            Row.of("a", 1, "a", 11),
                                            Row.of("b", 2, "b", 20),
                                            Row.of("c", 3, null, null))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT pk, pv, bk, bv FROM probe LEFT JOIN LATERAL "
                                    + "TABLE(SNAPSHOT(input => TABLE b)) AS s ON probe.pk = s.bk")
                    .build();

    public static final TableTestProgram INNER_JOIN_WITH_NON_EQUI_CONDITION =
            TableTestProgram.of(
                            "lateral-snapshot-join-non-equi",
                            "batch LATERAL SNAPSHOT join with an additional non-equi predicate")
                    .setupTableSource(probe())
                    .setupTableSource(build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "pv INT", "bk STRING", "bv INT")
                                    .consumedValues(
                                            Row.of("a", 1, "a", 11), Row.of("b", 2, "b", 20))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT pk, pv, bk, bv FROM probe JOIN LATERAL "
                                    + "TABLE(SNAPSHOT(input => TABLE b)) AS s "
                                    + "ON probe.pk = s.bk AND s.bv > 10")
                    .build();

    public static final TableTestProgram SNAPSHOT_ARGUMENTS_IGNORED =
            TableTestProgram.of(
                            "lateral-snapshot-join-arguments-ignored",
                            "streaming-only SNAPSHOT arguments do not affect the batch result")
                    .setupTableSource(probe())
                    .setupTableSource(build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "pv INT", "bk STRING", "bv INT")
                                    .consumedValues(
                                            Row.of("a", 1, "a", 10),
                                            Row.of("a", 1, "a", 11),
                                            Row.of("b", 2, "b", 20))
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT pk, pv, bk, bv FROM probe JOIN LATERAL "
                                    + "TABLE(SNAPSHOT("
                                    + "input => TABLE b, "
                                    + "load_completed_condition => 'compile_time', "
                                    + "load_completed_idle_timeout => INTERVAL '10' SECOND, "
                                    + "state_ttl => INTERVAL '1' DAY"
                                    + ")) AS s ON probe.pk = s.bk")
                    .build();
}
