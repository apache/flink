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

/**
 * {@link TableTestProgram} definitions for testing {@link StreamExecLateralSnapshotJoin}.
 *
 * <p>The programs cover a savepoint taken in each of the operator's two phases; the {@code
 * 'user_time'} gate is at {@code 00:00:03} in both.
 *
 * <ul>
 *   <li>{@link #LATERAL_SNAPSHOT_JOIN_PHASE_LOAD}: the transition to JOIN is not triggered before
 *       the savepoint, so the operator is still in LOAD; the savepoint captures the partial build
 *       multi-set and the buffered probe row. The flip to JOIN phase happens only after restore.
 *   <li>{@link #LATERAL_SNAPSHOT_JOIN_PHASE_JOIN}: the whole build side is loaded before the
 *       savepoint (the last build-side row triggers the flip), so the operator has flipped to JOIN
 *       and its build-side snapshot is materialized and frozen when the stop-with-savepoint fires.
 * </ul>
 *
 * <p>Both verify that the build state and the LOAD/JOIN phase (union operator state) survive the
 * savepoint: after restore the "after restore" probe rows join the restored snapshot.
 */
public class LateralSnapshotJoinTestPrograms {

    static final String[] PROBE_SCHEMA = {
        "pk STRING",
        "pv INT",
        "pts_str STRING",
        "pts AS TO_TIMESTAMP(pts_str)",
        "WATERMARK FOR pts AS pts"
    };

    static final String[] BUILD_SCHEMA = {
        "bk STRING",
        "bv INT",
        "bts_str STRING",
        "bts AS TO_TIMESTAMP(bts_str)",
        "WATERMARK FOR bts AS bts"
    };

    static final String[] SINK_SCHEMA = {"pk STRING", "pv INT", "bk STRING", "bv INT"};

    // Two rows for key 'a' exercise the per-key multi-set; the last row's watermark (00:00:03)
    // reaches the gate and flips the operator to JOIN.
    static final Row[] BUILD_BEFORE_DATA = {
        Row.of("a", 10, "2020-01-01 00:00:01"),
        Row.of("b", 20, "2020-01-01 00:00:02"),
        Row.of("a", 11, "2020-01-01 00:00:03")
    };

    static final Row[] PROBE_BEFORE_DATA = {
        Row.of("a", 100, "2020-01-01 00:00:06"), Row.of("b", 200, "2020-01-01 00:00:07")
    };

    // 'a' matches the restored snapshot; 'c' has no match.
    static final Row[] PROBE_AFTER_DATA = {
        Row.of("a", 101, "2020-01-01 00:00:10"), Row.of("c", 300, "2020-01-01 00:00:11")
    };

    // LOAD-phase restore scenario: none of these build rows reaches the 00:00:03 gate, so the
    // operator is still in LOAD when the savepoint is taken. The number of rows is the count the
    // savepoint trigger waits for (see LateralSnapshotJoinRestoreTest#awaitSavepointReady).
    static final Row[] LOAD_BUILD_BEFORE_DATA = {
        Row.of("a", 10, "2020-01-01 00:00:01"), Row.of("a", 11, "2020-01-01 00:00:02")
    };

    // The gate row (00:00:03) arrives only after restore and flips the operator to JOIN, draining
    // the probe rows buffered before the savepoint.
    static final Row[] LOAD_BUILD_AFTER_DATA = {Row.of("a", 12, "2020-01-01 00:00:03")};

    // Buffered during LOAD (LOAD emits nothing), so it survives the savepoint in probe state.
    static final Row[] LOAD_PROBE_BEFORE_DATA = {Row.of("a", 100, "2020-01-01 00:00:06")};

    static final Row[] LOAD_PROBE_AFTER_DATA = {Row.of("a", 101, "2020-01-01 00:00:10")};

    private static final String SNAPSHOT_BUILD =
            "LATERAL TABLE(SNAPSHOT("
                    + "input => TABLE b, "
                    + "load_completed_condition => 'user_time', "
                    + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:03' AS TIMESTAMP_LTZ(3))"
                    + ")) AS s ON probe.pk = s.bk";

    // Restore taken while the operator is in LOAD phase: the savepoint captures the partial build
    // multi-set and the buffered probe row, the LOAD phase is recorded in union operator state, and
    // nothing has been emitted yet. After restore the 00:00:03 build row completes the load, flips
    // to JOIN, and the buffered + after-restore probe rows join the full snapshot {10, 11, 12} for
    // key 'a'.
    public static final TableTestProgram LATERAL_SNAPSHOT_JOIN_PHASE_LOAD =
            TableTestProgram.of(
                            "lateral-snapshot-join-load-phase",
                            "validates a LATERAL SNAPSHOT inner join restored from a LOAD-phase savepoint")
                    .setupTableSource(
                            SourceTestStep.newBuilder("probe")
                                    .addSchema(PROBE_SCHEMA)
                                    .producedBeforeRestore(LOAD_PROBE_BEFORE_DATA)
                                    .producedAfterRestore(LOAD_PROBE_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("b")
                                    .addSchema(BUILD_SCHEMA)
                                    .producedBeforeRestore(LOAD_BUILD_BEFORE_DATA)
                                    .producedAfterRestore(LOAD_BUILD_AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(SINK_SCHEMA)
                                    // Empty before restore: LOAD phase emits nothing before the
                                    // savepoint. The explicit String[] picks the String overload
                                    // and marks this a SINK_WITH_RESTORE_DATA step.
                                    .consumedBeforeRestore(new String[0])
                                    .consumedAfterRestore(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[a, 100, a, 12]",
                                            "+I[a, 101, a, 10]",
                                            "+I[a, 101, a, 11]",
                                            "+I[a, 101, a, 12]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT probe.pk, probe.pv, s.bk, s.bv "
                                    + "FROM probe JOIN "
                                    + SNAPSHOT_BUILD)
                    .build();

    // Restore taken while the operator is in JOIN phase
    public static final TableTestProgram LATERAL_SNAPSHOT_JOIN_PHASE_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-join-inner",
                            "validates a LATERAL SNAPSHOT inner join across a restore")
                    .setupTableSource(
                            SourceTestStep.newBuilder("probe")
                                    .addSchema(PROBE_SCHEMA)
                                    .producedBeforeRestore(PROBE_BEFORE_DATA)
                                    .producedAfterRestore(PROBE_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("b")
                                    .addSchema(BUILD_SCHEMA)
                                    .producedBeforeRestore(BUILD_BEFORE_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]")
                                    .consumedAfterRestore("+I[a, 101, a, 10]", "+I[a, 101, a, 11]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT probe.pk, probe.pv, s.bk, s.bv "
                                    + "FROM probe JOIN "
                                    + SNAPSHOT_BUILD)
                    .build();
}
