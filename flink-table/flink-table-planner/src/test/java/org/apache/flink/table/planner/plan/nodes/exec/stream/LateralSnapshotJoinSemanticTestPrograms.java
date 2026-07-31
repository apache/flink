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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Deterministic result {@link TableTestProgram} definitions for the {@code LATERAL SNAPSHOT}
 * processing-time temporal join ({@link StreamExecLateralSnapshotJoin}).
 *
 * <p>Processing-time semantics make the join non-deterministic in general. To get stable results,
 * each build source appends a non-matching "flip-trigger" row at the flip timestamp ({@link
 * #FLIP_TRIGGER_TS}) while all real build rows are earlier. With per-record ({@code on-event})
 * watermarks the operator flips to the JOIN phase. Some programs throttle source emission ({@code
 * source.sleep-*}) to place probes deterministically around the flip.
 */
public class LateralSnapshotJoinSemanticTestPrograms {

    /** The {@code 'user_time'} condition reached mid-stream by the build-side flip-trigger row. */
    private static final String MID_FLIP =
            "load_completed_condition => 'user_time', "
                    + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:10' AS TIMESTAMP_LTZ(3))";

    /** A far-future flip condition: the flip happens only at end of all input. */
    private static final String END_FLIP =
            "load_completed_condition => 'user_time', "
                    + "load_completed_time => CAST(TIMESTAMP '2100-01-01 00:00:00' AS TIMESTAMP_LTZ(3))";

    /** Event time of the flip-trigger row; equal to the {@link #MID_FLIP} timestamp. */
    private static final String FLIP_TRIGGER_TS = "00:00:10";

    /** A build-side key that never matches any probe row. */
    private static final String FLIP_TRIGGER_KEY = "__flip_trigger__";

    private static final String[] PROBE_SCHEMA = {
        "pk STRING", "pv INT", "pts TIMESTAMP(3)", "WATERMARK FOR pts AS pts"
    };

    private static final String[] BUILD_SCHEMA = {
        "bk STRING", "bv INT", "bts TIMESTAMP(3)", "WATERMARK FOR bts AS bts"
    };

    // ------------------------------------------------------------------------------------------
    // Core join semantics
    // ------------------------------------------------------------------------------------------

    public static final TableTestProgram INNER_JOIN =
            TableTestProgram.of("lateral-snapshot-inner-join", "LATERAL SNAPSHOT inner join")
                    // Throttle the probe so the fast build flips first; the probes are then joined
                    // live in the JOIN phase (the build-side is finalized, so buffered-vs-live does
                    // not change the result).
                    .setupTableSource(throttledProbe(defaultProbe(), 40L))
                    .setupTableSource(appendBuild(withFlipTrigger(defaultBuild())))
                    .setupTableSink(
                            keyValueSink()
                                    .consumedValues(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]")
                                    .build())
                    .runSql(
                            innerJoin(
                                    "probe.pk, probe.pv, s.bk, s.bv", MID_FLIP, "probe.pk = s.bk"))
                    .build();

    public static final TableTestProgram LEFT_JOIN =
            TableTestProgram.of("lateral-snapshot-left-join", "LATERAL SNAPSHOT left join")
                    .setupTableSource(throttledProbe(defaultProbe(), 40L))
                    .setupTableSource(appendBuild(withFlipTrigger(defaultBuild())))
                    .setupTableSink(
                            keyValueSink()
                                    .consumedValues(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]",
                                            "+I[c, 300, null, null]")
                                    .build())
                    .runSql(leftJoin("probe.pk, probe.pv, s.bk, s.bv", MID_FLIP, "probe.pk = s.bk"))
                    .build();

    public static final TableTestProgram SELECT_STAR =
            TableTestProgram.of(
                            "lateral-snapshot-select-star",
                            "SELECT * materializes the build-side rowtime as a regular TIMESTAMP")
                    .setupTableSource(probe(List.of(Row.of("a", 100, ts("00:01:00")))))
                    .setupTableSource(
                            appendBuild(withFlipTrigger(List.of(Row.of("a", 10, ts("00:00:01"))))))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(
                                            "pk STRING",
                                            "pv INT",
                                            "pts TIMESTAMP(3)",
                                            "bk STRING",
                                            "bv INT",
                                            "bts TIMESTAMP(3)")
                                    .testMaterializedData()
                                    .consumedValues(
                                            "+I[a, 100, 2020-01-01T00:01, a, 10, 2020-01-01T00:00:01]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                    + "input => TABLE b, "
                                    + MID_FLIP
                                    + ")) AS s ON probe.pk = s.bk")
                    .build();

    public static final TableTestProgram COMPOSITE_KEYS =
            TableTestProgram.of("lateral-snapshot-composite-keys", "join on composite keys")
                    .setupTableSource(
                            probe(
                                    Arrays.asList(
                                            Row.of("a", 10, ts("00:01:00")),
                                            Row.of("a", 11, ts("00:01:01")),
                                            Row.of("z", 10, ts("00:01:01")),
                                            Row.of("b", 20, ts("00:01:02")))))
                    .setupTableSource(
                            appendBuild(
                                    withFlipTrigger(
                                            Arrays.asList(
                                                    Row.of("a", 10, ts("00:00:01")),
                                                    Row.of("a", 99, ts("00:00:02")),
                                                    Row.of("b", 20, ts("00:00:03"))))))
                    .setupTableSink(
                            keyValueSink()
                                    .consumedValues("+I[a, 10, a, 10]", "+I[b, 20, b, 20]")
                                    .build())
                    .runSql(
                            innerJoin(
                                    "probe.pk, probe.pv, s.bk, s.bv",
                                    MID_FLIP,
                                    "probe.pk = s.bk AND probe.pv = s.bv"))
                    .build();

    public static final TableTestProgram NON_EQUI =
            TableTestProgram.of("lateral-snapshot-non-equi", "join with a non-equi condition")
                    .setupTableSource(
                            probe(
                                    Arrays.asList(
                                            Row.of("a", 15, ts("00:01:00")),
                                            Row.of("a", 5, ts("00:01:01")))))
                    .setupTableSource(
                            appendBuild(
                                    withFlipTrigger(
                                            Arrays.asList(
                                                    Row.of("a", 10, ts("00:00:01")),
                                                    Row.of("a", 20, ts("00:00:02"))))))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "pv INT", "bv INT")
                                    .testMaterializedData()
                                    .consumedValues("+I[a, 15, 10]")
                                    .build())
                    .runSql(
                            innerJoin(
                                    "probe.pk, probe.pv, s.bv",
                                    MID_FLIP,
                                    "probe.pk = s.bk AND probe.pv > s.bv"))
                    .build();

    public static final TableTestProgram EMPTY_BUILD_INNER =
            TableTestProgram.of(
                            "lateral-snapshot-empty-build-inner",
                            "inner join over an empty build side yields no rows")
                    .setupTableSource(probe(defaultProbe()))
                    .setupTableSource(appendBuild(withFlipTrigger(List.of())))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "bk STRING")
                                    .testMaterializedData()
                                    .consumedValues(new String[0])
                                    .build())
                    .runSql(innerJoin("probe.pk, s.bk", MID_FLIP, "probe.pk = s.bk"))
                    .build();

    public static final TableTestProgram EMPTY_BUILD_LEFT =
            TableTestProgram.of(
                            "lateral-snapshot-empty-build-left",
                            "left join over an empty build side preserves the probe rows")
                    .setupTableSource(probe(defaultProbe()))
                    .setupTableSource(appendBuild(withFlipTrigger(List.of())))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pk STRING", "bk STRING")
                                    .testMaterializedData()
                                    .consumedValues("+I[a, null]", "+I[b, null]", "+I[c, null]")
                                    .build())
                    .runSql(leftJoin("probe.pk, s.bk", MID_FLIP, "probe.pk = s.bk"))
                    .build();

    // ------------------------------------------------------------------------------------------
    // Flip timing
    // ------------------------------------------------------------------------------------------

    public static final TableTestProgram FLIP_AT_END =
            TableTestProgram.of(
                            "lateral-snapshot-flip-at-end",
                            "far-future flip condition: the operator flips only at end-of-input")
                    .setupTableSource(probe(defaultProbe()))
                    .setupTableSource(appendBuild(withFlipTrigger(defaultBuild())))
                    .setupTableSink(
                            keyValueSink()
                                    .consumedValues(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]")
                                    .build())
                    .runSql(
                            innerJoin(
                                    "probe.pk, probe.pv, s.bk, s.bv", END_FLIP, "probe.pk = s.bk"))
                    .build();

    public static final TableTestProgram DEFAULT_COMPILE_TIME =
            TableTestProgram.of(
                            "lateral-snapshot-default-compile-time",
                            "default 'compile_time' condition flips at end-of-input for 2020 data")
                    .setupTableSource(probe(defaultProbe()))
                    .setupTableSource(appendBuild(withFlipTrigger(defaultBuild())))
                    .setupTableSink(
                            keyValueSink()
                                    .consumedValues(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]")
                                    .build())
                    // No options: 'load_completed_condition' defaults to 'compile_time'.
                    .runSql(
                            "INSERT INTO sink SELECT probe.pk, probe.pv, s.bk, s.bv "
                                    + "FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                    + "input => TABLE b"
                                    + ")) AS s ON probe.pk = s.bk")
                    .build();

    public static final TableTestProgram LIVE_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-live-join",
                            "probes are joined live per-record in the JOIN phase")
                    // Fast build flips almost immediately; the throttled probe stream is consumed
                    // entirely in the JOIN phase (including non-matching keys c). The snapshot is
                    // static after the flip, so the result is deterministic.
                    .setupTableSource(
                            throttledProbe(
                                    Arrays.asList(
                                            Row.of("a", 1, ts("00:00:01")),
                                            Row.of("b", 2, ts("00:00:02")),
                                            Row.of("c", 3, ts("00:00:03")),
                                            Row.of("a", 4, ts("00:00:04")),
                                            Row.of("b", 5, ts("00:00:05")),
                                            Row.of("c", 6, ts("00:00:06"))),
                                    40L))
                    .setupTableSource(
                            appendBuild(
                                    withFlipTrigger(
                                            Arrays.asList(
                                                    Row.of("a", 10, ts("00:00:01")),
                                                    Row.of("b", 20, ts("00:00:02"))))))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pv INT", "pk STRING", "bv INT")
                                    .testMaterializedData()
                                    .consumedValues(
                                            "+I[1, a, 10]",
                                            "+I[4, a, 10]",
                                            "+I[2, b, 20]",
                                            "+I[5, b, 20]")
                                    .build())
                    .runSql(innerJoin("probe.pv, probe.pk, s.bv", MID_FLIP, "probe.pk = s.bk"))
                    .build();

    public static final TableTestProgram BUFFERED_THEN_DRAINED =
            TableTestProgram.of(
                            "lateral-snapshot-buffered-then-drained",
                            "probes buffered during LOAD are drained against the final version")
                    // Un-throttled: buffered within a few ms, long before the flip.
                    .setupTableSource(probe(manyProbes(20)))
                    // Throttled build flips at ~240 ms, by which time the probe stream is fully
                    // buffered in LOAD; the flip drains all buffered probes against version 3.
                    .setupTableSource(
                            throttledUpsertBuild(
                                    Arrays.asList(
                                            Row.ofKind(RowKind.INSERT, "k", 1, ts("00:00:01")),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, "k", 2, ts("00:00:02")),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER, "k", 3, ts("00:00:05")),
                                            Row.ofKind(
                                                    RowKind.INSERT,
                                                    FLIP_TRIGGER_KEY,
                                                    0,
                                                    ts(FLIP_TRIGGER_TS))),
                                    80L))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("pv INT", "bv INT")
                                    .testMaterializedData()
                                    .consumedValues(
                                            IntStream.rangeClosed(1, 20)
                                                    .mapToObj(i -> String.format("+I[%d, 3]", i))
                                                    .toArray(String[]::new))
                                    .build())
                    .runSql(innerJoin("probe.pv, s.bv", MID_FLIP, "probe.pk = s.bk"))
                    .build();

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private static String innerJoin(String projection, String flip, String condition) {
        return join("JOIN", projection, flip, condition);
    }

    private static String leftJoin(String projection, String flip, String condition) {
        return join("LEFT JOIN", projection, flip, condition);
    }

    private static String join(String joinType, String projection, String flip, String condition) {
        return "INSERT INTO sink SELECT "
                + projection
                + " FROM probe "
                + joinType
                + " LATERAL TABLE(SNAPSHOT("
                + "input => TABLE b, "
                + flip
                + ")) AS s ON "
                + condition;
    }

    private static List<Row> defaultProbe() {
        return Arrays.asList(
                Row.of("a", 100, ts("00:01:00")),
                Row.of("b", 200, ts("00:01:01")),
                Row.of("c", 300, ts("00:01:02")));
    }

    private static List<Row> defaultBuild() {
        return Arrays.asList(
                Row.of("a", 10, ts("00:00:01")),
                Row.of("b", 20, ts("00:00:02")),
                Row.of("a", 11, ts("00:00:03")));
    }

    private static List<Row> manyProbes(int count) {
        return IntStream.rangeClosed(1, count)
                .mapToObj(i -> Row.of("k", i, ts(String.format("00:00:%02d", i))))
                .collect(Collectors.toList());
    }

    /**
     * Appends a non-matching build row at the {@link #FLIP_TRIGGER_TS} timestamp; its watermark
     * flips the operator to the JOIN phase mid-stream (all real build rows are earlier).
     */
    private static List<Row> withFlipTrigger(List<Row> data) {
        final List<Row> withTrigger = new ArrayList<>(data);
        withTrigger.add(Row.of(FLIP_TRIGGER_KEY, 0, ts(FLIP_TRIGGER_TS)));
        return withTrigger;
    }

    private static SourceTestStep probe(List<Row> data) {
        return SourceTestStep.newBuilder("probe")
                .addSchema(PROBE_SCHEMA)
                .producedValues(data.toArray(new Row[0]))
                .build();
    }

    private static SourceTestStep throttledProbe(List<Row> data, long sleepMillis) {
        return SourceTestStep.newBuilder("probe")
                .addSchema(PROBE_SCHEMA)
                .addOptions(throttleOptions(sleepMillis))
                .producedValues(data.toArray(new Row[0]))
                .build();
    }

    private static SourceTestStep appendBuild(List<Row> data) {
        return SourceTestStep.newBuilder("b")
                .addSchema(BUILD_SCHEMA)
                .producedValues(data.toArray(new Row[0]))
                .build();
    }

    private static SourceTestStep throttledUpsertBuild(List<Row> data, long sleepMillis) {
        return SourceTestStep.newBuilder("b")
                .addSchema(
                        "bk STRING",
                        "bv INT",
                        "bts TIMESTAMP(3)",
                        "WATERMARK FOR bts AS bts",
                        "PRIMARY KEY (bk) NOT ENFORCED")
                .addOption("changelog-mode", "I,UA,D")
                .addOptions(throttleOptions(sleepMillis))
                .producedValues(data.toArray(new Row[0]))
                .build();
    }

    private static SinkTestStep.Builder keyValueSink() {
        return SinkTestStep.newBuilder("sink")
                .addSchema("pk STRING", "pv INT", "bk STRING", "bv INT")
                .testMaterializedData();
    }

    private static Map<String, String> throttleOptions(long sleepMillis) {
        final Map<String, String> options = new HashMap<>();
        options.put("source.sleep-after-elements", "1");
        options.put("source.sleep-time", sleepMillis + "ms");
        return options;
    }

    private static LocalDateTime ts(String time) {
        return LocalDateTime.parse("2020-01-01T" + time);
    }
}
