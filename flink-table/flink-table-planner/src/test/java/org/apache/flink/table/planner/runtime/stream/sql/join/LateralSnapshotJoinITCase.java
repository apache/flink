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

package org.apache.flink.table.planner.runtime.stream.sql.join;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThatList;

/**
 * Result tests for the {@code LATERAL SNAPSHOT} processing-time temporal join that are
 * non-deterministic or benefit from dual-backend (HEAP/ROCKSDB) coverage. Deterministic,
 * backend-agnostic cases live in {@code LateralSnapshotJoinSemanticTests}; time-driven behavior
 * (idle-timeout flip, state-TTL eviction) in {@code LateralSnapshotJoinOperatorTest}.
 *
 * <p>To stabilize results, each build source appends a non-matching "flip-trigger" row at {@link
 * #MID_FLIP} (after all real rows); its per-record watermark flips the operator to the JOIN phase.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class LateralSnapshotJoinITCase extends StreamingWithStateTestBase {

    /** The {@code 'user_time'} condition reached mid-stream by the build-side flip-trigger row. */
    private static final String MID_FLIP =
            "load_completed_condition => 'user_time', "
                    + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:10' AS TIMESTAMP_LTZ(3))";

    /** Event time of the flip-trigger row; equal to the {@link #MID_FLIP} timestamp. */
    private static final String FLIP_TRIGGER_TS = "00:00:10";

    /** A build-side key that never matches any probe row. */
    private static final String FLIP_TRIGGER_KEY = "__flip_trigger__";

    public LateralSnapshotJoinITCase(StateBackendMode state) {
        super(state);
    }

    @BeforeEach
    @Override
    public void before() {
        super.before();
        env().setParallelism(1);
        tEnv().getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Parameters(name = "StateBackend={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {StreamingWithStateTestBase.HEAP_BACKEND()},
                    {StreamingWithStateTestBase.ROCKSDB_BACKEND()}
                });
    }

    // ------------------------------------------------------------------------------------------
    // Build-side changelog consolidation (all changes have rowtime < the flip ts)
    // ------------------------------------------------------------------------------------------

    @TestTemplate
    void testRetractingBuildChangelog() {
        createProbe(
                Arrays.asList(Row.of("a", 100, ts("00:01:00")), Row.of("b", 200, ts("00:01:01"))));
        // Key a is updated 10 -> 11; key b is inserted then deleted.
        createChangelogBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "a", 10, ts("00:00:01")),
                        Row.ofKind(RowKind.INSERT, "b", 20, ts("00:00:03")),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "a", 10, ts("00:00:01")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "a", 11, ts("00:00:02")),
                        Row.ofKind(RowKind.DELETE, "b", 20, ts("00:00:03"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual).containsExactlyInAnyOrder(Row.of("a", 11));
    }

    @TestTemplate
    void testUpsertBuildSource() {
        createProbe(
                Arrays.asList(Row.of("a", 100, ts("00:01:00")), Row.of("b", 200, ts("00:01:01"))));
        // Upsert source (I,UA,D with PK): a is upserted 10 -> 11, b is deleted.
        createUpsertBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "a", 10, ts("00:00:01")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "a", 11, ts("00:00:02")),
                        Row.ofKind(RowKind.INSERT, "b", 20, ts("00:00:03")),
                        Row.ofKind(RowKind.DELETE, "b", 20, ts("00:00:04"))));

        final List<Row> actual =
                collect(
                        "SELECT probe.pk, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                + "input => TABLE b, "
                                + MID_FLIP
                                + ")) AS s ON probe.pk = s.bk");

        assertThatList(actual).containsExactlyInAnyOrder(Row.of("a", 11));
    }

    // ------------------------------------------------------------------------------------------
    // Non-deterministic behavior (asserted with tolerant checks)
    // ------------------------------------------------------------------------------------------

    @TestTemplate
    void testProbesObserveProgressiveBuildVersions() {
        // Throttled build applies v2, v3, v4 progressively after the flip while a throttled probe
        // stream spans the progression. Observed versions are non-decreasing; the first probe sees
        // v1 and the last sees v4. Exact per-probe versions are not asserted (source timing and
        // post-flip visibility latency are not precise enough across environments).
        final int probeCount = 8;
        final List<Row> probes =
                IntStream.range(0, probeCount)
                        .mapToObj(i -> Row.of("k", i, ts(String.format("00:00:%02d", i))))
                        .toList();
        createProbe(probes, 200L); // last probe at ~1400 ms, well past the last post-flip update
        createUpsertBuild(
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "k", 1, ts("00:00:01")),
                        Row.ofKind(RowKind.INSERT, FLIP_TRIGGER_KEY, 0, ts(FLIP_TRIGGER_TS)),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 2, ts("00:00:20")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 3, ts("00:00:30")),
                        Row.ofKind(RowKind.UPDATE_AFTER, "k", 4, ts("00:00:40"))),
                50L);

        final List<Row> byProbeId =
                sortedByProbeId(
                        collect(
                                "SELECT probe.pv, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b, "
                                        + MID_FLIP
                                        + ")) AS s ON probe.pk = s.bk"));

        assertThat(byProbeId).hasSize(probeCount);
        assertMonotonicVersions(byProbeId);
        assertThat((Integer) byProbeId.get(0).getField(1)).isEqualTo(1);
        assertThat((Integer) byProbeId.get(probeCount - 1).getField(1)).isEqualTo(4);
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private List<Row> collect(String query) {
        return CollectionUtil.iteratorToList(tEnv().executeSql(query).collect());
    }

    /**
     * Appends a non-matching build row at the {@link #MID_FLIP} timestamp; its watermark flips the
     * operator to the JOIN phase mid-stream (all real build rows are earlier).
     */
    private List<Row> withFlipTrigger(List<Row> data) {
        final List<Row> withTrigger = new ArrayList<>(data);
        withTrigger.add(Row.of(FLIP_TRIGGER_KEY, 0, ts(FLIP_TRIGGER_TS)));
        return withTrigger;
    }

    private void createProbe(List<Row> data) {
        createProbe(data, 0L);
    }

    private void createProbe(List<Row> data, long sleepMillis) {
        final String id = TestValuesTableFactory.registerData(data);
        final Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.STRING())
                        .column("pv", DataTypes.INT())
                        .column("pts", DataTypes.TIMESTAMP(3))
                        .watermark("pts", "pts")
                        .build();
        tEnv().createTable("probe", valuesDescriptor(schema, id, sleepMillis).build());
    }

    private void createChangelogBuild(List<Row> data) {
        final String id = TestValuesTableFactory.registerData(withFlipTrigger(data));
        final Schema schema =
                Schema.newBuilder()
                        .column("bk", DataTypes.STRING())
                        .column("bv", DataTypes.INT())
                        .column("bts", DataTypes.TIMESTAMP(3))
                        .watermark("bts", "bts")
                        .build();
        tEnv().createTable(
                        "b",
                        valuesDescriptor(schema, id, 0L)
                                .option("changelog-mode", "I,UB,UA,D")
                                .build());
    }

    private void createUpsertBuild(List<Row> data) {
        createUpsertBuild(withFlipTrigger(data), 0L);
    }

    private void createUpsertBuild(List<Row> data, long sleepMillis) {
        final String id = TestValuesTableFactory.registerData(data);
        final Schema schema =
                Schema.newBuilder()
                        .column("bk", DataTypes.STRING().notNull())
                        .column("bv", DataTypes.INT())
                        .column("bts", DataTypes.TIMESTAMP(3))
                        .watermark("bts", "bts")
                        .primaryKey("bk")
                        .build();
        tEnv().createTable(
                        "b",
                        valuesDescriptor(schema, id, sleepMillis)
                                .option("changelog-mode", "I,UA,D")
                                .build());
    }

    private static TableDescriptor.Builder valuesDescriptor(
            Schema schema, String id, long sleepMillis) {
        final TableDescriptor.Builder descriptor =
                TableDescriptor.forConnector("values")
                        .schema(schema)
                        .option("bounded", "false")
                        .option("disable-lookup", "true")
                        .option("enable-watermark-push-down", "true")
                        .option("scan.watermark.emit.strategy", "on-event")
                        .option("data-id", id);
        if (sleepMillis > 0) {
            descriptor
                    .option("source.sleep-after-elements", "1")
                    .option("source.sleep-time", sleepMillis + "ms");
        }
        return descriptor;
    }

    /** Sorts join results (pv, bv) by the probe id pv, i.e. into probe processing order. */
    private static List<Row> sortedByProbeId(List<Row> results) {
        return results.stream()
                .sorted(Comparator.comparingInt(r -> r.<Integer>getFieldAs(0)))
                .collect(Collectors.toList());
    }

    /** Asserts the build version (field 1) is non-decreasing across the probe-ordered results. */
    private static void assertMonotonicVersions(List<Row> byProbeId) {
        int previous = Integer.MIN_VALUE;
        for (Row r : byProbeId) {
            final int version = r.<Integer>getFieldAs(1);
            assertThat(version).isGreaterThanOrEqualTo(previous);
            previous = version;
        }
    }

    private static LocalDateTime ts(String time) {
        return LocalDateTime.parse("2020-01-01T" + time);
    }
}
