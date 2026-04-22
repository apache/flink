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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.ProcessTableFunction.TimeContext;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link ProcessSetTableOperator} drives {@link ProcessTableRunner} correctly when user
 * timers are fired under unaligned checkpoints, both with and without interruptible timers.
 *
 * <p>A minimal {@link ProcessTableRunner} is used: {@link ProcessTableRunner#callEval()} emits a
 * "record-$ts@wm$wm" row via {@code evalCollector} and registers an unnamed event-time timer at the
 * row's event time (plus an additional named timer for the row at ts=1000); {@link
 * ProcessTableRunner#callOnTimer()} emits a "fired-[name-]$ts@wm$wm" row via {@code
 * onTimerCollector}. The operator is wrapped so that after each timer firing a non-deferrable
 * "mail-[name-]$ts" mailbox mail is scheduled — the PTF user API does not expose the mailbox, so
 * this is the only way to observe externally whether the mailbox is drained between timer firings.
 *
 * <p>With interruptible timers enabled, the mailbox is drained between successive timer firings, so
 * each "fired" row is immediately followed by its "mail" row. With interruptible timers disabled,
 * all timers in a batch fire first (and the watermark is emitted downstream) before any of the
 * scheduled mails are processed. The watermark values on "record" and "fired" rows also validate
 * that {@link TimeContext#currentWatermark()} advances consistently across input processing and
 * timer firing.
 */
class ProcessSetTableOperatorInterruptibleTimersTest {

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.BIGINT()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)));

    // Output layout is driven by PassPartitionKeysCollector / PassAllCollector in
    // AbstractProcessTableOperator: [partition-key, PTF-emitted label, rowtime].
    private static final DataType OUTPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.BIGINT()),
                    DataTypes.FIELD("label", DataTypes.STRING()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)));

    private static final InternalTypeInfo<RowData> INPUT_TYPE_INFO =
            InternalTypeInfo.of(INPUT_TYPE.getLogicalType());

    private static final InternalTypeInfo<RowData> OUTPUT_TYPE_INFO =
            InternalTypeInfo.of(OUTPUT_TYPE.getLogicalType());

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, INPUT_TYPE_INFO.toRowFieldTypes());

    private static final long KEY = 42L;

    private static final String NAMED_TIMER = "n";

    @ParameterizedTest(name = "interruptibleTimers={0}")
    @ValueSource(booleans = {true, false})
    void testTimersThroughProcessTableRunner(boolean interruptibleTimers) throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness = createHarness(interruptibleTimers)) {
            // Each input row registers one event-time timer at its own timestamp. The row at
            // ts=1000 additionally registers a named timer at the same timestamp, so the first
            // batch contains three unnamed firings and one named firing at identical timestamps
            // (ts=1000) — exercising both timer services advancing together.
            for (long ts = 1000L; ts <= 3000L; ts += 1000L) {
                harness.processElement(
                        new StreamRecord<>(
                                GenericRowData.of(KEY, TimestampData.fromEpochMillis(ts)), ts));
            }
            // Advance watermark past registered timers (first batch: three unnamed + one named).
            harness.processElement(new Watermark(5000L));

            // One more input row followed by a second watermark advance (second batch: one
            // unnamed timer only).
            harness.processElement(
                    new StreamRecord<>(
                            GenericRowData.of(KEY, TimestampData.fromEpochMillis(6000L)), 6000L));
            harness.processElement(new Watermark(7000L));

            List<String> labels =
                    harness.getOutput().stream()
                            .map(ProcessSetTableOperatorInterruptibleTimersTest::describe)
                            .collect(Collectors.toList());

            // Records emitted from callEval carry the watermark visible during eval, which is null
            // before any watermark has arrived and 5000 for the record processed between the two
            // watermarks.
            if (interruptibleTimers) {
                // With interruptible timers, the mailbox is drained between successive timer
                // firings, so each "fired-[name-]$ts" is immediately followed by its matching
                // "mail-[name-]$ts". The watermark is only emitted downstream once all firings in
                // the batch have completed.
                assertThat(labels)
                        .containsExactly(
                                recordLabel(1000L, null),
                                recordLabel(2000L, null),
                                recordLabel(3000L, null),
                                firedLabel(null, 1000L, 5000L),
                                mailLabel(null, 1000L),
                                firedLabel(null, 2000L, 5000L),
                                mailLabel(null, 2000L),
                                firedLabel(null, 3000L, 5000L),
                                mailLabel(null, 3000L),
                                firedLabel(NAMED_TIMER, 1000L, 5000L),
                                mailLabel(NAMED_TIMER, 1000L),
                                watermarkLabel(5000L),
                                recordLabel(6000L, 5000L),
                                firedLabel(null, 6000L, 7000L),
                                mailLabel(null, 6000L),
                                watermarkLabel(7000L));
            } else {
                // Without interruptible timers, all timers in a batch fire synchronously (emitting
                // their "fired" rows) before the watermark is emitted downstream. Only afterwards
                // does the mailbox drain the scheduled "mail" rows in FIFO order.
                assertThat(labels)
                        .containsExactly(
                                recordLabel(1000L, null),
                                recordLabel(2000L, null),
                                recordLabel(3000L, null),
                                firedLabel(null, 1000L, 5000L),
                                firedLabel(null, 2000L, 5000L),
                                firedLabel(null, 3000L, 5000L),
                                firedLabel(NAMED_TIMER, 1000L, 5000L),
                                watermarkLabel(5000L),
                                mailLabel(null, 1000L),
                                mailLabel(null, 2000L),
                                mailLabel(null, 3000L),
                                mailLabel(NAMED_TIMER, 1000L),
                                recordLabel(6000L, 5000L),
                                firedLabel(null, 6000L, 7000L),
                                watermarkLabel(7000L),
                                mailLabel(null, 6000L));
            }
        }
    }

    private StreamTaskMailboxTestHarness<RowData> createHarness(boolean interruptibleTimers)
            throws Exception {
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        MultipleInputStreamTask::new, OUTPUT_TYPE_INFO)
                .addJobConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1))
                .addJobConfig(CheckpointingOptions.ENABLE_UNALIGNED, true)
                .addJobConfig(
                        CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS,
                        interruptibleTimers)
                .setKeyType(KEY_SELECTOR.getProducedType())
                .addInput(INPUT_TYPE_INFO, 1, KEY_SELECTOR)
                .setupOperatorChain(new TestOperatorFactory())
                .name("ptf")
                .finishForSingletonOperatorChain(
                        OUTPUT_TYPE_INFO.createSerializer(new SerializerConfigImpl()))
                .build();
    }

    private static String describe(Object obj) {
        if (obj instanceof Watermark) {
            return watermarkLabel(((Watermark) obj).getTimestamp());
        } else if (obj instanceof StreamRecord) {
            Object value = ((StreamRecord<?>) obj).getValue();
            if (value instanceof RowData) {
                // Field 1 is the PTF-emitted label (see OUTPUT_TYPE); fields 0 and 2 are the
                // pass-through partition key and rowtime.
                return ((RowData) value).getString(1).toString();
            }
            return value.toString();
        }
        return obj.toString();
    }

    private static String recordLabel(long ts, @Nullable Long watermark) {
        return "record-" + ts + "@wm" + (watermark == null ? "null" : watermark);
    }

    private static String firedLabel(@Nullable String name, long ts, long watermark) {
        return "fired-" + (name == null ? "" : name + "-") + ts + "@wm" + watermark;
    }

    private static String mailLabel(@Nullable String name, long ts) {
        return "mail-" + (name == null ? "" : name + "-") + ts;
    }

    private static String watermarkLabel(long ts) {
        return "Watermark@" + ts;
    }

    private static RuntimeTableSemantics tableSemantics() {
        return new RuntimeTableSemantics(
                "r",
                0,
                INPUT_TYPE,
                new int[] {0},
                new int[0],
                new RuntimeTableSemantics.SortDirection[0],
                RuntimeChangelogMode.serialize(ChangelogMode.insertOnly()),
                /* passColumnsThrough */ false,
                /* hasSetSemantics */ true,
                /* timeColumn */ 1);
    }

    // --------------------------------------------------------------------------------------------
    // Factory and operator
    // --------------------------------------------------------------------------------------------

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<RowData>> T createStreamOperator(
                StreamOperatorParameters<RowData> parameters) {
            return (T) new TestProcessSetTableOperator(parameters);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return TestProcessSetTableOperator.class;
        }
    }

    /**
     * {@link ProcessSetTableOperator} with a test-only override of {@link #onEventTime} that
     * schedules a non-deferrable "mail-[name-]$ts" emission after each timer firing. The PTF user
     * API does not expose the mailbox; this test-only shortcut is the only way to observe whether
     * interruption happens between timer firings driven by {@link
     * ProcessTableRunner#callOnTimer()}.
     */
    private static class TestProcessSetTableOperator extends ProcessSetTableOperator {

        private final MailboxExecutor mailboxExecutor;

        TestProcessSetTableOperator(StreamOperatorParameters<RowData> parameters) {
            super(
                    parameters,
                    List.of(tableSemantics()),
                    List.of(),
                    // No ORDER BY on the input, so the comparator slot is null (the operator
                    // short-circuits sort-buffer setup when orderByColumns is empty). The array
                    // size must still match the number of inputs.
                    new RecordComparator[] {null},
                    new TestProcessTableRunner(),
                    new HashFunction[0],
                    new RecordEqualiser[0],
                    RuntimeChangelogMode.serialize(ChangelogMode.insertOnly()),
                    List.of());
            this.mailboxExecutor = parameters.getMailboxExecutor();
        }

        @Override
        public void onEventTime(InternalTimer<RowData, Object> timer) throws Exception {
            // Runs the full PTF path: ingestTimerEvent -> processOnTimer -> callOnTimer, emitting
            // "fired-[name-]$ts" via onTimerCollector.
            super.onEventTime(timer);
            long ts = timer.getTimestamp();
            long keyValue = timer.getKey().getLong(0);
            final Object ns = timer.getNamespace();
            final String name = (ns instanceof StringData) ? ns.toString() : null;
            mailboxExecutor.execute(
                    () ->
                            output.collect(
                                    new StreamRecord<>(
                                            GenericRowData.of(
                                                    keyValue,
                                                    StringData.fromString(mailLabel(name, ts)),
                                                    TimestampData.fromEpochMillis(ts)))),
                    mailLabel(name, ts));
        }
    }

    /**
     * Minimal {@link ProcessTableRunner}: {@link #callEval} emits a "record-$ts@wm$wm" row and
     * registers an unnamed event-time timer at the input row's event time (plus a named timer for
     * the row at ts=1000); {@link #callOnTimer} emits a "fired-[name-]$ts@wm$wm" row.
     */
    private static class TestProcessTableRunner extends ProcessTableRunner {

        @Override
        public void callEval() {
            final TimeContext<Long> tc = runnerContext.timeContext(Long.class);
            final long ts = Objects.requireNonNull(tc.time(), "input row event time");
            final Long wm = tc.currentWatermark();
            tc.registerOnTime(ts);
            if (ts == 1000L) {
                tc.registerOnTime(NAMED_TIMER, ts);
            }
            evalCollector.collect(GenericRowData.of(StringData.fromString(recordLabel(ts, wm))));
        }

        @Override
        public void callOnTimer() {
            final TimeContext<Long> tc = runnerOnTimerContext.timeContext(Long.class);
            final long ts = Objects.requireNonNull(tc.time(), "timer fire time");
            final long wm =
                    Objects.requireNonNull(tc.currentWatermark(), "watermark at timer fire");
            final String name = runnerOnTimerContext.currentTimer();
            onTimerCollector.collect(
                    GenericRowData.of(StringData.fromString(firedLabel(name, ts, wm))));
        }
    }
}
