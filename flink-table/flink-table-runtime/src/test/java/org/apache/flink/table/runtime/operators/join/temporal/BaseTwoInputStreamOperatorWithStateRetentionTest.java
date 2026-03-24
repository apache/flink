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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link BaseTwoInputStreamOperatorWithStateRetention} supports interruptible timers
 * with unaligned checkpoints.
 */
class BaseTwoInputStreamOperatorWithStateRetentionTest {

    private static final InternalTypeInfo<RowData> ROW_TYPE =
            InternalTypeInfo.ofFields(new BigIntType(), VarCharType.STRING_TYPE);

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {0}, ROW_TYPE.toRowFieldTypes());

    @Test
    void testInterruptibleTimersWithWatermarks() throws Exception {
        final Instant firstWindowEnd = Instant.ofEpochMilli(1000L);
        final int numFirstWindowTimers = 2;
        final Instant secondWindowEnd = Instant.ofEpochMilli(2000L);
        final int numSecondWindowTimers = 2;

        try (StreamTaskMailboxTestHarness<RowData> harness =
                createHarness(
                        new StubOperatorWithEventTimeTimers(0, 0)
                                .withTimers(firstWindowEnd, numFirstWindowTimers)
                                .withTimers(secondWindowEnd, numSecondWindowTimers))) {
            harness.processElement(new StreamRecord<>(row(1L, "register")), 0);
            harness.processAll();
            // Advance watermarks on both inputs so the combined watermark progresses.
            harness.processElement(asWatermark(firstWindowEnd), 0);
            harness.processElement(asWatermark(firstWindowEnd), 1);
            harness.processElement(asWatermark(secondWindowEnd), 0);
            harness.processElement(asWatermark(secondWindowEnd), 1);

            List<String> output =
                    harness.getOutput().stream()
                            .map(BaseTwoInputStreamOperatorWithStateRetentionTest::describeRecord)
                            .collect(Collectors.toList());
            assertThat(output)
                    .containsExactly(
                            firedDesc(0L),
                            mailDesc(0L),
                            firedDesc(1L),
                            mailDesc(1L),
                            watermarkDesc(firstWindowEnd),
                            firedDesc(0L),
                            mailDesc(0L),
                            firedDesc(1L),
                            mailDesc(1L),
                            watermarkDesc(secondWindowEnd));
        }
    }

    private StreamTaskMailboxTestHarness<RowData> createHarness(
            StubOperatorWithEventTimeTimers operator) throws Exception {
        return new StreamTaskMailboxTestHarnessBuilder<>(
                        (Environment env) -> new TwoInputStreamTask<>(env), ROW_TYPE)
                .addJobConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1))
                .addJobConfig(CheckpointingOptions.ENABLE_UNALIGNED, true)
                .addJobConfig(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true)
                .setKeyType(KEY_SELECTOR.getProducedType())
                .addInput(ROW_TYPE, 1, KEY_SELECTOR)
                .addInput(ROW_TYPE, 1, KEY_SELECTOR)
                .setupOperatorChain(SimpleOperatorFactory.of(operator))
                .name("temporal-join")
                .finishForSingletonOperatorChain(ROW_TYPE.createSerializer(null))
                .build();
    }

    private static RowData row(long key, String value) {
        return GenericRowData.of(key, StringData.fromString(value));
    }

    private static Watermark asWatermark(Instant timestamp) {
        return new Watermark(timestamp.toEpochMilli());
    }

    private static String describeRecord(Object obj) {
        if (obj instanceof Watermark) {
            return "Watermark@" + ((Watermark) obj).getTimestamp();
        } else if (obj instanceof StreamRecord) {
            Object value = ((StreamRecord<?>) obj).getValue();
            if (value instanceof RowData) {
                RowData row = (RowData) value;
                return row.getLong(0) + "," + row.getString(1).toString();
            }
            return value.toString();
        }
        return obj.toString();
    }

    private static String firedDesc(long key) {
        return key + ",fired";
    }

    private static String mailDesc(long key) {
        return key + ",mail";
    }

    private static String watermarkDesc(Instant timestamp) {
        return "Watermark@" + timestamp.toEpochMilli();
    }

    /**
     * A stub {@link BaseTwoInputStreamOperatorWithStateRetention} that registers event-time timers
     * and emits records when they fire, used to verify interruptible timer support.
     */
    private static class StubOperatorWithEventTimeTimers
            extends BaseTwoInputStreamOperatorWithStateRetention
            implements YieldingOperator<RowData> {

        private final Map<Instant, Integer> timersToRegister;
        private transient @Nullable MailboxExecutor mailboxExecutor;
        private transient InternalTimerService<VoidNamespace> eventTimeTimerService;

        StubOperatorWithEventTimeTimers(long minRetentionTime, long maxRetentionTime) {
            this(minRetentionTime, maxRetentionTime, Collections.emptyMap());
        }

        StubOperatorWithEventTimeTimers(
                long minRetentionTime,
                long maxRetentionTime,
                Map<Instant, Integer> timersToRegister) {
            super(minRetentionTime, maxRetentionTime);
            this.timersToRegister = timersToRegister;
        }

        @Override
        public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
            super.setMailboxExecutor(mailboxExecutor);
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public void open() throws Exception {
            super.open();
            eventTimeTimerService =
                    getInternalTimerService("event-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public void processElement1(StreamRecord<RowData> element) {
            registerTimers();
        }

        @Override
        public void processElement2(StreamRecord<RowData> element) {
            registerTimers();
        }

        private void registerTimers() {
            for (Map.Entry<Instant, Integer> entry : timersToRegister.entrySet()) {
                for (int keyIdx = 0; keyIdx < entry.getValue(); keyIdx++) {
                    setCurrentKey(GenericRowData.of((long) keyIdx, null));
                    eventTimeTimerService.registerEventTimeTimer(
                            VoidNamespace.INSTANCE, entry.getKey().toEpochMilli());
                }
            }
        }

        @Override
        public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
            RowData key = (RowData) timer.getKey();
            long keyValue = key.getLong(0);
            mailboxExecutor.execute(
                    () -> output.collect(new StreamRecord<>(row(keyValue, "mail"))),
                    "mail-" + keyValue);
            output.collect(new StreamRecord<>(row(keyValue, "fired")));
        }

        @Override
        public void cleanupState(long time) {}

        StubOperatorWithEventTimeTimers withTimers(Instant timestamp, int count) {
            Map<Instant, Integer> copy = new HashMap<>(timersToRegister);
            copy.put(timestamp, count);
            return new StubOperatorWithEventTimeTimers(0, 0, copy);
        }
    }
}
