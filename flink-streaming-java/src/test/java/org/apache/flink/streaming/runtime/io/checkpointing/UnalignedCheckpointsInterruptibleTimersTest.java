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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.MailboxWatermarkProcessor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.YieldingOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests interaction between {@link CheckpointingOptions#ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS} and
 * unaligned checkpoints.
 */
@ExtendWith(TestLoggerExtension.class)
class UnalignedCheckpointsInterruptibleTimersTest {

    @Test
    void testSingleWatermarkHoldingOperatorInTheChain() throws Exception {
        final Instant firstWindowEnd = Instant.ofEpochMilli(1000L);
        final int numFirstWindowTimers = 2;
        final Instant secondWindowEnd = Instant.ofEpochMilli(2000L);
        final int numSecondWindowTimers = 2;

        try (final StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, Types.STRING)
                        .modifyStreamConfig(
                                UnalignedCheckpointsInterruptibleTimersTest::setupStreamConfig)
                        .addInput(Types.STRING)
                        .setupOperatorChain(
                                SimpleOperatorFactory.of(
                                        new MultipleTimersAtTheSameTimestamp()
                                                .withTimers(firstWindowEnd, numFirstWindowTimers)
                                                .withTimers(
                                                        secondWindowEnd, numSecondWindowTimers)))
                        .name("first")
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {
            harness.processElement(new StreamRecord<>("register timers"));
            harness.processAll();
            harness.processElement(asWatermark(firstWindowEnd));
            harness.processElement(asWatermark(secondWindowEnd));

            assertThat(harness.getOutput())
                    .containsExactly(
                            asFiredRecord("key-0"),
                            asMailRecord("key-0"),
                            asFiredRecord("key-1"),
                            asMailRecord("key-1"),
                            asWatermark(firstWindowEnd),
                            asFiredRecord("key-0"),
                            asMailRecord("key-0"),
                            asFiredRecord("key-1"),
                            asMailRecord("key-1"),
                            asWatermark(secondWindowEnd));
        }
    }

    @Test
    void testWatermarkProgressWithNoTimers() throws Exception {
        final Instant firstWindowEnd = Instant.ofEpochMilli(1000L);
        final Instant secondWindowEnd = Instant.ofEpochMilli(2000L);

        try (final StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, Types.STRING)
                        .modifyStreamConfig(
                                UnalignedCheckpointsInterruptibleTimersTest::setupStreamConfig)
                        .addInput(Types.STRING)
                        .setupOperatorChain(
                                SimpleOperatorFactory.of(new MultipleTimersAtTheSameTimestamp()))
                        .name("first")
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {
            harness.setAutoProcess(false);
            harness.processElement(new StreamRecord<>("impulse"));
            harness.processAll();
            harness.processElement(asWatermark(firstWindowEnd));
            harness.processElement(asWatermark(secondWindowEnd));

            final List<Watermark> seenWatermarks = new ArrayList<>();
            while (seenWatermarks.size() < 2) {
                harness.processSingleStep();
                Object outputElement;
                while ((outputElement = harness.getOutput().poll()) != null) {
                    if (outputElement instanceof Watermark) {
                        seenWatermarks.add((Watermark) outputElement);
                    }
                }
            }
            assertThat(seenWatermarks)
                    .containsExactly(asWatermark(firstWindowEnd), asWatermark(secondWindowEnd));
        }
    }

    private static Watermark asWatermark(Instant timestamp) {
        return new Watermark(timestamp.toEpochMilli());
    }

    private static StreamRecord<String> asFiredRecord(String key) {
        return new StreamRecord("fired-" + key);
    }

    private static StreamRecord<String> asMailRecord(String key) {
        return new StreamRecord("mail-" + key);
    }

    private static void setupStreamConfig(StreamConfig cfg) {
        cfg.setUnalignedCheckpointsEnabled(true);
        cfg.setUnalignedCheckpointsSplittableTimersEnabled(true);
        cfg.setStateKeySerializer(StringSerializer.INSTANCE);
    }

    private static class MultipleTimersAtTheSameTimestamp extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>,
                    Triggerable<String, String>,
                    YieldingOperator<String> {

        private final Map<Instant, Integer> timersToRegister;
        private transient @Nullable MailboxExecutor mailboxExecutor;
        private transient @Nullable MailboxWatermarkProcessor watermarkProcessor;

        MultipleTimersAtTheSameTimestamp() {
            this(Collections.emptyMap());
        }

        MultipleTimersAtTheSameTimestamp(Map<Instant, Integer> timersToRegister) {
            this.timersToRegister = timersToRegister;
        }

        @Override
        public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public void open() throws Exception {
            super.open();
            if (getTimeServiceManager().isPresent()) {
                this.watermarkProcessor =
                        new MailboxWatermarkProcessor(
                                output, mailboxExecutor, getTimeServiceManager().get());
            }
        }

        @Override
        public void processElement(StreamRecord<String> element) {
            if (!timersToRegister.isEmpty()) {
                final InternalTimerService<String> timers =
                        getInternalTimerService("timers", StringSerializer.INSTANCE, this);
                for (Map.Entry<Instant, Integer> entry : timersToRegister.entrySet()) {
                    for (int keyIdx = 0; keyIdx < entry.getValue(); keyIdx++) {
                        final String key = String.format("key-%d", keyIdx);
                        setCurrentKey(key);
                        timers.registerEventTimeTimer(
                                String.format("window-%s", entry.getKey()),
                                entry.getKey().toEpochMilli());
                    }
                }
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            if (watermarkProcessor == null) {
                super.processWatermark(mark);
            } else {
                watermarkProcessor.emitWatermarkInsideMailbox(mark);
            }
        }

        @Override
        public void onEventTime(InternalTimer<String, String> timer) throws Exception {
            mailboxExecutor.execute(
                    () -> output.collect(asMailRecord(timer.getKey())), "mail-" + timer.getKey());
            output.collect(asFiredRecord(timer.getKey()));
        }

        @Override
        public void onProcessingTime(InternalTimer<String, String> timer) throws Exception {}

        MultipleTimersAtTheSameTimestamp withTimers(Instant timestamp, int count) {
            final Map<Instant, Integer> copy = new HashMap<>(timersToRegister);
            copy.put(timestamp, count);
            return new MultipleTimersAtTheSameTimestamp(copy);
        }
    }
}
