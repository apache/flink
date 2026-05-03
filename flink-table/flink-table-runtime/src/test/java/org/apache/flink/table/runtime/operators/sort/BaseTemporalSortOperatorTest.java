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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link BaseTemporalSortOperator} supports interruptible timers with unaligned
 * checkpoints.
 */
class BaseTemporalSortOperatorTest {

    private static final InternalTypeInfo<RowData> ROW_TYPE =
            InternalTypeInfo.ofFields(new BigIntType(), VarCharType.STRING_TYPE);

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {0}, ROW_TYPE.toRowFieldTypes());

    @Test
    void testInterruptibleTimersWithWatermarks() throws Exception {
        try (StreamTaskMailboxTestHarness<RowData> harness = createHarness()) {
            // Register event-time timers at 4 distinct timestamps under the same key.
            long key = 42L;
            for (long ts = 1000L; ts <= 4000L; ts += 1000L) {
                harness.processElement(
                        new StreamRecord<>(GenericRowData.of(key, StringData.fromString("v")), ts));
            }
            harness.processAll();

            // Advance watermark past all timers.
            harness.processElement(new Watermark(5000L));

            List<String> output =
                    harness.getOutput().stream()
                            .map(BaseTemporalSortOperatorTest::describeRecord)
                            .collect(Collectors.toList());
            // With interruptible timers, each timer fires and yields to the mailbox
            // before the next timer fires. The mail record between fired records proves this.
            assertThat(output)
                    .containsExactly(
                            firedDesc(1000L),
                            mailDesc(1000L),
                            firedDesc(2000L),
                            mailDesc(2000L),
                            firedDesc(3000L),
                            mailDesc(3000L),
                            firedDesc(4000L),
                            mailDesc(4000L),
                            watermarkDesc(5000L));
        }
    }

    private StreamTaskMailboxTestHarness<RowData> createHarness() throws Exception {
        return new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, ROW_TYPE)
                .addJobConfig(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1))
                .addJobConfig(CheckpointingOptions.ENABLE_UNALIGNED, true)
                .addJobConfig(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, true)
                .setKeyType(KEY_SELECTOR.getProducedType())
                .addInput(ROW_TYPE, 1, KEY_SELECTOR)
                .setupOperatorChain(SimpleOperatorFactory.of(new StubTemporalSortOperator()))
                .name("sort")
                .finishForSingletonOperatorChain(ROW_TYPE.createSerializer(null))
                .build();
    }

    private static String describeRecord(Object obj) {
        if (obj instanceof Watermark) {
            return watermarkDesc(((Watermark) obj).getTimestamp());
        } else if (obj instanceof StreamRecord) {
            Object value = ((StreamRecord<?>) obj).getValue();
            if (value instanceof RowData) {
                RowData row = (RowData) value;
                return row.getString(1).toString();
            }
            return value.toString();
        }
        return obj.toString();
    }

    private static String firedDesc(long timestamp) {
        return "fired-" + timestamp;
    }

    private static String mailDesc(long timestamp) {
        return "mail-" + timestamp;
    }

    private static String watermarkDesc(long timestamp) {
        return "Watermark@" + timestamp;
    }

    /**
     * A stub {@link BaseTemporalSortOperator} that registers event-time timers per key and emits
     * labeled records when they fire, used to verify interruptible timer support.
     */
    private static class StubTemporalSortOperator extends BaseTemporalSortOperator
            implements YieldingOperator<RowData> {

        private transient @Nullable MailboxExecutor mailboxExecutor;

        @Override
        public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
            super.setMailboxExecutor(mailboxExecutor);
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws Exception {
            setCurrentKey(element.getValue());
            timerService.registerEventTimeTimer(element.getTimestamp());
        }

        @Override
        public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
            long ts = timer.getTimestamp();
            long keyValue = timer.getKey().getLong(0);
            mailboxExecutor.execute(
                    () ->
                            collector.collect(
                                    GenericRowData.of(
                                            keyValue, StringData.fromString(mailDesc(ts)))),
                    mailDesc(ts));
            collector.collect(GenericRowData.of(keyValue, StringData.fromString(firedDesc(ts))));
        }

        @Override
        public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) {}
    }
}
