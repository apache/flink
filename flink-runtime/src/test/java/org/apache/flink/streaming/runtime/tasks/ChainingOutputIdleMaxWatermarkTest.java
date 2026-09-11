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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for FLINK-40499: {@link ChainingOutput#emitWatermark(Watermark)} drops watermarks while the
 * announced status is IDLE, so tasks must emit {@link WatermarkStatus#ACTIVE} before the final
 * {@link Watermark#MAX_WATERMARK} at end-of-job drain to make sure it is delivered.
 */
class ChainingOutputIdleMaxWatermarkTest {

    @Test
    void activeStatusFollowedByMaxWatermarkIsDeliveredAfterIdle() {
        final CollectingInput input = new CollectingInput();
        final ChainingOutput<String> chainingOutput = createChainingOutput(input);

        chainingOutput.emitWatermarkStatus(WatermarkStatus.IDLE);
        // this is what the drain path (advanceToEndOfEventTime) does: re-activate, then emit MAX
        chainingOutput.emitWatermarkStatus(WatermarkStatus.ACTIVE);
        chainingOutput.emitWatermark(Watermark.MAX_WATERMARK);

        assertThat(input.events)
                .as(
                        "Re-activating the output before the final MAX_WATERMARK must deliver "
                                + "both events downstream")
                .containsExactly(
                        WatermarkStatus.IDLE, WatermarkStatus.ACTIVE, Watermark.MAX_WATERMARK);
    }

    /**
     * Characterization of the idle gate: a watermark emitted while the announced status is IDLE is
     * dropped. This is why the task-level drain path must re-activate the output first instead of
     * relying on the watermark passing through.
     */
    @Test
    void maxWatermarkAloneIsDroppedWhileIdle() {
        final CollectingInput input = new CollectingInput();
        final ChainingOutput<String> chainingOutput = createChainingOutput(input);

        chainingOutput.emitWatermarkStatus(WatermarkStatus.IDLE);
        chainingOutput.emitWatermark(Watermark.MAX_WATERMARK);

        assertThat(input.events).containsExactly(WatermarkStatus.IDLE);
    }

    /**
     * Documents why {@code MultipleInputStreamTask#advanceToEndOfEventTime()} may emit ACTIVE
     * unconditionally: on a task deployed as finished, the chained source output never left its
     * initial ACTIVE status, so the redundant ACTIVE is deduplicated and never reaches the
     * status-rejecting {@link FinishedOnRestoreInput} underneath. The input is genuinely reachable
     * (a differing status does throw) — it is the deduplication that makes ACTIVE safe.
     */
    @Test
    void redundantActiveIsDeduplicatedBeforeFinishedOnRestoreInput() {
        final ChainingOutput<String> chainingOutput =
                new ChainingOutput<>(
                        new FinishedOnRestoreInput<>(new RecordWriterOutput[0], 1),
                        null,
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(),
                        null);

        assertThatCode(() -> chainingOutput.emitWatermarkStatus(WatermarkStatus.ACTIVE))
                .as("A redundant ACTIVE must be deduplicated and never reach the input")
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> chainingOutput.emitWatermarkStatus(WatermarkStatus.IDLE))
                .as("A status change does reach FinishedOnRestoreInput, which rejects it")
                .isInstanceOf(ExceptionInChainedOperatorException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private static ChainingOutput<String> createChainingOutput(CollectingInput input) {
        return new ChainingOutput<>(
                input,
                null,
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup(),
                null);
    }

    private static final class CollectingInput implements Input<String> {
        final List<Object> events = new ArrayList<>();

        @Override
        public void processElement(StreamRecord<String> element) {
            events.add(element);
        }

        @Override
        public void processWatermark(Watermark mark) {
            events.add(mark);
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) {
            events.add(watermarkStatus);
        }

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) {
            events.add(latencyMarker);
        }

        @Override
        public void setKeyContextElement(StreamRecord<String> record) {}
    }
}
