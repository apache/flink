/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for FLINK-40499: a {@link MultipleInputStreamTask} whose chained source went IDLE must still
 * deliver {@link Watermark#MAX_WATERMARK} through the chained source output when the task is
 * drained, so that downstream event-time timers and windows fire at end of job.
 *
 * <p>This is the multi-input counterpart of {@code SourceOperatorStreamTaskIdleDrainTest}: {@code
 * ChainingOutput#emitWatermark} drops watermarks while the announced status is IDLE, so {@code
 * MultipleInputStreamTask#advanceToEndOfEventTime()} must emit {@link WatermarkStatus#ACTIVE}
 * before the MAX watermark.
 */
class MultipleInputStreamTaskIdleDrainTest {

    @Test
    void maxWatermarkReachesOutputAtDrainWhenChainedSourceWasIdle() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 1)
                        .addSourceInput(
                                new SourceOperatorFactory<>(
                                        new MockSource(
                                                Boundedness.CONTINUOUS_UNBOUNDED, 2, true, true),
                                        WatermarkStrategy.noWatermarks()),
                                BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 1)
                        .setupOutputForSingletonOperatorChain(
                                new MultipleInputStreamTaskTest
                                        .MapToStringMultipleInputOperatorFactory(3))
                        .build()) {

            // the source reader has no splits and marks the chained source output IDLE
            testHarness.processAll();

            // make both network inputs idle so the chained source's watermark is the deciding one
            testHarness.processElement(WatermarkStatus.IDLE, 0, 0);
            testHarness.processElement(WatermarkStatus.IDLE, 1, 0);
            testHarness.processAll();

            assertThat(testHarness.getOutput())
                    .as("Precondition: every input (incl. the chained source) went idle")
                    .contains(WatermarkStatus.IDLE);

            // this is what StreamTask.endData(DRAIN) invokes at drain
            testHarness.getStreamTask().advanceToEndOfEventTime();
            testHarness.processAll();

            assertThat(testHarness.getOutput())
                    .as(
                            "At drain, WatermarkStatus.ACTIVE followed by MAX_WATERMARK must be "
                                    + "emitted even though the chained source's last announced "
                                    + "status was IDLE")
                    .containsSubsequence(
                            WatermarkStatus.IDLE, WatermarkStatus.ACTIVE, Watermark.MAX_WATERMARK);
        }
    }
}
