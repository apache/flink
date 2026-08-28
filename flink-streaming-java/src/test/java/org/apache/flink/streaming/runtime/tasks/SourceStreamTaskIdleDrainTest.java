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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for FLINK-40499: a {@link SourceStreamTask} whose legacy source went IDLE before finishing
 * must still emit {@link Watermark#MAX_WATERMARK} when the task is drained.
 */
class SourceStreamTaskIdleDrainTest {

    @Test
    void maxWatermarkIsEmittedAtDrainEvenIfSourceWentIdle() throws Exception {
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                SourceStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .setCollectNetworkEvents()
                        .setupOutputForSingletonOperatorChain(
                                new StreamSource<>(new IdleBeforeFinishingSource()))
                        .build()) {

            final EndOfData endOfData = new EndOfData(StopMode.DRAIN);
            testHarness.processUntil(() -> testHarness.getOutput().contains(endOfData));

            assertThat(testHarness.getOutput())
                    .containsExactly(
                            WatermarkStatus.IDLE,
                            WatermarkStatus.ACTIVE,
                            Watermark.MAX_WATERMARK,
                            endOfData);
        }
    }

    private static class IdleBeforeFinishingSource implements SourceFunction<Integer> {

        @Override
        public void run(SourceContext<Integer> ctx) {
            ctx.markAsTemporarilyIdle();
        }

        @Override
        public void cancel() {}
    }
}
