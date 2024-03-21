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

package org.apache.flink.process.impl.context;

import org.apache.flink.process.api.context.ProcessingTimeManager;
import org.apache.flink.process.api.context.StateManager;
import org.apache.flink.process.impl.common.TestingTimestampCollector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultTwoOutputNonPartitionedContext}. */
class DefaultTwoOutputNonPartitionedContextTest {
    @Test
    void testGetStateManager() {
        DefaultTwoOutputNonPartitionedContext<Void, Void> context =
                new DefaultTwoOutputNonPartitionedContext<>(null, null, null);
        StateManager stateManager = context.getStateManager();
        assertThat(stateManager.getCurrentKey()).isEmpty();
    }

    @Test
    void testGetProcessingTimeManager() {
        DefaultTwoOutputNonPartitionedContext<Void, Void> context =
                new DefaultTwoOutputNonPartitionedContext<>(null, null, null);
        ProcessingTimeManager processingTimeManager = context.getProcessingTimeManager();
        assertThatThrownBy(processingTimeManager::currentProcessingTime)
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> processingTimeManager.registerProcessingTimer(1L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> processingTimeManager.deleteProcessingTimeTimer(1L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testApplyToAllPartitions() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        List<Integer> collectedFromFirstOutput = new ArrayList<>();
        List<Long> collectedFromSecondOutput = new ArrayList<>();

        TestingTimestampCollector<Integer> firstCollector =
                TestingTimestampCollector.<Integer>builder()
                        .setCollectConsumer(collectedFromFirstOutput::add)
                        .build();
        TestingTimestampCollector<Long> secondCollector =
                TestingTimestampCollector.<Long>builder()
                        .setCollectConsumer(collectedFromSecondOutput::add)
                        .build();
        DefaultTwoOutputNonPartitionedContext<Integer, Long> nonPartitionedContext =
                new DefaultTwoOutputNonPartitionedContext<>(
                        new DefaultRuntimeContext(
                                ContextTestUtils.createStreamingRuntimeContext(),
                                1,
                                2,
                                "mock-task",
                                Optional::empty,
                                (ignore) -> {},
                                UnsupportedProcessingTimeManager.INSTANCE),
                        firstCollector,
                        secondCollector);
        nonPartitionedContext.applyToAllPartitions(
                (firstOutput, secondOutput, ctx) -> {
                    counter.incrementAndGet();
                    assertThat(ctx.getStateManager().getCurrentKey()).isEmpty();
                    firstOutput.collect(10);
                    secondOutput.collect(20L);
                });
        assertThat(counter.get()).isEqualTo(1);
        assertThat(collectedFromFirstOutput).containsExactly(10);
        assertThat(collectedFromSecondOutput).containsExactly(20L);
    }
}
