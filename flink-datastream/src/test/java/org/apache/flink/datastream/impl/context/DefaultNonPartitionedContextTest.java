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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.impl.common.TestingTimestampCollector;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultNonPartitionedContext}. */
class DefaultNonPartitionedContextTest {
    @Test
    void testApplyToAllPartitions() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        List<Integer> collectedData = new ArrayList<>();

        TestingTimestampCollector<Integer> collector =
                TestingTimestampCollector.<Integer>builder()
                        .setCollectConsumer(collectedData::add)
                        .build();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        StreamingRuntimeContext operatorRuntimeContext =
                ContextTestUtils.createStreamingRuntimeContext();
        DefaultRuntimeContext runtimeContext =
                new DefaultRuntimeContext(
                        operatorRuntimeContext.getJobInfo().getJobName(),
                        operatorRuntimeContext.getJobType(),
                        1,
                        2,
                        "mock-task",
                        operatorRuntimeContext.getMetricGroup());

        DefaultNonPartitionedContext<Integer> nonPartitionedContext =
                new DefaultNonPartitionedContext<>(
                        runtimeContext,
                        new DefaultPartitionedContext(
                                runtimeContext,
                                Optional::empty,
                                (key) -> cf.complete(null),
                                UnsupportedProcessingTimeManager.INSTANCE,
                                ContextTestUtils.createStreamingRuntimeContext(),
                                new MockOperatorStateStore(),
                                null), // TODOJEY
                        collector,
                        false,
                        null,
                        null); // TODOJEY
        nonPartitionedContext.applyToAllPartitions(
                (out, ctx) -> {
                    counter.incrementAndGet();
                    out.collect(10);
                });
        assertThat(counter.get()).isEqualTo(1);
        assertThat(cf).isNotCompleted();
        assertThat(collectedData).containsExactly(10);
    }

    @Test
    void testKeyedApplyToAllPartitions() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        List<Integer> collectedData = new ArrayList<>();

        TestingTimestampCollector<Integer> collector =
                TestingTimestampCollector.<Integer>builder()
                        .setCollectConsumer(collectedData::add)
                        .build();
        // put all keys
        Set<Object> allKeys = new HashSet<>();
        allKeys.add(1);
        allKeys.add(2);
        allKeys.add(3);

        AtomicInteger currentKey = new AtomicInteger(-1);
        StreamingRuntimeContext operatorRuntimeContext =
                ContextTestUtils.createStreamingRuntimeContext();
        DefaultRuntimeContext runtimeContext =
                new DefaultRuntimeContext(
                        operatorRuntimeContext.getJobInfo().getJobName(),
                        operatorRuntimeContext.getJobType(),
                        1,
                        2,
                        "mock-task",
                        operatorRuntimeContext.getMetricGroup());
        DefaultNonPartitionedContext<Integer> nonPartitionedContext =
                new DefaultNonPartitionedContext<>(
                        runtimeContext,
                        new DefaultPartitionedContext(
                                runtimeContext,
                                currentKey::get,
                                (key) -> currentKey.set((Integer) key),
                                UnsupportedProcessingTimeManager.INSTANCE,
                                ContextTestUtils.createStreamingRuntimeContext(),
                                new MockOperatorStateStore(),
                                null), // TODOJEY
                        collector,
                        true,
                        allKeys,
                        null); // TODOJEY
        nonPartitionedContext.applyToAllPartitions(
                (out, ctx) -> {
                    counter.incrementAndGet();
                    Integer key = ctx.getStateManager().getCurrentKey();
                    assertThat(key).isIn(allKeys);
                    out.collect(key);
                });
        assertThat(counter.get()).isEqualTo(allKeys.size());
        assertThat(collectedData).containsExactlyInAnyOrder(1, 2, 3);
    }
}
