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

import org.apache.flink.process.impl.common.TestingTimestampCollector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultKeyedTwoOutputNonPartitionedContext}. */
class DefaultKeyedTwoOutputNonPartitionedContextTest {
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
        // put all keys
        List<Object> allKeys = new ArrayList<>();
        allKeys.add(1);
        allKeys.add(2);
        allKeys.add(3);
        TestingAllKeysContext keysContext =
                TestingAllKeysContext.builder()
                        .setGetAllKeysIterSupplier(allKeys::iterator)
                        .build();

        AtomicInteger currentKey = new AtomicInteger(-1);
        DefaultKeyedTwoOutputNonPartitionedContext<Integer, Long> nonPartitionedContext =
                new DefaultKeyedTwoOutputNonPartitionedContext<>(
                        keysContext,
                        new DefaultRuntimeContext(
                                ContextTestUtils.createStreamingRuntimeContext(),
                                1,
                                2,
                                "mock-task",
                                () -> Optional.of(currentKey.get()),
                                (key) -> currentKey.set((Integer) key),
                                UnsupportedProcessingTimeManager.INSTANCE),
                        firstCollector,
                        secondCollector);
        nonPartitionedContext.applyToAllPartitions(
                (firstOut, secondOut, ctx) -> {
                    counter.incrementAndGet();
                    Optional<Integer> key = ctx.getStateManager().getCurrentKey();
                    assertThat(key)
                            .isPresent()
                            .hasValueSatisfying(v -> assertThat(v).isIn(allKeys));
                    firstOut.collect(key.get());
                    secondOut.collect(Long.valueOf(key.get()));
                });
        assertThat(counter.get()).isEqualTo(allKeys.size());
        assertThat(collectedFromFirstOutput).containsExactlyInAnyOrder(1, 2, 3);
        assertThat(collectedFromSecondOutput).containsExactlyInAnyOrder(1L, 2L, 3L);
    }
}
