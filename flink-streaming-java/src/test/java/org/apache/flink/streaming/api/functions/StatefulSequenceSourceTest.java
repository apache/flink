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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.BlockingSourceContext;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StatefulSequenceSource}. */
class StatefulSequenceSourceTest {

    @Test
    void testCheckpointRestore() throws Exception {
        final int initElement = 0;
        final int maxElement = 100;
        final int maxParallelsim = 2;

        final Set<Long> expectedOutput = new HashSet<>();
        for (long i = initElement; i <= maxElement; i++) {
            expectedOutput.add(i);
        }

        final ConcurrentHashMap<String, List<Long>> outputCollector = new ConcurrentHashMap<>();
        final OneShotLatch latchToTrigger1 = new OneShotLatch();
        final OneShotLatch latchToWait1 = new OneShotLatch();
        final OneShotLatch latchToTrigger2 = new OneShotLatch();
        final OneShotLatch latchToWait2 = new OneShotLatch();

        final StatefulSequenceSource source1 = new StatefulSequenceSource(initElement, maxElement);
        StreamSource<Long, StatefulSequenceSource> src1 = new StreamSource<>(source1);

        final AbstractStreamOperatorTestHarness<Long> testHarness1 =
                new AbstractStreamOperatorTestHarness<>(src1, maxParallelsim, 2, 0);
        testHarness1.open();

        final StatefulSequenceSource source2 = new StatefulSequenceSource(initElement, maxElement);
        StreamSource<Long, StatefulSequenceSource> src2 = new StreamSource<>(source2);

        final AbstractStreamOperatorTestHarness<Long> testHarness2 =
                new AbstractStreamOperatorTestHarness<>(src2, maxParallelsim, 2, 1);
        testHarness2.open();

        // run the source asynchronously
        CheckedThread runner1 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source1.run(
                                new BlockingSourceContext<>(
                                        "1", latchToTrigger1, latchToWait1, outputCollector, 21));
                    }
                };

        // run the source asynchronously
        CheckedThread runner2 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source2.run(
                                new BlockingSourceContext<>(
                                        "2", latchToTrigger2, latchToWait2, outputCollector, 32));
                    }
                };

        runner1.start();
        runner2.start();

        if (!latchToTrigger1.isTriggered()) {
            latchToTrigger1.await();
        }

        if (!latchToTrigger2.isTriggered()) {
            latchToTrigger2.await();
        }

        OperatorSubtaskState snapshot =
                AbstractStreamOperatorTestHarness.repackageState(
                        testHarness1.snapshot(0L, 0L), testHarness2.snapshot(0L, 0L));

        final StatefulSequenceSource source3 = new StatefulSequenceSource(initElement, maxElement);
        StreamSource<Long, StatefulSequenceSource> src3 = new StreamSource<>(source3);

        final OperatorSubtaskState initState =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        snapshot, maxParallelsim, 2, 1, 0);

        final AbstractStreamOperatorTestHarness<Long> testHarness3 =
                new AbstractStreamOperatorTestHarness<>(src3, maxParallelsim, 1, 0);
        testHarness3.setup();
        testHarness3.initializeState(initState);
        testHarness3.open();

        final OneShotLatch latchToTrigger3 = new OneShotLatch();
        final OneShotLatch latchToWait3 = new OneShotLatch();
        latchToWait3.trigger();

        // run the source asynchronously
        CheckedThread runner3 =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        source3.run(
                                new BlockingSourceContext<>(
                                        "3", latchToTrigger3, latchToWait3, outputCollector, 3));
                    }
                };
        runner3.start();
        runner3.sync();

        assertThat(outputCollector).hasSize(3); // we have 3 tasks.

        // test for at-most-once
        Set<Long> dedupRes = new HashSet<>(Math.abs(maxElement - initElement) + 1);
        for (Map.Entry<String, List<Long>> elementsPerTask : outputCollector.entrySet()) {
            String key = elementsPerTask.getKey();
            List<Long> elements = outputCollector.get(key);

            // this tests the correctness of the latches in the test
            assertThat(elements).isNotEmpty();

            for (Long elem : elements) {
                assertThat(dedupRes.add(elem)).as("Duplicate entry: " + elem).isTrue();

                assertThat(expectedOutput.contains(elem))
                        .as("Unexpected element: " + elem)
                        .isTrue();
            }
        }

        // test for exactly-once
        assertThat(dedupRes).hasSize(Math.abs(initElement - maxElement) + 1);

        latchToWait1.trigger();
        latchToWait2.trigger();

        // wait for everybody ot finish.
        runner1.sync();
        runner2.sync();
    }
}
