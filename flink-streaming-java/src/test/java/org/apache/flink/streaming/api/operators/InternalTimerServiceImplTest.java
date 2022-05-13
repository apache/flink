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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link InternalTimerServiceImpl}. */
@RunWith(Parameterized.class)
public class InternalTimerServiceImplTest {

    private final int maxParallelism;
    private final KeyGroupRange testKeyGroupRange;

    private static InternalTimer<Integer, String> anyInternalTimer() {
        return any();
    }

    public InternalTimerServiceImplTest(int startKeyGroup, int endKeyGroup, int maxParallelism) {
        this.testKeyGroupRange = new KeyGroupRange(startKeyGroup, endKeyGroup);
        this.maxParallelism = maxParallelism;
    }

    @Test
    public void testKeyGroupStartIndexSetting() {

        int startKeyGroupIdx = 7;
        int endKeyGroupIdx = 21;
        KeyGroupRange testKeyGroupList = new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx);

        TestKeyContext keyContext = new TestKeyContext();

        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

        InternalTimerServiceImpl<Integer, String> service =
                createInternalTimerService(
                        testKeyGroupList,
                        keyContext,
                        processingTimeService,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        createQueueFactory());

        Assert.assertEquals(startKeyGroupIdx, service.getLocalKeyGroupRangeStartIdx());
    }

    @Test
    public void testTimerAssignmentToKeyGroups() {
        int totalNoOfTimers = 100;

        int totalNoOfKeyGroups = 100;
        int startKeyGroupIdx = 0;
        int endKeyGroupIdx = totalNoOfKeyGroups - 1; // we have 0 to 99

        @SuppressWarnings("unchecked")
        Set<TimerHeapInternalTimer<Integer, String>>[] expectedNonEmptyTimerSets =
                new HashSet[totalNoOfKeyGroups];
        TestKeyContext keyContext = new TestKeyContext();

        final KeyGroupRange keyGroupRange = new KeyGroupRange(startKeyGroupIdx, endKeyGroupIdx);

        final PriorityQueueSetFactory priorityQueueSetFactory =
                createQueueFactory(keyGroupRange, totalNoOfKeyGroups);

        InternalTimerServiceImpl<Integer, String> timerService =
                createInternalTimerService(
                        keyGroupRange,
                        keyContext,
                        new TestProcessingTimeService(),
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        priorityQueueSetFactory);

        timerService.startTimerService(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE, mock(Triggerable.class));

        for (int i = 0; i < totalNoOfTimers; i++) {

            // create the timer to be registered
            TimerHeapInternalTimer<Integer, String> timer =
                    new TimerHeapInternalTimer<>(10 + i, i, "hello_world_" + i);
            int keyGroupIdx =
                    KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalNoOfKeyGroups);

            // add it in the adequate expected set of timers per keygroup
            Set<TimerHeapInternalTimer<Integer, String>> timerSet =
                    expectedNonEmptyTimerSets[keyGroupIdx];
            if (timerSet == null) {
                timerSet = new HashSet<>();
                expectedNonEmptyTimerSets[keyGroupIdx] = timerSet;
            }
            timerSet.add(timer);

            // register the timer as both processing and event time one
            keyContext.setCurrentKey(timer.getKey());
            timerService.registerEventTimeTimer(timer.getNamespace(), timer.getTimestamp());
            timerService.registerProcessingTimeTimer(timer.getNamespace(), timer.getTimestamp());
        }

        List<Set<TimerHeapInternalTimer<Integer, String>>> eventTimeTimers =
                timerService.getEventTimeTimersPerKeyGroup();
        List<Set<TimerHeapInternalTimer<Integer, String>>> processingTimeTimers =
                timerService.getProcessingTimeTimersPerKeyGroup();

        // finally verify that the actual timers per key group sets are the expected ones.
        for (int i = 0; i < expectedNonEmptyTimerSets.length; i++) {
            Set<TimerHeapInternalTimer<Integer, String>> expected = expectedNonEmptyTimerSets[i];
            Set<TimerHeapInternalTimer<Integer, String>> actualEvent = eventTimeTimers.get(i);
            Set<TimerHeapInternalTimer<Integer, String>> actualProcessing =
                    processingTimeTimers.get(i);

            if (expected == null) {
                Assert.assertTrue(actualEvent.isEmpty());
                Assert.assertTrue(actualProcessing.isEmpty());
            } else {
                Assert.assertEquals(expected, actualEvent);
                Assert.assertEquals(expected, actualProcessing);
            }
        }
    }

    /**
     * Verify that we only ever have one processing-time task registered at the {@link
     * ProcessingTimeService}.
     */
    @Test
    public void testOnlySetsOnePhysicalProcessingTimeTimer() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();

        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        PriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(testKeyGroupRange, maxParallelism, 128);
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        priorityQueueSetFactory);

        int key = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        keyContext.setCurrentKey(key);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("ciao", 20);
        timerService.registerProcessingTimeTimer("ciao", 30);
        timerService.registerProcessingTimeTimer("hello", 10);
        timerService.registerProcessingTimeTimer("hello", 20);

        assertEquals(5, timerService.numProcessingTimeTimers());
        assertEquals(2, timerService.numProcessingTimeTimers("hello"));
        assertEquals(3, timerService.numProcessingTimeTimers("ciao"));

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(10L));

        processingTimeService.setCurrentTime(10);

        assertEquals(3, timerService.numProcessingTimeTimers());
        assertEquals(1, timerService.numProcessingTimeTimers("hello"));
        assertEquals(2, timerService.numProcessingTimeTimers("ciao"));

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(20L));

        processingTimeService.setCurrentTime(20);

        assertEquals(1, timerService.numProcessingTimeTimers());
        assertEquals(0, timerService.numProcessingTimeTimers("hello"));
        assertEquals(1, timerService.numProcessingTimeTimers("ciao"));

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(30L));

        processingTimeService.setCurrentTime(30);

        assertEquals(0, timerService.numProcessingTimeTimers());

        assertEquals(0, processingTimeService.getNumActiveTimers());

        timerService.registerProcessingTimeTimer("ciao", 40);

        assertEquals(1, processingTimeService.getNumActiveTimers());
    }

    /**
     * Verify that registering a processing-time timer that is earlier than the existing timers
     * removes the one physical timer and creates one for the earlier timestamp {@link
     * ProcessingTimeService}.
     */
    @Test
    public void testRegisterEarlierProcessingTimerMovesPhysicalProcessingTimer() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();

        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        int key = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);

        keyContext.setCurrentKey(key);

        timerService.registerProcessingTimeTimer("ciao", 20);

        assertEquals(1, timerService.numProcessingTimeTimers());

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(20L));

        timerService.registerProcessingTimeTimer("ciao", 10);

        assertEquals(2, timerService.numProcessingTimeTimers());

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(10L));
    }

    /** */
    @Test
    public void testRegisteringProcessingTimeTimerInOnProcessingTimeDoesNotLeakPhysicalTimers()
            throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();

        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

        final InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        int key = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);

        keyContext.setCurrentKey(key);

        timerService.registerProcessingTimeTimer("ciao", 10);

        assertEquals(1, timerService.numProcessingTimeTimers());

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(10L));

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                timerService.registerProcessingTimeTimer("ciao", 20);
                                return null;
                            }
                        })
                .when(mockTriggerable)
                .onProcessingTime(anyInternalTimer());

        processingTimeService.setCurrentTime(10);

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(20L));

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                timerService.registerProcessingTimeTimer("ciao", 30);
                                return null;
                            }
                        })
                .when(mockTriggerable)
                .onProcessingTime(anyInternalTimer());

        processingTimeService.setCurrentTime(20);

        assertEquals(1, timerService.numProcessingTimeTimers());

        assertEquals(1, processingTimeService.getNumActiveTimers());
        assertThat(processingTimeService.getActiveTimerTimestamps(), containsInAnyOrder(30L));
    }

    @Test
    public void testCurrentProcessingTime() throws Exception {

        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        processingTimeService.setCurrentTime(17L);
        assertEquals(17, timerService.currentProcessingTime());

        processingTimeService.setCurrentTime(42);
        assertEquals(42, timerService.currentProcessingTime());
    }

    @Test
    public void testCurrentEventTime() throws Exception {

        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        timerService.advanceWatermark(17);
        assertEquals(17, timerService.currentWatermark());

        timerService.advanceWatermark(42);
        assertEquals(42, timerService.currentWatermark());
    }

    /** This also verifies that we don't have leakage between keys/namespaces. */
    @Test
    public void testSetAndFireEventTimeTimers() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        keyContext.setCurrentKey(key1);

        timerService.registerEventTimeTimer("ciao", 10);
        timerService.registerEventTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);

        timerService.registerEventTimeTimer("ciao", 10);
        timerService.registerEventTimeTimer("hello", 10);

        assertEquals(4, timerService.numEventTimeTimers());
        assertEquals(2, timerService.numEventTimeTimers("hello"));
        assertEquals(2, timerService.numEventTimeTimers("ciao"));

        timerService.advanceWatermark(10);

        verify(mockTriggerable, times(4)).onEventTime(anyInternalTimer());
        verify(mockTriggerable, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));
        verify(mockTriggerable, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));

        assertEquals(0, timerService.numEventTimeTimers());
    }

    /** This also verifies that we don't have leakage between keys/namespaces. */
    @Test
    public void testSetAndFireProcessingTimeTimers() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        keyContext.setCurrentKey(key1);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("hello", 10);

        assertEquals(4, timerService.numProcessingTimeTimers());
        assertEquals(2, timerService.numProcessingTimeTimers("hello"));
        assertEquals(2, timerService.numProcessingTimeTimers("ciao"));

        processingTimeService.setCurrentTime(10);

        verify(mockTriggerable, times(4)).onProcessingTime(anyInternalTimer());
        verify(mockTriggerable, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));
        verify(mockTriggerable, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));

        assertEquals(0, timerService.numProcessingTimeTimers());
    }

    /**
     * This also verifies that we don't have leakage between keys/namespaces.
     *
     * <p>This also verifies that deleted timers don't fire.
     */
    @Test
    public void testDeleteEventTimeTimers() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        keyContext.setCurrentKey(key1);

        timerService.registerEventTimeTimer("ciao", 10);
        timerService.registerEventTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);

        timerService.registerEventTimeTimer("ciao", 10);
        timerService.registerEventTimeTimer("hello", 10);

        assertEquals(4, timerService.numEventTimeTimers());
        assertEquals(2, timerService.numEventTimeTimers("hello"));
        assertEquals(2, timerService.numEventTimeTimers("ciao"));

        keyContext.setCurrentKey(key1);
        timerService.deleteEventTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);
        timerService.deleteEventTimeTimer("ciao", 10);

        assertEquals(2, timerService.numEventTimeTimers());
        assertEquals(1, timerService.numEventTimeTimers("hello"));
        assertEquals(1, timerService.numEventTimeTimers("ciao"));

        timerService.advanceWatermark(10);

        verify(mockTriggerable, times(2)).onEventTime(anyInternalTimer());
        verify(mockTriggerable, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable, times(0))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable, times(0))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));
        verify(mockTriggerable, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));

        assertEquals(0, timerService.numEventTimeTimers());
    }

    /**
     * This also verifies that we don't have leakage between keys/namespaces.
     *
     * <p>This also verifies that deleted timers don't fire.
     */
    @Test
    public void testDeleteProcessingTimeTimers() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        keyContext.setCurrentKey(key1);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("hello", 10);

        assertEquals(4, timerService.numProcessingTimeTimers());
        assertEquals(2, timerService.numProcessingTimeTimers("hello"));
        assertEquals(2, timerService.numProcessingTimeTimers("ciao"));

        keyContext.setCurrentKey(key1);
        timerService.deleteProcessingTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);
        timerService.deleteProcessingTimeTimer("ciao", 10);

        assertEquals(2, timerService.numProcessingTimeTimers());
        assertEquals(1, timerService.numProcessingTimeTimers("hello"));
        assertEquals(1, timerService.numProcessingTimeTimers("ciao"));

        processingTimeService.setCurrentTime(10);

        verify(mockTriggerable, times(2)).onProcessingTime(anyInternalTimer());
        verify(mockTriggerable, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable, times(0))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable, times(0))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));
        verify(mockTriggerable, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));

        assertEquals(0, timerService.numEventTimeTimers());
    }

    /**
     * This also verifies that we iterate over all timers and set the key context on each element.
     */
    @Test
    public void testForEachEventTimeTimers() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        Set<Tuple3<Integer, String, Long>> timers = new HashSet<>();
        timers.add(Tuple3.of(key1, "ciao", 10L));
        timers.add(Tuple3.of(key1, "hello", 10L));
        timers.add(Tuple3.of(key2, "ciao", 10L));
        timers.add(Tuple3.of(key2, "hello", 10L));

        for (Tuple3<Integer, String, Long> timer : timers) {
            keyContext.setCurrentKey(timer.f0);
            timerService.registerEventTimeTimer(timer.f1, timer.f2);
        }

        Set<Tuple3<Integer, String, Long>> results = new HashSet<>();
        timerService.forEachEventTimeTimer(
                (namespace, timer) -> {
                    results.add(Tuple3.of((Integer) keyContext.getCurrentKey(), namespace, timer));
                });

        Assert.assertEquals(timers, results);
    }

    /**
     * This also verifies that we iterate over all timers and set the key context on each element.
     */
    @Test
    public void testForEachProcessingTimeTimers() throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        Set<Tuple3<Integer, String, Long>> timers = new HashSet<>();
        timers.add(Tuple3.of(key1, "ciao", 10L));
        timers.add(Tuple3.of(key1, "hello", 10L));
        timers.add(Tuple3.of(key2, "ciao", 10L));
        timers.add(Tuple3.of(key2, "hello", 10L));

        for (Tuple3<Integer, String, Long> timer : timers) {
            keyContext.setCurrentKey(timer.f0);
            timerService.registerProcessingTimeTimer(timer.f1, timer.f2);
        }

        Set<Tuple3<Integer, String, Long>> results = new HashSet<>();
        timerService.forEachProcessingTimeTimer(
                (namespace, timer) -> {
                    results.add(Tuple3.of((Integer) keyContext.getCurrentKey(), namespace, timer));
                });

        Assert.assertEquals(timers, results);
    }

    @Test
    public void testSnapshotAndRestore() throws Exception {
        testSnapshotAndRestore(InternalTimerServiceSerializationProxy.VERSION);
    }

    @Test
    public void testSnapshotAndRestorePreVersioned() throws Exception {
        testSnapshotAndRestore(InternalTimersSnapshotReaderWriters.NO_VERSION);
    }

    /**
     * This test checks whether timers are assigned to correct key groups and whether
     * snapshot/restore respects key groups.
     */
    @Test
    public void testSnapshotAndRebalancingRestore() throws Exception {
        testSnapshotAndRebalancingRestore(InternalTimerServiceSerializationProxy.VERSION);
    }

    @Test
    public void testSnapshotAndRebalancingRestorePreVersioned() throws Exception {
        testSnapshotAndRebalancingRestore(InternalTimersSnapshotReaderWriters.NO_VERSION);
    }

    private void testSnapshotAndRestore(int snapshotVersion) throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        // get two different keys
        int key1 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        int key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        while (key2 == key1) {
            key2 = getKeyInKeyGroupRange(testKeyGroupRange, maxParallelism);
        }

        keyContext.setCurrentKey(key1);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerEventTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);

        timerService.registerEventTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("hello", 10);

        assertEquals(2, timerService.numProcessingTimeTimers());
        assertEquals(1, timerService.numProcessingTimeTimers("hello"));
        assertEquals(1, timerService.numProcessingTimeTimers("ciao"));
        assertEquals(2, timerService.numEventTimeTimers());
        assertEquals(1, timerService.numEventTimeTimers("hello"));
        assertEquals(1, timerService.numEventTimeTimers("ciao"));

        Map<Integer, byte[]> snapshot = new HashMap<>();
        for (Integer keyGroupIndex : testKeyGroupRange) {
            try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
                InternalTimersSnapshot<Integer, String> timersSnapshot =
                        timerService.snapshotTimersForKeyGroup(keyGroupIndex);

                InternalTimersSnapshotReaderWriters.getWriterForVersion(
                                snapshotVersion,
                                timersSnapshot,
                                timerService.getKeySerializer(),
                                timerService.getNamespaceSerializer())
                        .writeTimersSnapshot(new DataOutputViewStreamWrapper(outStream));

                snapshot.put(keyGroupIndex, outStream.toByteArray());
            }
        }

        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable2 = mock(Triggerable.class);

        keyContext = new TestKeyContext();
        processingTimeService = new TestProcessingTimeService();

        timerService =
                restoreTimerService(
                        snapshot,
                        snapshotVersion,
                        mockTriggerable2,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        createQueueFactory());

        processingTimeService.setCurrentTime(10);
        timerService.advanceWatermark(10);

        verify(mockTriggerable2, times(2)).onProcessingTime(anyInternalTimer());
        verify(mockTriggerable2, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable2, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));
        verify(mockTriggerable2, times(2)).onEventTime(anyInternalTimer());
        verify(mockTriggerable2, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable2, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));

        assertEquals(0, timerService.numEventTimeTimers());
    }

    private void testSnapshotAndRebalancingRestore(int snapshotVersion) throws Exception {
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable = mock(Triggerable.class);

        TestKeyContext keyContext = new TestKeyContext();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        final PriorityQueueSetFactory queueFactory = createQueueFactory();
        InternalTimerServiceImpl<Integer, String> timerService =
                createAndStartInternalTimerService(
                        mockTriggerable,
                        keyContext,
                        processingTimeService,
                        testKeyGroupRange,
                        queueFactory);

        int midpoint =
                testKeyGroupRange.getStartKeyGroup()
                        + (testKeyGroupRange.getEndKeyGroup()
                                        - testKeyGroupRange.getStartKeyGroup())
                                / 2;

        // get two sub key-ranges so that we can restore two ranges separately
        KeyGroupRange subKeyGroupRange1 =
                new KeyGroupRange(testKeyGroupRange.getStartKeyGroup(), midpoint);
        KeyGroupRange subKeyGroupRange2 =
                new KeyGroupRange(midpoint + 1, testKeyGroupRange.getEndKeyGroup());

        // get two different keys, one per sub range
        int key1 = getKeyInKeyGroupRange(subKeyGroupRange1, maxParallelism);
        int key2 = getKeyInKeyGroupRange(subKeyGroupRange2, maxParallelism);

        keyContext.setCurrentKey(key1);

        timerService.registerProcessingTimeTimer("ciao", 10);
        timerService.registerEventTimeTimer("hello", 10);

        keyContext.setCurrentKey(key2);

        timerService.registerEventTimeTimer("ciao", 10);
        timerService.registerProcessingTimeTimer("hello", 10);

        assertEquals(2, timerService.numProcessingTimeTimers());
        assertEquals(1, timerService.numProcessingTimeTimers("hello"));
        assertEquals(1, timerService.numProcessingTimeTimers("ciao"));
        assertEquals(2, timerService.numEventTimeTimers());
        assertEquals(1, timerService.numEventTimeTimers("hello"));
        assertEquals(1, timerService.numEventTimeTimers("ciao"));

        // one map per sub key-group range
        Map<Integer, byte[]> snapshot1 = new HashMap<>();
        Map<Integer, byte[]> snapshot2 = new HashMap<>();
        for (Integer keyGroupIndex : testKeyGroupRange) {
            try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()) {
                InternalTimersSnapshot<Integer, String> timersSnapshot =
                        timerService.snapshotTimersForKeyGroup(keyGroupIndex);

                InternalTimersSnapshotReaderWriters.getWriterForVersion(
                                snapshotVersion,
                                timersSnapshot,
                                timerService.getKeySerializer(),
                                timerService.getNamespaceSerializer())
                        .writeTimersSnapshot(new DataOutputViewStreamWrapper(outStream));

                if (subKeyGroupRange1.contains(keyGroupIndex)) {
                    snapshot1.put(keyGroupIndex, outStream.toByteArray());
                } else if (subKeyGroupRange2.contains(keyGroupIndex)) {
                    snapshot2.put(keyGroupIndex, outStream.toByteArray());
                } else {
                    throw new IllegalStateException(
                            "Key-Group index doesn't belong to any sub range.");
                }
            }
        }

        // from now on we need everything twice. once per sub key-group range
        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable1 = mock(Triggerable.class);

        @SuppressWarnings("unchecked")
        Triggerable<Integer, String> mockTriggerable2 = mock(Triggerable.class);

        TestKeyContext keyContext1 = new TestKeyContext();
        TestKeyContext keyContext2 = new TestKeyContext();

        TestProcessingTimeService processingTimeService1 = new TestProcessingTimeService();
        TestProcessingTimeService processingTimeService2 = new TestProcessingTimeService();

        InternalTimerServiceImpl<Integer, String> timerService1 =
                restoreTimerService(
                        snapshot1,
                        snapshotVersion,
                        mockTriggerable1,
                        keyContext1,
                        processingTimeService1,
                        subKeyGroupRange1,
                        queueFactory);

        InternalTimerServiceImpl<Integer, String> timerService2 =
                restoreTimerService(
                        snapshot2,
                        snapshotVersion,
                        mockTriggerable2,
                        keyContext2,
                        processingTimeService2,
                        subKeyGroupRange2,
                        queueFactory);

        processingTimeService1.setCurrentTime(10);
        timerService1.advanceWatermark(10);

        verify(mockTriggerable1, times(1)).onProcessingTime(anyInternalTimer());
        verify(mockTriggerable1, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable1, never())
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));
        verify(mockTriggerable1, times(1)).onEventTime(anyInternalTimer());
        verify(mockTriggerable1, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable1, never())
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));

        assertEquals(0, timerService1.numEventTimeTimers());

        processingTimeService2.setCurrentTime(10);
        timerService2.advanceWatermark(10);

        verify(mockTriggerable2, times(1)).onProcessingTime(anyInternalTimer());
        verify(mockTriggerable2, never())
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key1, "ciao")));
        verify(mockTriggerable2, times(1))
                .onProcessingTime(eq(new TimerHeapInternalTimer<>(10, key2, "hello")));
        verify(mockTriggerable2, times(1)).onEventTime(anyInternalTimer());
        verify(mockTriggerable2, never())
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key1, "hello")));
        verify(mockTriggerable2, times(1))
                .onEventTime(eq(new TimerHeapInternalTimer<>(10, key2, "ciao")));

        assertEquals(0, timerService2.numEventTimeTimers());
    }

    private static class TestKeyContext implements KeyContext {

        private Object key;

        @Override
        public void setCurrentKey(Object key) {
            this.key = key;
        }

        @Override
        public Object getCurrentKey() {
            return key;
        }
    }

    private static int getKeyInKeyGroup(int keyGroup, int maxParallelism) {
        Random rand = new Random(System.currentTimeMillis());
        int result = rand.nextInt();
        while (KeyGroupRangeAssignment.assignToKeyGroup(result, maxParallelism) != keyGroup) {
            result = rand.nextInt();
        }
        return result;
    }

    private static int getKeyInKeyGroupRange(KeyGroupRange range, int maxParallelism) {
        Random rand = new Random(System.currentTimeMillis());
        int result = rand.nextInt();
        while (!range.contains(KeyGroupRangeAssignment.assignToKeyGroup(result, maxParallelism))) {
            result = rand.nextInt();
        }
        return result;
    }

    private static InternalTimerServiceImpl<Integer, String> createAndStartInternalTimerService(
            Triggerable<Integer, String> triggerable,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            KeyGroupRange keyGroupList,
            PriorityQueueSetFactory priorityQueueSetFactory) {
        InternalTimerServiceImpl<Integer, String> service =
                createInternalTimerService(
                        keyGroupList,
                        keyContext,
                        processingTimeService,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        priorityQueueSetFactory);

        service.startTimerService(IntSerializer.INSTANCE, StringSerializer.INSTANCE, triggerable);
        return service;
    }

    private static InternalTimerServiceImpl<Integer, String> restoreTimerService(
            Map<Integer, byte[]> state,
            int snapshotVersion,
            Triggerable<Integer, String> triggerable,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            KeyGroupRange keyGroupsList,
            PriorityQueueSetFactory priorityQueueSetFactory)
            throws Exception {

        // create an empty service
        InternalTimerServiceImpl<Integer, String> service =
                createInternalTimerService(
                        keyGroupsList,
                        keyContext,
                        processingTimeService,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        priorityQueueSetFactory);

        // restore the timers
        for (Integer keyGroupIndex : keyGroupsList) {
            if (state.containsKey(keyGroupIndex)) {
                try (ByteArrayInputStream inputStream =
                        new ByteArrayInputStream(state.get(keyGroupIndex))) {
                    InternalTimersSnapshot<?, ?> restoredTimersSnapshot =
                            InternalTimersSnapshotReaderWriters.getReaderForVersion(
                                            snapshotVersion,
                                            InternalTimerServiceImplTest.class.getClassLoader())
                                    .readTimersSnapshot(
                                            new DataInputViewStreamWrapper(inputStream));

                    service.restoreTimersForKeyGroup(restoredTimersSnapshot, keyGroupIndex);
                }
            }
        }

        // initialize the service
        service.startTimerService(IntSerializer.INSTANCE, StringSerializer.INSTANCE, triggerable);
        return service;
    }

    private PriorityQueueSetFactory createQueueFactory() {
        return createQueueFactory(testKeyGroupRange, maxParallelism);
    }

    protected PriorityQueueSetFactory createQueueFactory(
            KeyGroupRange keyGroupRange, int numKeyGroups) {
        return new HeapPriorityQueueSetFactory(keyGroupRange, numKeyGroups, 128);
    }

    // ------------------------------------------------------------------------
    //  Parametrization for testing with different key-group ranges
    // ------------------------------------------------------------------------

    @Parameterized.Parameters(name = "start = {0}, end = {1}, max = {2}")
    @SuppressWarnings("unchecked,rawtypes")
    public static Collection<Object[]> keyRanges() {
        return Arrays.asList(
                new Object[][] {
                    {0, Short.MAX_VALUE - 1, Short.MAX_VALUE},
                    {0, 10, Short.MAX_VALUE},
                    {0, 10, 10},
                    {10, Short.MAX_VALUE - 1, Short.MAX_VALUE},
                    {2, 5, 100},
                    {2, 5, 6}
                });
    }

    private static <K, N> InternalTimerServiceImpl<K, N> createInternalTimerService(
            KeyGroupRange keyGroupsList,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            PriorityQueueSetFactory priorityQueueSetFactory) {

        TimerSerializer<K, N> timerSerializer =
                new TimerSerializer<>(keySerializer, namespaceSerializer);

        return new InternalTimerServiceImpl<>(
                keyGroupsList,
                keyContext,
                processingTimeService,
                createTimerQueue(
                        "__test_processing_timers", timerSerializer, priorityQueueSetFactory),
                createTimerQueue("__test_event_timers", timerSerializer, priorityQueueSetFactory));
    }

    private static <K, N>
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> createTimerQueue(
                    String name,
                    TimerSerializer<K, N> timerSerializer,
                    PriorityQueueSetFactory priorityQueueSetFactory) {
        return priorityQueueSetFactory.create(name, timerSerializer);
    }
}
