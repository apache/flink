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

package org.apache.flink.table.runtime.operators.window.triggers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TestInternalTimerService;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Utility for testing {@link Trigger} behaviour. Partly copied from {@code
 * org.apache.flink.streaming.runtime.operators.windowing.TriggerTestHarness}
 */
public class TriggerTestHarness<W extends Window> {

    private static final Integer KEY = 1;

    private final TypeSerializer<W> windowSerializer;

    private final HeapKeyedStateBackend<Integer> stateBackend;

    private final TestInternalTimerService<Integer, W> internalTimerService;

    private final TestTriggerContext<Integer, W> context;

    private final Trigger<W> trigger;

    public TriggerTestHarness(Trigger<W> trigger) throws Exception {
        this(trigger, (TypeSerializer<W>) new TimeWindow.Serializer());
    }

    public TriggerTestHarness(Trigger<W> trigger, TypeSerializer<W> windowSerializer)
            throws Exception {
        this.windowSerializer = windowSerializer;
        this.trigger = trigger;

        // we only ever use one key, other tests make sure that windows work across different keys
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        HashMapStateBackend backend = new HashMapStateBackend();

        @SuppressWarnings("unchecked")
        HeapKeyedStateBackend<Integer> stateBackend =
                (HeapKeyedStateBackend<Integer>)
                        backend.createKeyedStateBackend(
                                dummyEnv,
                                new JobID(),
                                "test_op",
                                IntSerializer.INSTANCE,
                                1,
                                new KeyGroupRange(0, 0),
                                new KvStateRegistry()
                                        .createTaskRegistry(new JobID(), new JobVertexID()),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());
        this.stateBackend = stateBackend;

        this.stateBackend.setCurrentKey(KEY);

        this.internalTimerService =
                new TestInternalTimerService<>(
                        new KeyContext() {
                            @Override
                            public void setCurrentKey(Object key) {
                                // ignore
                            }

                            @Override
                            public Object getCurrentKey() {
                                return KEY;
                            }
                        });
        this.context =
                new TestTriggerContext<>(internalTimerService, stateBackend, windowSerializer);
        context.key = KEY;
    }

    public void setWindow(W window) {
        context.window = window;
    }

    public int numProcessingTimeTimers() {
        return internalTimerService.numProcessingTimeTimers();
    }

    public int numProcessingTimeTimers(W window) {
        return internalTimerService.numProcessingTimeTimers(window);
    }

    public int numEventTimeTimers() {
        return internalTimerService.numEventTimeTimers();
    }

    public int numEventTimeTimers(W window) {
        return internalTimerService.numEventTimeTimers(window);
    }

    public int numStateEntries() {
        return stateBackend.numKeyValueStateEntries();
    }

    public int numStateEntries(W window) {
        return stateBackend.numKeyValueStateEntries(window);
    }

    public Collection<Tuple2<W, Boolean>> advanceProcessingTime(long time) throws Exception {
        Collection<TestInternalTimerService.Timer<Integer, W>> firedTimers =
                internalTimerService.advanceProcessingTime(time);

        Collection<Tuple2<W, Boolean>> result = new ArrayList<>();

        for (TestInternalTimerService.Timer<Integer, W> timer : firedTimers) {
            context.window = timer.getNamespace();
            context.key = timer.getKey();
            boolean triggered =
                    trigger.onProcessingTime(timer.getTimestamp(), timer.getNamespace());

            result.add(Tuple2.of(timer.getNamespace(), triggered));
        }

        return result;
    }

    public Collection<Tuple2<W, Boolean>> advanceWaterMark(long time) throws Exception {
        Collection<TestInternalTimerService.Timer<Integer, W>> firedTimers =
                internalTimerService.advanceWatermark(time);

        Collection<Tuple2<W, Boolean>> result = new ArrayList<>();

        for (TestInternalTimerService.Timer<Integer, W> timer : firedTimers) {
            context.window = timer.getNamespace();
            context.key = timer.getKey();
            boolean triggered = trigger.onEventTime(timer.getTimestamp(), timer.getNamespace());

            result.add(Tuple2.of(timer.getNamespace(), triggered));
        }

        return result;
    }

    public TestTriggerContext<Integer, W> getTriggerContext() {
        return context;
    }

    private static class TestTriggerContext<K, W extends Window> implements Trigger.TriggerContext {

        protected final InternalTimerService<W> timerService;
        protected final KeyedStateBackend<Integer> stateBackend;
        protected K key;
        protected W window;
        protected final TypeSerializer<W> windowSerializer;

        TestTriggerContext(
                InternalTimerService<W> timerService,
                KeyedStateBackend<Integer> stateBackend,
                TypeSerializer<W> windowSerializer) {
            this.timerService = timerService;
            this.stateBackend = stateBackend;
            this.windowSerializer = windowSerializer;
        }

        @Override
        public long getCurrentProcessingTime() {
            return timerService.currentProcessingTime();
        }

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public long getCurrentWatermark() {
            return timerService.currentWatermark();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            timerService.registerProcessingTimeTimer(window, time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            timerService.registerEventTimeTimer(window, time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            timerService.deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            timerService.deleteEventTimeTimer(window, time);
        }

        @Override
        public ZoneId getShiftTimeZone() {
            return null;
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            try {
                return stateBackend.getPartitionedState(window, windowSerializer, stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Error getting state", e);
            }
        }
    }
}
