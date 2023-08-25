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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/** Tests for {@link InternalTimeServiceManagerImpl}. */
public class InternalTimeServiceManagerImplTest extends TestLogger {

    /** This test fixes some constants, because changing them can harm backwards compatibility. */
    @Test
    public void fixConstants() {
        String expectedTimerStatePrefix = "_timer_state";
        Assert.assertEquals(
                expectedTimerStatePrefix, InternalTimeServiceManagerImpl.TIMER_STATE_PREFIX);
        Assert.assertEquals(
                expectedTimerStatePrefix + "/processing_",
                InternalTimeServiceManagerImpl.PROCESSING_TIMER_PREFIX);
        Assert.assertEquals(
                expectedTimerStatePrefix + "/event_",
                InternalTimeServiceManagerImpl.EVENT_TIMER_PREFIX);
    }

    /** Tests the registration of the timer metrics. */
    @Test
    @SuppressWarnings("unchecked")
    public void testMetricsRegistration() throws Exception {
        final ArrayList<String> addGroupNames = new ArrayList<>();
        final ArrayList<String> registeredGaugeNames = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        addGroupNames.add(name);
                        return new UnregisteredMetricsGroup() {
                            @Override
                            public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                                if (gauge != null) {
                                    registeredGaugeNames.add(name);
                                }
                                return gauge;
                            }
                        };
                    }
                };

        // Assert timer metric group
        InternalTimeServiceManager<Integer> timeServiceManager =
                createInternalTimeServiceManager(
                        IntSerializer.INSTANCE,
                        1,
                        new KeyGroupRange(0, 0),
                        new TestProcessingTimeService(),
                        metricGroup);
        Assert.assertEquals(1, addGroupNames.size());
        Assert.assertEquals(addGroupNames.get(0), "numberOfTimers");

        // Assert time-service metric
        timeServiceManager.getInternalTimerService(
                "timer-service-1",
                IntSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                mock(Triggerable.class));
        Assert.assertEquals(1, registeredGaugeNames.size());
        Assert.assertEquals(registeredGaugeNames.get(0), "timer-service-1");

        // Assert co-exist time-service metric
        timeServiceManager.getInternalTimerService(
                "timer-service-2",
                IntSerializer.INSTANCE,
                IntSerializer.INSTANCE,
                mock(Triggerable.class));
        Assert.assertEquals(2, registeredGaugeNames.size());
        Assert.assertEquals(registeredGaugeNames.get(0), "timer-service-1");
        Assert.assertEquals(registeredGaugeNames.get(1), "timer-service-2");
    }

    /** Tests that the metrics are updated properly. */
    @Test
    @SuppressWarnings("unchecked")
    public void testMetricsAreUpdated() throws Exception {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();
        String timeServiceName = "time-service";
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return new UnregisteredMetricsGroup() {
                            @Override
                            public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                                registeredGauges.put(name, gauge);
                                return gauge;
                            }
                        };
                    }
                };
        InternalTimeServiceManager<Integer> timeServiceManager =
                createInternalTimeServiceManager(
                        IntSerializer.INSTANCE,
                        1,
                        new KeyGroupRange(0, 0),
                        processingTimeService,
                        metricGroup);
        InternalTimerService<Integer> timeService =
                timeServiceManager.getInternalTimerService(
                        timeServiceName,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        mock(Triggerable.class));

        // Make sure to adjust this test if metrics are added/removed
        assertEquals(1, registeredGauges.size());

        // Check initial values
        Gauge<Integer> timeServiceNumTimer = (Gauge<Integer>) registeredGauges.get(timeServiceName);

        assertEquals(Integer.valueOf(0), timeServiceNumTimer.getValue());

        timeService.registerEventTimeTimer(1, 10);
        timeService.registerProcessingTimeTimer(
                1, processingTimeService.getCurrentProcessingTime() + 1);

        // Check counts
        assertEquals(Integer.valueOf(2), timeServiceNumTimer.getValue());

        // Verify triggered event-time timer is not counted
        timeServiceManager.advanceWatermark(new Watermark(100));
        assertEquals(Integer.valueOf(1), timeServiceNumTimer.getValue());

        // Verify triggered processing-time timer is not counted
        processingTimeService.advance(processingTimeService.getCurrentProcessingTime() + 2);
        assertEquals(Integer.valueOf(0), timeServiceNumTimer.getValue());
    }

    private static <K> InternalTimeServiceManager<K> createInternalTimeServiceManager(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ProcessingTimeService processingTimeService,
            MetricGroup metricGroup)
            throws Exception {
        StateBackend stateBackend = new MemoryStateBackend();
        Environment env = new DummyEnvironment();
        CheckpointableKeyedStateBackend<K> keyedStateBackend =
                stateBackend.createKeyedStateBackend(
                        env,
                        new JobID(),
                        "test_op",
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        new CloseableRegistry());
        KeyContext keyContext = new DummyKeyContext();

        return InternalTimeServiceManagerImpl.create(
                keyedStateBackend,
                ClassLoader.getSystemClassLoader(),
                keyContext,
                processingTimeService,
                Collections.emptyList(),
                mock(StreamTaskCancellationContext.class),
                metricGroup);
    }

    private static class DummyKeyContext implements KeyContext {
        @Override
        public void setCurrentKey(Object key) {}

        @Override
        public Object getCurrentKey() {
            return 0;
        }
    }
}
