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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesBuilder;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import akka.actor.ActorSystem;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.metrics.util.MetricUtils.METRIC_GROUP_FLINK;
import static org.apache.flink.runtime.metrics.util.MetricUtils.METRIC_GROUP_MANAGED_MEMORY;
import static org.apache.flink.runtime.metrics.util.MetricUtils.METRIC_GROUP_MEMORY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link MetricUtils} class. */
public class MetricUtilsTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(MetricUtilsTest.class);

    /**
     * Tests that the {@link MetricUtils#startRemoteMetricsRpcService(Configuration, String)}
     * respects the given {@link MetricOptions#QUERY_SERVICE_THREAD_PRIORITY}.
     */
    @Test
    public void testStartMetricActorSystemRespectsThreadPriority() throws Exception {
        final Configuration configuration = new Configuration();
        final int expectedThreadPriority = 3;
        configuration.setInteger(
                MetricOptions.QUERY_SERVICE_THREAD_PRIORITY, expectedThreadPriority);

        final RpcService rpcService =
                MetricUtils.startRemoteMetricsRpcService(configuration, "localhost");
        assertThat(rpcService, instanceOf(AkkaRpcService.class));

        final ActorSystem actorSystem = ((AkkaRpcService) rpcService).getActorSystem();

        try {
            final int threadPriority =
                    actorSystem
                            .settings()
                            .config()
                            .getInt("akka.actor.default-dispatcher.thread-priority");

            assertThat(threadPriority, is(expectedThreadPriority));
        } finally {
            AkkaUtils.terminateActorSystem(actorSystem).get();
        }
    }

    @Test
    public void testNonHeapMetricsCompleteness() {
        final InterceptingOperatorMetricGroup nonHeapMetrics =
                new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateNonHeapMemoryMetrics(nonHeapMetrics);

        Assert.assertNotNull(nonHeapMetrics.get(MetricNames.MEMORY_USED));
        Assert.assertNotNull(nonHeapMetrics.get(MetricNames.MEMORY_COMMITTED));
        Assert.assertNotNull(nonHeapMetrics.get(MetricNames.MEMORY_MAX));
    }

    @Test
    public void testMetaspaceCompleteness() {
        final InterceptingOperatorMetricGroup metaspaceMetrics =
                new InterceptingOperatorMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return this;
                    }
                };

        MetricUtils.instantiateMetaspaceMemoryMetrics(metaspaceMetrics);

        Assert.assertNotNull(metaspaceMetrics.get(MetricNames.MEMORY_USED));
        Assert.assertNotNull(metaspaceMetrics.get(MetricNames.MEMORY_COMMITTED));
        Assert.assertNotNull(metaspaceMetrics.get(MetricNames.MEMORY_MAX));
    }

    @Test
    public void testHeapMetricsCompleteness() {
        final InterceptingOperatorMetricGroup heapMetrics = new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateHeapMemoryMetrics(heapMetrics);

        Assert.assertNotNull(heapMetrics.get(MetricNames.MEMORY_USED));
        Assert.assertNotNull(heapMetrics.get(MetricNames.MEMORY_COMMITTED));
        Assert.assertNotNull(heapMetrics.get(MetricNames.MEMORY_MAX));
    }

    /**
     * Tests that heap/non-heap metrics do not rely on a static MemoryUsage instance.
     *
     * <p>We can only check this easily for the currently used heap memory, so we use it this as a
     * proxy for testing the functionality in general.
     */
    @Test
    public void testHeapMetricUsageNotStatic() throws Exception {
        final InterceptingOperatorMetricGroup heapMetrics = new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateHeapMemoryMetrics(heapMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) heapMetrics.get(MetricNames.MEMORY_USED);

        final long usedHeapInitially = used.getValue();

        // check memory usage difference multiple times since other tests may affect memory usage as
        // well
        for (int x = 0; x < 10; x++) {
            final byte[] array = new byte[1024 * 1024 * 8];
            final long usedHeapAfterAllocation = used.getValue();

            if (usedHeapInitially != usedHeapAfterAllocation) {
                return;
            }
            Thread.sleep(50);
        }
        Assert.fail("Heap usage metric never changed it's value.");
    }

    @Test
    public void testMetaspaceMetricUsageNotStatic() throws InterruptedException {
        final InterceptingOperatorMetricGroup metaspaceMetrics =
                new InterceptingOperatorMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return this;
                    }
                };

        MetricUtils.instantiateMetaspaceMemoryMetrics(metaspaceMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) metaspaceMetrics.get(MetricNames.MEMORY_USED);

        final long usedMetaspaceInitially = used.getValue();

        // check memory usage difference multiple times since other tests may affect memory usage as
        // well
        for (int x = 0; x < 10; x++) {
            List<Runnable> consumerList = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                consumerList.add(() -> {});
            }

            final long usedMetaspaceAfterAllocation = used.getValue();

            if (usedMetaspaceInitially != usedMetaspaceAfterAllocation) {
                return;
            }
            Thread.sleep(50);
        }
        Assert.fail("Metaspace usage metric never changed it's value.");
    }

    @Test
    public void testManagedMemoryMetricsInitialization() throws MemoryAllocationException {
        final int maxMemorySize = 16284;
        final int numberOfAllocatedPages = 2;
        final int pageSize = 4096;
        final Object owner = new Object();

        final MemoryManager memoryManager = MemoryManager.create(maxMemorySize, pageSize);
        memoryManager.allocatePages(owner, numberOfAllocatedPages);
        final TaskManagerServices taskManagerServices =
                new TaskManagerServicesBuilder()
                        .setTaskSlotTable(
                                new TestingTaskSlotTable.TestingTaskSlotTableBuilder<Task>()
                                        .memoryManagerGetterReturns(memoryManager)
                                        .allActiveSlotAllocationIds(
                                                () -> Sets.newHashSet(new AllocationID()))
                                        .build())
                        .setManagedMemorySize(maxMemorySize)
                        .build();

        List<String> actualSubGroupPath = new ArrayList<>();
        final InterceptingOperatorMetricGroup metricGroup =
                new InterceptingOperatorMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        actualSubGroupPath.add(name);
                        return this;
                    }
                };
        MetricUtils.instantiateFlinkMemoryMetricGroup(
                metricGroup,
                taskManagerServices.getTaskSlotTable(),
                taskManagerServices::getManagedMemorySize);

        Gauge<Number> usedMetric = (Gauge<Number>) metricGroup.get("Used");
        Gauge<Number> maxMetric = (Gauge<Number>) metricGroup.get("Total");

        assertThat(usedMetric.getValue().intValue(), is(numberOfAllocatedPages * pageSize));
        assertThat(maxMetric.getValue().intValue(), is(maxMemorySize));

        assertThat(
                actualSubGroupPath,
                is(
                        Arrays.asList(
                                METRIC_GROUP_FLINK,
                                METRIC_GROUP_MEMORY,
                                METRIC_GROUP_MANAGED_MEMORY)));
    }
}
