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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesBuilder;
import org.apache.flink.runtime.taskexecutor.slot.TestingTaskSlotTable;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.metrics.util.MetricUtils.METRIC_GROUP_FLINK;
import static org.apache.flink.runtime.metrics.util.MetricUtils.METRIC_GROUP_MANAGED_MEMORY;
import static org.apache.flink.runtime.metrics.util.MetricUtils.METRIC_GROUP_MEMORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link MetricUtils} class. */
class MetricUtilsTest {

    /** Container for local objects to keep them from gc runs. */
    private final List<Object> referencedObjects = new ArrayList<>();

    @AfterEach
    void cleanupReferencedObjects() {
        referencedObjects.clear();
    }

    /**
     * Tests that the {@link MetricUtils#startRemoteMetricsRpcService(Configuration, String, String,
     * RpcSystem)} respects the given {@link MetricOptions#QUERY_SERVICE_THREAD_PRIORITY}.
     */
    @Test
    void testStartMetricActorSystemRespectsThreadPriority() throws Exception {
        final Configuration configuration = new Configuration();
        final int expectedThreadPriority = 3;
        configuration.setInteger(
                MetricOptions.QUERY_SERVICE_THREAD_PRIORITY, expectedThreadPriority);

        final RpcService rpcService =
                MetricUtils.startRemoteMetricsRpcService(
                        configuration, "localhost", null, RpcSystem.load());

        try {
            final int threadPriority =
                    rpcService
                            .getScheduledExecutor()
                            .schedule(
                                    () -> Thread.currentThread().getPriority(), 0, TimeUnit.SECONDS)
                            .get();
            assertThat(threadPriority).isEqualTo(expectedThreadPriority);
        } finally {
            rpcService.closeAsync().get();
        }
    }

    @Test
    void testNonHeapMetricsCompleteness() {
        final InterceptingOperatorMetricGroup nonHeapMetrics =
                new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateNonHeapMemoryMetrics(nonHeapMetrics);

        assertThat(nonHeapMetrics.get(MetricNames.MEMORY_USED)).isNotNull();
        assertThat(nonHeapMetrics.get(MetricNames.MEMORY_COMMITTED)).isNotNull();
        assertThat(nonHeapMetrics.get(MetricNames.MEMORY_MAX)).isNotNull();
    }

    @Test
    void testMetaspaceCompleteness() {
        assertThat(hasMetaspaceMemoryPool())
                .withFailMessage("Requires JVM with Metaspace memory pool")
                .isTrue();

        final InterceptingOperatorMetricGroup metaspaceMetrics =
                new InterceptingOperatorMetricGroup() {
                    @Override
                    public MetricGroup addGroup(String name) {
                        return this;
                    }
                };

        MetricUtils.instantiateMetaspaceMemoryMetrics(metaspaceMetrics);

        assertThat(metaspaceMetrics.get(MetricNames.MEMORY_USED)).isNotNull();
        assertThat(metaspaceMetrics.get(MetricNames.MEMORY_COMMITTED)).isNotNull();
        assertThat(metaspaceMetrics.get(MetricNames.MEMORY_MAX)).isNotNull();
    }

    @Test
    void testHeapMetricsCompleteness() {
        final InterceptingOperatorMetricGroup heapMetrics = new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateHeapMemoryMetrics(heapMetrics);

        assertThat(heapMetrics.get(MetricNames.MEMORY_USED)).isNotNull();
        assertThat(heapMetrics.get(MetricNames.MEMORY_COMMITTED)).isNotNull();
        assertThat(heapMetrics.get(MetricNames.MEMORY_MAX)).isNotNull();
    }

    /**
     * Tests that heap/non-heap metrics do not rely on a static MemoryUsage instance.
     *
     * <p>We can only check this easily for the currently used heap memory, so we use it this as a
     * proxy for testing the functionality in general.
     */
    @Test
    void testHeapMetricUsageNotStatic() throws Exception {
        final InterceptingOperatorMetricGroup heapMetrics = new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateHeapMemoryMetrics(heapMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) heapMetrics.get(MetricNames.MEMORY_USED);

        runUntilMetricChanged("Heap", 10, () -> new byte[1024 * 1024 * 8], used);
    }

    @Test
    void testMetaspaceMetricUsageNotStatic() throws Exception {
        assertThat(hasMetaspaceMemoryPool())
                .withFailMessage("Requires JVM with Metaspace memory pool")
                .isTrue();

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

        runUntilMetricChanged("Metaspace", 10, MetricUtilsTest::redefineDummyClass, used);
    }

    @Test
    void testNonHeapMetricUsageNotStatic() throws Exception {
        final InterceptingOperatorMetricGroup nonHeapMetrics =
                new InterceptingOperatorMetricGroup();

        MetricUtils.instantiateNonHeapMemoryMetrics(nonHeapMetrics);

        @SuppressWarnings("unchecked")
        final Gauge<Long> used = (Gauge<Long>) nonHeapMetrics.get(MetricNames.MEMORY_USED);

        runUntilMetricChanged("Non-heap", 10, MetricUtilsTest::redefineDummyClass, used);
    }

    @Test
    void testManagedMemoryMetricsInitialization() throws MemoryAllocationException, FlinkException {
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
        try {

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

            assertThat(usedMetric.getValue().intValue())
                    .isEqualTo(numberOfAllocatedPages * pageSize);
            assertThat(maxMetric.getValue().intValue()).isEqualTo(maxMemorySize);

            assertThat(actualSubGroupPath)
                    .containsAnyElementsOf(
                            Arrays.asList(
                                    METRIC_GROUP_FLINK,
                                    METRIC_GROUP_MEMORY,
                                    METRIC_GROUP_MANAGED_MEMORY));
        } finally {
            taskManagerServices.shutDown();
        }
    }

    // --------------- utility methods and classes ---------------

    /**
     * An extreme simple class with no dependencies outside "java" package to ease re-defining from
     * its bytecode.
     */
    private static class Dummy {}

    /**
     * Define an new class using {@link Dummy} class's name and bytecode to consume Metaspace and
     * NonHeap memory.
     */
    private static Class<?> redefineDummyClass() throws ClassNotFoundException {
        Class<?> clazz = Dummy.class;
        ChildFirstClassLoader classLoader =
                new ChildFirstClassLoader(
                        ClassLoaderUtils.getClasspathURLs(),
                        clazz.getClassLoader(),
                        new String[] {"java."},
                        ignored -> {});

        Class<?> newClass = classLoader.loadClass(clazz.getName());

        assertThat(newClass).isNotSameAs(clazz);
        assertThat(newClass.getName()).isEqualTo(clazz.getName());
        return newClass;
    }

    private static boolean hasMetaspaceMemoryPool() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                .anyMatch(bean -> "Metaspace".equals(bean.getName()));
    }

    /** Caller may choose to run multiple times for possible interference with other tests. */
    private void runUntilMetricChanged(
            String name, int maxRuns, CheckedSupplier<Object> objectCreator, Gauge<Long> metric)
            throws Exception {
        maxRuns = Math.max(1, maxRuns);
        long initialValue = metric.getValue();
        for (int i = 0; i < maxRuns; i++) {
            Object object = objectCreator.get();
            long currentValue = metric.getValue();
            if (currentValue != initialValue) {
                return;
            }
            referencedObjects.add(object);
            Thread.sleep(50);
        }
        String msg =
                String.format(
                        "%s usage metric never changed its value after %d runs.", name, maxRuns);
        fail(msg);
    }
}
