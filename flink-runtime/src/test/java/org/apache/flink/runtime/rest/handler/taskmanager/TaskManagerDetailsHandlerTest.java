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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerFileMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMetricsInfo;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests the {@link TaskManagerDetailsHandler} implementation. */
public class TaskManagerDetailsHandlerTest extends TestLogger {

    private static final ResourceID TASK_MANAGER_ID = ResourceID.generate();

    private TestingResourceManagerGateway resourceManagerGateway;
    private MetricFetcher metricFetcher;

    private TaskManagerDetailsHandler testInstance;

    @Before
    public void setup() throws HandlerRequestException {
        resourceManagerGateway = new TestingResourceManagerGateway();
        metricFetcher = new TestingMetricFetcher();

        testInstance =
                new TaskManagerDetailsHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT(),
                        Collections.emptyMap(),
                        TaskManagerDetailsHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway),
                        metricFetcher);
    }

    @Test
    public void testTaskManagerMetricsInfoExtraction()
            throws RestHandlerException, ExecutionException, InterruptedException,
                    JsonProcessingException, HandlerRequestException {
        initializeMetricStore(metricFetcher.getMetricStore());
        resourceManagerGateway.setRequestTaskManagerInfoFunction(
                taskManagerId -> CompletableFuture.completedFuture(createEmptyTaskManagerInfo()));

        HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> request = createRequest();
        TaskManagerDetailsInfo taskManagerDetailsInfo =
                testInstance.handleRequest(request, resourceManagerGateway).get();

        TaskManagerMetricsInfo actual = taskManagerDetailsInfo.getTaskManagerMetricsInfo();
        TaskManagerMetricsInfo expected =
                new TaskManagerMetricsInfo(
                        1L,
                        2L,
                        3L,
                        4L,
                        5L,
                        6L,
                        7L,
                        8L,
                        9L,
                        10L,
                        11L,
                        12L,
                        15L,
                        16L,
                        17L,
                        18L,
                        19L,
                        20L,
                        Collections.emptyList());

        ObjectMapper objectMapper = new ObjectMapper();
        String actualJson = objectMapper.writeValueAsString(actual);
        String expectedJson = objectMapper.writeValueAsString(expected);

        assertThat(actualJson, is(expectedJson));
    }

    private static void initializeMetricStore(MetricStore metricStore) {
        QueryScopeInfo.TaskManagerQueryScopeInfo tmScope =
                new QueryScopeInfo.TaskManagerQueryScopeInfo(TASK_MANAGER_ID.toString(), "Status");

        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Heap.Used", 1));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Heap.Committed", 2));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Heap.Max", 3));

        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.NonHeap.Used", 4));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.NonHeap.Committed", 5));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.NonHeap.Max", 6));

        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Direct.Count", 7));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Direct.MemoryUsed", 8));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Direct.TotalCapacity", 9));

        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Mapped.Count", 10));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Mapped.MemoryUsed", 11));
        metricStore.add(new MetricDump.CounterDump(tmScope, "JVM.Memory.Mapped.TotalCapacity", 12));

        metricStore.add(new MetricDump.CounterDump(tmScope, "Network.AvailableMemorySegments", 13));
        metricStore.add(new MetricDump.CounterDump(tmScope, "Network.TotalMemorySegments", 14));

        metricStore.add(
                new MetricDump.CounterDump(tmScope, "Shuffle.Netty.AvailableMemorySegments", 15));
        metricStore.add(
                new MetricDump.CounterDump(tmScope, "Shuffle.Netty.UsedMemorySegments", 16));
        metricStore.add(
                new MetricDump.CounterDump(tmScope, "Shuffle.Netty.TotalMemorySegments", 17));

        metricStore.add(new MetricDump.CounterDump(tmScope, "Shuffle.Netty.AvailableMemory", 18));
        metricStore.add(new MetricDump.CounterDump(tmScope, "Shuffle.Netty.UsedMemory", 19));
        metricStore.add(new MetricDump.CounterDump(tmScope, "Shuffle.Netty.TotalMemory", 20));
    }

    private static TaskManagerInfo createEmptyTaskManagerInfo() {
        return new TaskManagerInfo(
                TASK_MANAGER_ID,
                UUID.randomUUID().toString(),
                0,
                0,
                0L,
                0,
                0,
                ResourceProfile.ZERO,
                ResourceProfile.ZERO,
                new HardwareDescription(0, 0L, 0L, 0L),
                new TaskExecutorMemoryConfiguration(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    private static HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> createRequest()
            throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(TaskManagerIdPathParameter.KEY, TASK_MANAGER_ID.toString());

        return new HandlerRequest<>(
                EmptyRequestBody.getInstance(),
                new TaskManagerFileMessageParameters(),
                pathParameters,
                Collections.emptyMap());
    }

    private static class TestingMetricFetcher implements MetricFetcher {

        private final MetricStore metricStore = new MetricStore();

        @Override
        public MetricStore getMetricStore() {
            return metricStore;
        }

        @Override
        public void update() {
            // nothing to do
        }
    }
}
