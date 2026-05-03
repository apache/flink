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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails.CurrentAttempts;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNQueryParameter;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TopNMetricsHandler}.
 *
 * <p>These tests exercise the three aggregation dimensions separately: TaskManager-scoped CPU load,
 * subtask-scoped back-pressure ratio, and TaskManager-scoped GC time summed across all reported
 * garbage-collector names. They also verify empty-store fall-through and the topN cut-off.
 */
class TopNMetricsHandlerTest {

    private static final String JOB_ID = new JobID().toString();
    private static final String VERTEX_ID = "vertexA";

    /** CPU load metric name as emitted by the Status.JVM.CPU metric group. */
    private static final String CPU_LOAD_METRIC = "Status.JVM.CPU.Load";

    private static final String GC_TIME_PREFIX = "Status.JVM.GarbageCollector.";
    private static final String GC_TIME_SUFFIX = ".Time";

    private MetricStore metricStore;
    private TopNMetricsHandler handler;

    @BeforeEach
    void setUp() {
        metricStore = new MetricStore();

        final MetricFetcher fetcher =
                new MetricFetcher() {
                    @Override
                    public MetricStore getMetricStore() {
                        return metricStore;
                    }

                    @Override
                    public void update() {}

                    @Override
                    public long getLastUpdateTime() {
                        return 0;
                    }
                };

        final DispatcherGateway gateway = new TestingDispatcherGateway();
        final GatewayRetriever<DispatcherGateway> retriever =
                () -> CompletableFuture.completedFuture(gateway);

        handler =
                new TopNMetricsHandler(
                        retriever, Duration.ofMillis(50), Collections.emptyMap(), fetcher);
    }

    // ---------------------------------------------------------------------------------------------
    // CPU dimension
    // ---------------------------------------------------------------------------------------------

    @Test
    void testTopCpuConsumersIsTaskManagerScopedAndSortedDescending() throws Exception {
        addTaskManagerCpuLoad("tm-1", 0.10);
        addTaskManagerCpuLoad("tm-2", 0.80);
        addTaskManagerCpuLoad("tm-3", 0.45);

        final TopNMetricsResponseBody body = invokeHandler();
        final List<TopNMetricsResponseBody.CpuConsumerInfo> cpu = body.getTopCpuConsumers();

        assertThat(cpu).hasSize(3);
        assertThat(cpu.get(0).getTaskManagerId()).isEqualTo("tm-2");
        assertThat(cpu.get(0).getCpuPercentage()).isEqualTo(80.0);
        assertThat(cpu.get(1).getTaskManagerId()).isEqualTo("tm-3");
        assertThat(cpu.get(2).getTaskManagerId()).isEqualTo("tm-1");

        // CPU is TaskManager-scoped: subtask index is not applicable (-1) and operatorName is
        // the literal "TaskManager" so the frontend can render it uniformly.
        assertThat(cpu)
                .allSatisfy(
                        info -> {
                            assertThat(info.getSubtaskId()).isEqualTo(-1);
                            assertThat(info.getOperatorName()).isEqualTo("TaskManager");
                        });
    }

    @Test
    void testTopCpuConsumersSkipsTaskManagersWithoutMetric() throws Exception {
        addTaskManagerCpuLoad("tm-with-cpu", 0.42);
        // tm-without-cpu reports only a GC metric, no CPU load
        addTaskManagerGcTime("tm-without-cpu", "G1-Young", 100.0);

        final TopNMetricsResponseBody body = invokeHandler();

        assertThat(body.getTopCpuConsumers())
                .singleElement()
                .satisfies(info -> assertThat(info.getTaskManagerId()).isEqualTo("tm-with-cpu"));
    }

    // ---------------------------------------------------------------------------------------------
    // Backpressure dimension
    // ---------------------------------------------------------------------------------------------

    @Test
    void testTopBackpressureOperatorsAreSubtaskScopedAndClampedToUnitRatio() throws Exception {
        registerVertex(JOB_ID, VERTEX_ID, /* subtaskCount */ 3);
        // 0 ms/s ->  0.0, 500 ms/s -> 0.5, 1000 ms/s -> 1.0
        addSubtaskBackpressure(JOB_ID, VERTEX_ID, 0, 0.0);
        addSubtaskBackpressure(JOB_ID, VERTEX_ID, 1, 500.0);
        addSubtaskBackpressure(JOB_ID, VERTEX_ID, 2, 1000.0);

        final TopNMetricsResponseBody body = invokeHandler();
        final List<TopNMetricsResponseBody.BackpressureOperatorInfo> bp =
                body.getTopBackpressureOperators();

        assertThat(bp).hasSize(3);
        assertThat(bp.get(0).getSubtaskId()).isEqualTo(2);
        assertThat(bp.get(0).getBackpressureRatio()).isEqualTo(1.0);
        assertThat(bp.get(1).getSubtaskId()).isEqualTo(1);
        assertThat(bp.get(1).getBackpressureRatio()).isEqualTo(0.5);
        assertThat(bp.get(2).getSubtaskId()).isEqualTo(0);
        assertThat(bp.get(2).getBackpressureRatio()).isEqualTo(0.0);
    }

    @Test
    void testTopBackpressureOperatorsReturnsEmptyWhenJobHasNoMetricsYet() throws Exception {
        // No vertex/subtask data added for JOB_ID at all.
        final TopNMetricsResponseBody body = invokeHandler();
        assertThat(body.getTopBackpressureOperators()).isEmpty();
    }

    // ---------------------------------------------------------------------------------------------
    // GC dimension
    // ---------------------------------------------------------------------------------------------

    @Test
    void testTopGcIntensiveTaskManagersSumsAcrossAllGarbageCollectors() throws Exception {
        addTaskManagerGcTime("tm-quiet", "G1-Young", 50.0);
        addTaskManagerGcTime("tm-quiet", "G1-Old", 10.0); // total 60

        addTaskManagerGcTime("tm-busy", "G1-Young", 400.0);
        addTaskManagerGcTime("tm-busy", "G1-Old", 200.0); // total 600

        final TopNMetricsResponseBody body = invokeHandler();
        final List<TopNMetricsResponseBody.GcTaskInfo> gc = body.getTopGcIntensiveTasks();

        assertThat(gc).hasSize(2);
        assertThat(gc.get(0).getTaskManagerId()).isEqualTo("tm-busy");
        assertThat(gc.get(0).getGcTimePercentage()).isEqualTo(600.0);
        assertThat(gc.get(1).getTaskManagerId()).isEqualTo("tm-quiet");
        assertThat(gc.get(1).getGcTimePercentage()).isEqualTo(60.0);
    }

    @Test
    void testTopGcIntensiveTaskManagersSkipsTaskManagersWithoutGcMetrics() throws Exception {
        addTaskManagerCpuLoad("tm-cpu-only", 0.5); // no GC metric
        addTaskManagerGcTime("tm-with-gc", "G1-Young", 42.0);

        final TopNMetricsResponseBody body = invokeHandler();

        assertThat(body.getTopGcIntensiveTasks())
                .singleElement()
                .satisfies(info -> assertThat(info.getTaskManagerId()).isEqualTo("tm-with-gc"));
    }

    // ---------------------------------------------------------------------------------------------
    // Top-N cut-off and fault tolerance
    // ---------------------------------------------------------------------------------------------

    @Test
    void testResultsAreCappedAtDefaultTopN() throws Exception {
        // Seed 7 TaskManagers with distinct CPU loads; expect at most 5 in the response.
        for (int i = 0; i < 7; i++) {
            addTaskManagerCpuLoad("tm-" + i, 0.1 * i);
        }
        final TopNMetricsResponseBody body = invokeHandler();
        assertThat(body.getTopCpuConsumers()).hasSize(5);
    }

    @Test
    void testHandlerIsResilientToCompletelyEmptyMetricStore() throws Exception {
        final TopNMetricsResponseBody body = invokeHandler();
        assertThat(body.getTopCpuConsumers()).isEmpty();
        assertThat(body.getTopBackpressureOperators()).isEmpty();
        assertThat(body.getTopGcIntensiveTasks()).isEmpty();
    }

    @Test
    void testCustomTopNQueryParameterIsHonored() throws Exception {
        for (int i = 0; i < 10; i++) {
            addTaskManagerCpuLoad("tm-" + i, 0.01 * i);
        }
        final TopNMetricsResponseBody body = invokeHandlerWithTopN(3);
        assertThat(body.getTopCpuConsumers()).hasSize(3);
        // Largest three loads: tm-9 (0.09) > tm-8 (0.08) > tm-7 (0.07)
        assertThat(body.getTopCpuConsumers().get(0).getTaskManagerId()).isEqualTo("tm-9");
        assertThat(body.getTopCpuConsumers().get(2).getTaskManagerId()).isEqualTo("tm-7");
    }

    @Test
    void testDefaultTopNAppliesWhenQueryParameterIsAbsent() throws Exception {
        for (int i = 0; i < 12; i++) {
            addTaskManagerCpuLoad("tm-" + i, 0.01 * i);
        }
        // Default topN is 5.
        assertThat(invokeHandler().getTopCpuConsumers()).hasSize(5);
    }

    // ---------------------------------------------------------------------------------------------
    // Test helpers
    // ---------------------------------------------------------------------------------------------

    private TopNMetricsResponseBody invokeHandler() throws Exception {
        return invokeHandler(Collections.emptyMap());
    }

    private TopNMetricsResponseBody invokeHandlerWithTopN(int topN) throws Exception {
        return invokeHandler(
                Collections.singletonMap(
                        TopNQueryParameter.KEY, Collections.singletonList(Integer.toString(topN))));
    }

    private TopNMetricsResponseBody invokeHandler(Map<String, List<String>> queryParameters)
            throws Exception {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, JOB_ID);

        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        new TopNMetricsMessageParameters(),
                        pathParameters,
                        queryParameters,
                        Collections.emptyList());

        return handler.handleRequest(request, new TestingDispatcherGateway()).get();
    }

    private void addTaskManagerCpuLoad(String tmId, double load) {
        // CPU_LOAD_METRIC already contains the Status.JVM.CPU scope verbatim, so we register it
        // under the empty scope to have MetricStore key it exactly as "Status.JVM.CPU.Load".
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskManagerQueryScopeInfo(tmId, ""),
                        CPU_LOAD_METRIC,
                        Double.toString(load)));
    }

    private void addTaskManagerGcTime(String tmId, String gcName, double millis) {
        final String metricName = GC_TIME_PREFIX + gcName + GC_TIME_SUFFIX;
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskManagerQueryScopeInfo(tmId, ""),
                        metricName,
                        Double.toString(millis)));
    }

    private void addSubtaskBackpressure(
            String jobId, String vertexId, int subtaskIndex, double bpMsPerSec) {
        metricStore.add(
                new MetricDump.GaugeDump(
                        new QueryScopeInfo.TaskQueryScopeInfo(
                                jobId, vertexId, subtaskIndex, /* attemptNumber */ 0, ""),
                        MetricNames.TASK_BACK_PRESSURED_TIME,
                        Double.toString(bpMsPerSec)));
    }

    /**
     * Register a vertex with subtaskCount representative attempts for the given job. Necessary
     * because TopNMetricsHandler iterates over MetricStore#getRepresentativeAttempts() to discover
     * which vertices belong to the job; merely adding a TaskQueryScopeInfo dump is not enough.
     */
    private void registerVertex(String jobId, String vertexId, int subtaskCount) {
        final Map<Integer, CurrentAttempts> subtaskAttempts = new HashMap<>();
        for (int i = 0; i < subtaskCount; i++) {
            final Set<Integer> currentAttempts = new HashSet<>();
            currentAttempts.add(0);
            subtaskAttempts.put(i, new CurrentAttempts(0, currentAttempts, false));
        }
        final Map<String, Map<Integer, CurrentAttempts>> vertices = new HashMap<>();
        vertices.put(vertexId, subtaskAttempts);

        final JobDetails job =
                new JobDetails(
                        JobID.fromHexString(jobId),
                        "testJob",
                        0L,
                        0L,
                        0L,
                        JobStatus.RUNNING,
                        0L,
                        new int[10],
                        1,
                        vertices);

        metricStore.updateCurrentExecutionAttempts(Collections.singletonList(job));
    }
}
