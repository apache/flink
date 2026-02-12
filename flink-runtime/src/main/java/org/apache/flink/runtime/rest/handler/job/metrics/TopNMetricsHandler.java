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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.JobVertexHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody.BackpressureOperator;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody.CpuConsumer;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody.GcIntensiveTask;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.metrics.dump.QueryScopeUtils.createScopedMetricName;

/**
 * Handler that returns Top N metrics for a job, including top CPU consumers, top backpressure
 * operators, and top GC intensive tasks.
 */
public class TopNMetricsHandler
        extends JobVertexHandler<EmptyRequestBody, TopNMetricsResponseBody> {

    private static final int DEFAULT_TOP_N = 5;

    private final TopNMetricsHeaders headers;
    private final MetricFetcher metricFetcher;

    public TopNMetricsHandler(
            @Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            @Nonnull Time timeout,
            @Nonnull Map<String, String> responseHeaders,
            @Nonnull TopNMetricsHeaders headers,
            @Nonnull Executor executor,
            @Nonnull MetricFetcher metricFetcher) {
        super(leaderRetriever, timeout, responseHeaders, executor);
        this.headers = headers;
        this.metricFetcher = metricFetcher;
    }

    @Override
    protected CompletableFuture<TopNMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        // Fetch metrics data
                        metricFetcher.update();

                        // Collect top CPU consumers
                        List<CpuConsumer> topCpuConsumers = collectTopCpuConsumers(executionGraph);

                        // Collect top backpressure operators
                        List<BackpressureOperator> topBackpressureOperators =
                                collectTopBackpressureOperators(executionGraph);

                        // Collect top GC intensive tasks
                        List<GcIntensiveTask> topGcIntensiveTasks =
                                collectTopGcIntensiveTasks(executionGraph);

                        return new TopNMetricsResponseBody(
                                topCpuConsumers, topBackpressureOperators, topGcIntensiveTasks);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to fetch Top N metrics", e);
                    }
                },
                executor);
    }

    /**
     * Collect top N CPU consumers from all subtasks.
     */
    private List<CpuConsumer> collectTopCpuConsumers(AccessExecutionGraph executionGraph) {
        List<CpuConsumer> cpuConsumers = new ArrayList<>();

        for (AccessExecutionJobVertex jobVertex : executionGraph.getVerticesTopologically()) {
            for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
                String vertexName = jobVertex.getName();
                int subtaskId = vertex.getParallelSubtaskIndex();

                // Try to get CPU usage metric
                String cpuMetricName =
                        createScopedMetricName(
                                MetricNames.TASK_NAME, MetricNames.CPU_NAME, MetricNames.USAGE_NAME);

                Double cpuValue =
                        metricFetcher
                                .getMetric(
                                        vertex.getCurrentExecutionAttempt()
                                                .getTaskManagerLocation()
                                                .getResourceID(),
                                        vertexName,
                                        subtaskId,
                                        cpuMetricName)
                                .orElse(0.0);

                // Only include subtasks with meaningful CPU usage
                if (cpuValue > 0.1) {
                    String taskManagerId =
                            vertex.getCurrentExecutionAttempt()
                                    .getTaskManagerLocation()
                                    .getResourceID()
                                    .toString();
                    cpuConsumers.add(
                            new CpuConsumer(
                                    subtaskId, vertexName, cpuValue * 100.0, taskManagerId));
                }
            }
        }

        // Sort by CPU usage descending and take top N
        return cpuConsumers.stream()
                .sorted(Comparator.comparing(CpuConsumer::getCpuPercentage).reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    /**
     * Collect top N backpressure operators.
     */
    private List<BackpressureOperator> collectTopBackpressureOperators(
            AccessExecutionGraph executionGraph) {
        List<BackpressureOperator> backpressureOperators = new ArrayList<>();

        for (AccessExecutionJobVertex jobVertex : executionGraph.getVerticesTopologically()) {
            String operatorId = jobVertex.getID().toString();
            String operatorName = jobVertex.getName();

            // Get backpressure ratio from job vertex
            double backpressureRatio = getBackpressureRatio(jobVertex);

            // Create entry for each subtask with backpressure
            if (backpressureRatio > 0.05) {
                for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
                    int subtaskId = vertex.getParallelSubtaskIndex();
                    backpressureOperators.add(
                            new BackpressureOperator(operatorId, operatorName, backpressureRatio, subtaskId));
                }
            }
        }

        // Sort by backpressure ratio descending and take top N
        return backpressureOperators.stream()
                .sorted(Comparator.comparing(BackpressureOperator::getBackpressureRatio).reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    /**
     * Get backpressure ratio for a job vertex.
     */
    private double getBackpressureRatio(AccessExecutionJobVertex jobVertex) {
        try {
            // Try to get backpressure ratio from metrics
            String backpressureMetricName =
                    createScopedMetricName(MetricNames.TASK_NAME, MetricNames.BACKPRESSURE_NAME);

            Double ratio = 0.0;
            int count = 0;

            for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
                String vertexName = jobVertex.getName();
                int subtaskId = vertex.getParallelSubtaskIndex();

                Double value =
                        metricFetcher
                                .getMetric(
                                        vertex.getCurrentExecutionAttempt()
                                                .getTaskManagerLocation()
                                                .getResourceID(),
                                        vertexName,
                                        subtaskId,
                                        backpressureMetricName)
                                .orElse(0.0);
                ratio += value;
                count++;
            }

            return count > 0 ? ratio / count : 0.0;
        } catch (Exception e) {
            return 0.0;
        }
    }

    /**
     * Collect top N GC intensive tasks.
     */
    private List<GcIntensiveTask> collectTopGcIntensiveTasks(AccessExecutionGraph executionGraph) {
        List<GcIntensiveTask> gcTasks = new ArrayList<>();

        for (AccessExecutionJobVertex jobVertex : executionGraph.getVerticesTopologically()) {
            String taskName = jobVertex.getName();

            // Aggregate GC time across all subtasks
            double totalGcTime = 0.0;
            double totalCpuTime = 0.0;
            String taskManagerId = null;

            for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
                String vertexName = jobVertex.getName();
                int subtaskId = vertex.getParallelSubtaskIndex();

                // Get GC time metric
                String gcTimeMetricName =
                        createScopedMetricName(
                                MetricNames.TASK_NAME,
                                MetricNames.GC_NAME,
                                MetricNames.TIME_NAME);

                Double gcTime =
                        metricFetcher
                                .getMetric(
                                        vertex.getCurrentExecutionAttempt()
                                                .getTaskManagerLocation()
                                                .getResourceID(),
                                        vertexName,
                                        subtaskId,
                                        gcTimeMetricName)
                                .orElse(0.0);

                // Get CPU time metric (as proxy for total time)
                String cpuTimeMetricName =
                        createScopedMetricName(MetricNames.TASK_NAME, MetricNames.CPU_NAME, MetricNames.TIME_NAME);

                Double cpuTime =
                        metricFetcher
                                .getMetric(
                                        vertex.getCurrentExecutionAttempt()
                                                .getTaskManagerLocation()
                                                .getResourceID(),
                                        vertexName,
                                        subtaskId,
                                        cpuTimeMetricName)
                                .orElse(1.0);

                totalGcTime += gcTime;
                totalCpuTime += cpuTime;

                if (taskManagerId == null) {
                    taskManagerId =
                            vertex.getCurrentExecutionAttempt()
                                    .getTaskManagerLocation()
                                    .getResourceID()
                                    .toString();
                }
            }

            // Calculate GC time percentage
            double gcPercentage = totalCpuTime > 0 ? (totalGcTime / totalCpuTime) * 100 : 0.0;

            // Only include tasks with meaningful GC time
            if (gcPercentage > 1.0) {
                gcTasks.add(
                        new GcIntensiveTask(
                                taskName,
                                gcPercentage,
                                taskManagerId != null ? taskManagerId : "unknown"));
            }
        }

        // Sort by GC time percentage descending and take top N
        return gcTasks.stream()
                .sorted(Comparator.comparing(GcIntensiveTask::getGcTimePercentage).reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    @Override
    public TopNMetricsHeaders getMessageHeaders() {
        return headers;
    }

    /**
     * Simple metric fetcher for demonstration purposes.
     * In production, this would use the actual MetricFetcher.
     */
    private static class MetricFetcher {
        public void update() {
            // In production, this would update the metric cache
        }

        public java.util.Optional<Double> getMetric(
                org.apache.flink.runtime.clusterframework.types.ResourceID resourceID,
                String taskName,
                int subtaskId,
                String metricName) {
            // In production, this would query the metric store
            return java.util.Optional.empty();
        }
    }
}
