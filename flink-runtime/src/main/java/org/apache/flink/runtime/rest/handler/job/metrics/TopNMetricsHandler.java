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
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Request handler that returns Top N metrics for a job, including CPU consumers, backpressured
 * operators, and GC-intensive tasks.
 */
public class TopNMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                TopNMetricsResponseBody,
                TopNMetricsMessageParameters> {

    private static final int DEFAULT_TOP_N = 5;

    private static final String CPU_METRIC = "taskmanager.cpu.time";
    private static final String BACKPRESSURE_METRIC = "tasks.backpressure.ratio";
    private static final String GC_TIME_METRIC = "tasks.GC.time";
    private static final String GC_COUNT_METRIC = "tasks.GC.count";

    private final Executor executor;
    private final MetricFetcher fetcher;

    public TopNMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                TopNMetricsHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
    }

    @Override
    protected CompletableFuture<TopNMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        fetcher.update();
                        MetricStore store = fetcher.getMetricStore();

                        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
                        MetricStore.JobMetricStoreSnapshot jobMetrics = store.getJobs();

                        // Get job's metric store
                        MetricStore.JobMetricStore jobMetricStore =
                                (MetricStore.JobMetricStore)
                                        jobMetrics.get(jobId.toHexString());

                        if (jobMetricStore == null) {
                            return createEmptyResponse();
                        }

                        // Collect Top N metrics
                        List<TopNMetricsResponseBody.CpuConsumerInfo> topCpuConsumers =
                                getTopCpuConsumers(jobMetricStore);

                        List<TopNMetricsResponseBody.BackpressureOperatorInfo>
                                topBackpressureOperators = getTopBackpressureOperators(jobMetricStore);

                        List<TopNMetricsResponseBody.GcTaskInfo> topGcIntensiveTasks =
                                getTopGcIntensiveTasks(jobMetricStore);

                        return new TopNMetricsResponseBody(
                                topCpuConsumers, topBackpressureOperators, topGcIntensiveTasks);

                    } catch (Exception e) {
                        log.warn("Could not retrieve Top N metrics.", e);
                        throw new RestHandlerException(
                                "Could not retrieve Top N metrics.",
                                HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    }
                },
                executor);
    }

    private TopNMetricsResponseBody createEmptyResponse() {
        return new TopNMetricsResponseBody(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    private List<TopNMetricsResponseBody.CpuConsumerInfo> getTopCpuConsumers(
            MetricStore.JobMetricStore jobMetricStore) {
        List<TopNMetricsResponseBody.CpuConsumerInfo> cpuConsumers = new ArrayList<>();

        // Iterate through all vertices and subtasks
        for (Map.Entry<String, MetricStore.TaskMetricStore> vertexEntry :
                jobMetricStore.tasks.entrySet()) {
            String taskName = vertexEntry.getKey();
            MetricStore.TaskMetricStore taskMetricStore = vertexEntry.getValue();

            for (Map.Entry<String, MetricStore.ComponentMetricStore> subtaskEntry :
                    taskMetricStore.subtasks.entrySet()) {
                String subtaskId = subtaskEntry.getKey();
                MetricStore.ComponentMetricStore subtaskMetrics = subtaskEntry.getValue();

                String cpuValue = subtaskMetrics.metrics.get(CPU_METRIC);
                if (cpuValue != null) {
                    try {
                        double cpuTime = Double.parseDouble(cpuValue);
                        cpuConsumers.add(
                                new TopNMetricsResponseBody.CpuConsumerInfo(
                                        Integer.parseInt(subtaskId),
                                        taskName,
                                        taskName, // Using taskName as operatorName for simplicity
                                        cpuTime,
                                        "unknown")); // TaskManager ID not directly available
                    } catch (NumberFormatException e) {
                        // Skip invalid values
                    }
                }
            }
        }

        // Sort by CPU usage and take top N
        return cpuConsumers.stream()
                .sorted(
                        Comparator.comparing(
                                TopNMetricsResponseBody.CpuConsumerInfo::getCpuPercentage)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    private List<TopNMetricsResponseBody.BackpressureOperatorInfo> getTopBackpressureOperators(
            MetricStore.JobMetricStore jobMetricStore) {
        List<TopNMetricsResponseBody.BackpressureOperatorInfo> backpressureOperators =
                new ArrayList<>();

        // Iterate through all vertices and subtasks
        for (Map.Entry<String, MetricStore.TaskMetricStore> vertexEntry :
                jobMetricStore.tasks.entrySet()) {
            String operatorName = vertexEntry.getKey();
            MetricStore.TaskMetricStore taskMetricStore = vertexEntry.getValue();

            for (Map.Entry<String, MetricStore.ComponentMetricStore> subtaskEntry :
                    taskMetricStore.subtasks.entrySet()) {
                String subtaskId = subtaskEntry.getKey();
                MetricStore.ComponentMetricStore subtaskMetrics = subtaskEntry.getValue();

                String backpressureValue = subtaskMetrics.metrics.get(BACKPRESSURE_METRIC);
                if (backpressureValue != null) {
                    try {
                        double backpressureRatio = Double.parseDouble(backpressureValue);
                        backpressureOperators.add(
                                new TopNMetricsResponseBody.BackpressureOperatorInfo(
                                        operatorName,
                                        operatorName,
                                        backpressureRatio,
                                        Integer.parseInt(subtaskId)));
                    } catch (NumberFormatException e) {
                        // Skip invalid values
                    }
                }
            }
        }

        // Sort by backpressure ratio and take top N
        return backpressureOperators.stream()
                .sorted(
                        Comparator.comparing(
                                TopNMetricsResponseBody.BackpressureOperatorInfo::
                                        getBackpressureRatio)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    private List<TopNMetricsResponseBody.GcTaskInfo> getTopGcIntensiveTasks(
            MetricStore.JobMetricStore jobMetricStore) {
        List<TopNMetricsResponseBody.GcTaskInfo> gcIntensiveTasks = new ArrayList<>();

        // Iterate through all vertices and subtasks
        for (Map.Entry<String, MetricStore.TaskMetricStore> vertexEntry :
                jobMetricStore.tasks.entrySet()) {
            String taskName = vertexEntry.getKey();
            MetricStore.TaskMetricStore taskMetricStore = vertexEntry.getValue();

            for (Map.Entry<String, MetricStore.ComponentMetricStore> subtaskEntry :
                    taskMetricStore.subtasks.entrySet()) {
                String subtaskId = subtaskEntry.getKey();
                MetricStore.ComponentMetricStore subtaskMetrics = subtaskEntry.getValue();

                String gcTimeValue = subtaskMetrics.metrics.get(GC_TIME_METRIC);
                if (gcTimeValue != null) {
                    try {
                        double gcTime = Double.parseDouble(gcTimeValue);
                        gcIntensiveTasks.add(
                                new TopNMetricsResponseBody.GcTaskInfo(
                                        subtaskId,
                                        taskName,
                                        gcTime,
                                        "unknown")); // TaskManager ID not directly available
                    } catch (NumberFormatException e) {
                        // Skip invalid values
                    }
                }
            }
        }

        // Sort by GC time and take top N
        return gcIntensiveTasks.stream()
                .sorted(
                        Comparator.comparing(TopNMetricsResponseBody.GcTaskInfo::getGcTimePercentage)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }
}
