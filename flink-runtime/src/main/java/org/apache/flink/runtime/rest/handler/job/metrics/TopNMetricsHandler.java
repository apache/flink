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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Request handler that returns Top N metrics for a job, including CPU consumers, backpressured
 * operators, and GC-intensive tasks.
 */
@Internal
public class TopNMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                TopNMetricsResponseBody,
                TopNMetricsMessageParameters> {

    private static final Logger LOG = LoggerFactory.getLogger(TopNMetricsHandler.class);

    private static final int DEFAULT_TOP_N = 5;

    private static final String CPU_METRIC = "taskmanager.cpu.time";
    private static final String BACKPRESSURE_METRIC = "tasks.backpressure.ratio";
    private static final String GC_TIME_METRIC = "tasks.GC.time";

    private final MetricFetcher metricFetcher;

    public TopNMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> headers,
            MetricFetcher metricFetcher) {
        super(leaderRetriever, timeout, headers, TopNMetricsHeaders.getInstance());
        this.metricFetcher = requireNonNull(metricFetcher, "metricFetcher must not be null");
    }

    @Override
    protected CompletableFuture<TopNMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        metricFetcher.update();

        final MetricStore metricStore = metricFetcher.getMetricStore();
        final String jobId = request.getPathParameter(JobIDPathParameter.class).toString();

        try {
            // Collect Top N metrics
            List<TopNMetricsResponseBody.CpuConsumerInfo> topCpuConsumers =
                    getTopCpuConsumers(metricStore, jobId);

            List<TopNMetricsResponseBody.BackpressureOperatorInfo> topBackpressureOperators =
                    getTopBackpressureOperators(metricStore, jobId);

            List<TopNMetricsResponseBody.GcTaskInfo> topGcIntensiveTasks =
                    getTopGcIntensiveTasks(metricStore, jobId);

            return CompletableFuture.completedFuture(
                    new TopNMetricsResponseBody(
                            topCpuConsumers, topBackpressureOperators, topGcIntensiveTasks));

        } catch (Exception e) {
            LOG.info("Could not retrieve Top N metrics for job {}.", jobId, e);
            throw new RestHandlerException(
                    "Could not retrieve Top N metrics for job " + jobId,
                    HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private List<TopNMetricsResponseBody.CpuConsumerInfo> getTopCpuConsumers(
            MetricStore metricStore, String jobId) {
        List<TopNMetricsResponseBody.CpuConsumerInfo> cpuConsumers = new ArrayList<>();

        // Get all task IDs for this job from representativeAttempts
        Map<String, Map<String, Map<Integer, Integer>>> representativeAttempts =
                metricStore.getRepresentativeAttempts();
        Map<String, Map<Integer, Integer>> jobTasks = representativeAttempts.get(jobId);

        if (jobTasks == null) {
            return cpuConsumers;
        }

        for (String taskId : jobTasks.keySet()) {
            MetricStore.TaskMetricStore taskMetricStore =
                    metricStore.getTaskMetricStore(jobId, taskId);
            if (taskMetricStore == null) {
                continue;
            }

            // Get all subtasks
            for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> subtaskEntry :
                    taskMetricStore.getAllSubtaskMetricStores().entrySet()) {
                int subtaskIndex = subtaskEntry.getKey();
                MetricStore.SubtaskMetricStore subtaskMetricStore = subtaskEntry.getValue();

                String cpuValue = subtaskMetricStore.getMetric(CPU_METRIC);
                if (cpuValue != null) {
                    try {
                        double cpuTime = Double.parseDouble(cpuValue);
                        cpuConsumers.add(
                                new TopNMetricsResponseBody.CpuConsumerInfo(
                                        subtaskIndex,
                                        taskId,
                                        taskId, // Using taskId as operatorName for simplicity
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
            MetricStore metricStore, String jobId) {
        List<TopNMetricsResponseBody.BackpressureOperatorInfo> backpressureOperators =
                new ArrayList<>();

        // Get all task IDs for this job from representativeAttempts
        Map<String, Map<String, Map<Integer, Integer>>> representativeAttempts =
                metricStore.getRepresentativeAttempts();
        Map<String, Map<Integer, Integer>> jobTasks = representativeAttempts.get(jobId);

        if (jobTasks == null) {
            return backpressureOperators;
        }

        for (String operatorName : jobTasks.keySet()) {
            MetricStore.TaskMetricStore taskMetricStore =
                    metricStore.getTaskMetricStore(jobId, operatorName);
            if (taskMetricStore == null) {
                continue;
            }

            // Get all subtasks
            for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> subtaskEntry :
                    taskMetricStore.getAllSubtaskMetricStores().entrySet()) {
                int subtaskIndex = subtaskEntry.getKey();
                MetricStore.SubtaskMetricStore subtaskMetricStore = subtaskEntry.getValue();

                String backpressureValue = subtaskMetricStore.getMetric(BACKPRESSURE_METRIC);
                if (backpressureValue != null) {
                    try {
                        double backpressureRatio = Double.parseDouble(backpressureValue);
                        backpressureOperators.add(
                                new TopNMetricsResponseBody.BackpressureOperatorInfo(
                                        operatorName,
                                        operatorName,
                                        backpressureRatio,
                                        subtaskIndex));
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
                                        TopNMetricsResponseBody.BackpressureOperatorInfo
                                                ::getBackpressureRatio)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    private List<TopNMetricsResponseBody.GcTaskInfo> getTopGcIntensiveTasks(
            MetricStore metricStore, String jobId) {
        List<TopNMetricsResponseBody.GcTaskInfo> gcIntensiveTasks = new ArrayList<>();

        // Get all task IDs for this job from representativeAttempts
        Map<String, Map<String, Map<Integer, Integer>>> representativeAttempts =
                metricStore.getRepresentativeAttempts();
        Map<String, Map<Integer, Integer>> jobTasks = representativeAttempts.get(jobId);

        if (jobTasks == null) {
            return gcIntensiveTasks;
        }

        for (String taskId : jobTasks.keySet()) {
            MetricStore.TaskMetricStore taskMetricStore =
                    metricStore.getTaskMetricStore(jobId, taskId);
            if (taskMetricStore == null) {
                continue;
            }

            // Get all subtasks
            for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> subtaskEntry :
                    taskMetricStore.getAllSubtaskMetricStores().entrySet()) {
                int subtaskIndex = subtaskEntry.getKey();
                MetricStore.SubtaskMetricStore subtaskMetricStore = subtaskEntry.getValue();

                String gcTimeValue = subtaskMetricStore.getMetric(GC_TIME_METRIC);
                if (gcTimeValue != null) {
                    try {
                        double gcTime = Double.parseDouble(gcTimeValue);
                        gcIntensiveTasks.add(
                                new TopNMetricsResponseBody.GcTaskInfo(
                                        String.valueOf(subtaskIndex),
                                        taskId,
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
                        Comparator.comparing(
                                        TopNMetricsResponseBody.GcTaskInfo::getGcTimePercentage)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }
}
