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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.handler.legacy.messages.DiagnosisResponseBody;
import org.apache.flink.runtime.rest.handler.legacy.messages.DiagnosisResponseBody.DiagnosticSuggestion;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.diagnosis.DiagnosisMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler that provides automated diagnostic suggestions based on job metrics. The
 * Diagnosis Advisor analyzes CPU, memory, GC, and backpressure metrics to identify common
 * performance issues and provide actionable recommendations.
 */
public class DiagnosisHandler
        extends AbstractJobHandler<
                EmptyRequestBody, DiagnosisResponseBody, DiagnosisMessageParameters> {

    private static final String CPU_USAGE_METRIC = "taskmanager.cpu.usage";
    private static final String HEAP_USED_METRIC = "taskmanager.memory.heap.used";
    private static final String HEAP_MAX_METRIC = "taskmanager.memory.heap.max";
    private static final String GC_TIME_METRIC = "taskmanager.GarbageCollector.time";
    private static final String GC_COUNT_METRIC = "taskmanager.GarbageCollector.count";
    private static final String BACKPRESSURE_METRIC = "tasks.backpressure.ratio";

    // Thresholds for diagnosis rules
    private static final double HIGH_CPU_THRESHOLD = 0.8; // 80%
    private static final double HIGH_HEAP_THRESHOLD = 0.7; // 70%
    private static final double HIGH_BACKPRESSURE_THRESHOLD = 0.5; // 50%
    private static final double LOW_CPU_THRESHOLD = 0.3; // 30%

    private final Executor executor;
    private final MetricFetcher fetcher;

    public DiagnosisHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(leaderRetriever, timeout, responseHeaders, DiagnosisHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
    }

    @Override
    protected CompletableFuture<DiagnosisResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        fetcher.update();
                        MetricStore store = fetcher.getMetricStore();

                        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
                        MetricStore.JobMetricStore jobMetricStore = store.getJob(jobId.toHexString());

                        if (jobMetricStore == null) {
                            return createEmptyResponse();
                        }

                        // Collect metrics for diagnosis
                        Map<String, Object> metrics = collectMetrics(jobMetricStore);

                        // Apply diagnosis rules
                        List<DiagnosticSuggestion> suggestions = diagnose(metrics);

                        return new DiagnosisResponseBody(suggestions, Instant.now().toString());

                    } catch (Exception e) {
                        log.warn("Could not generate diagnosis.", e);
                        throw new RestHandlerException(
                                "Could not generate diagnosis.",
                                HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    }
                },
                executor);
    }

    private DiagnosisResponseBody createEmptyResponse() {
        return new DiagnosisResponseBody(Collections.emptyList(), Instant.now().toString());
    }

    private Map<String, Object> collectMetrics(MetricStore.JobMetricStore jobMetricStore) {
        Map<String, Object> metrics = new HashMap<>();

        // Collect CPU metrics (average across all task managers)
        double totalCpuUsage = 0.0;
        int cpuMetricCount = 0;

        // Collect memory and GC metrics
        double totalHeapUsed = 0.0;
        double totalHeapMax = 0.0;
        double totalGcCount = 0.0;

        // Collect backpressure metrics
        double maxBackpressureRatio = 0.0;

        // Iterate through task managers
        for (MetricStore.TaskManagerMetricStore tmStore : jobMetricStore.taskManagers.values()) {
            // CPU metrics
            String cpuValue = tmStore.metrics.get(CPU_USAGE_METRIC);
            if (cpuValue != null) {
                try {
                    totalCpuUsage += Double.parseDouble(cpuValue);
                    cpuMetricCount++;
                } catch (NumberFormatException e) {
                    // Skip invalid values
                }
            }

            // Memory metrics
            String heapUsed = tmStore.metrics.get(HEAP_USED_METRIC);
            String heapMax = tmStore.metrics.get(HEAP_MAX_METRIC);
            if (heapUsed != null && heapMax != null) {
                try {
                    totalHeapUsed += Double.parseDouble(heapUsed);
                    totalHeapMax += Double.parseDouble(heapMax);
                } catch (NumberFormatException e) {
                    // Skip invalid values
                }
            }

            // GC metrics
            String gcCount = tmStore.metrics.get(GC_COUNT_METRIC);
            if (gcCount != null) {
                try {
                    totalGcCount += Double.parseDouble(gcCount);
                } catch (NumberFormatException e) {
                    // Skip invalid values
                }
            }
        }

        // Calculate average values
        double avgCpuUsage = cpuMetricCount > 0 ? totalCpuUsage / cpuMetricCount : 0.0;
        double heapUsageRatio = totalHeapMax > 0 ? totalHeapUsed / totalHeapMax : 0.0;

        // Collect backpressure from tasks
        for (MetricStore.TaskMetricStore taskStore : jobMetricStore.tasks.values()) {
            for (MetricStore.ComponentMetricStore subtaskStore : taskStore.subtasks.values()) {
                String bpValue = subtaskStore.metrics.get(BACKPRESSURE_METRIC);
                if (bpValue != null) {
                    try {
                        double bpRatio = Double.parseDouble(bpValue);
                        maxBackpressureRatio = Math.max(maxBackpressureRatio, bpRatio);
                    } catch (NumberFormatException e) {
                        // Skip invalid values
                    }
                }
            }
        }

        metrics.put("cpuUsage", avgCpuUsage);
        metrics.put("heapUsageRatio", heapUsageRatio);
        metrics.put("gcCount", totalGcCount);
        metrics.put("maxBackpressureRatio", maxBackpressureRatio);

        return metrics;
    }

    private List<DiagnosticSuggestion> diagnose(Map<String, Object> metrics) {
        List<DiagnosticSuggestion> suggestions = new ArrayList<>();

        double cpuUsage = (double) metrics.getOrDefault("cpuUsage", 0.0);
        double heapUsageRatio = (double) metrics.getOrDefault("heapUsageRatio", 0.0);
        double gcCount = (double) metrics.getOrDefault("gcCount", 0.0);
        double maxBackpressureRatio = (double) metrics.getOrDefault("maxBackpressureRatio", 0.0);

        // Rule 1: High CPU + High Heap Memory -> Possible GC issue
        if (cpuUsage > HIGH_CPU_THRESHOLD && heapUsageRatio > HIGH_HEAP_THRESHOLD) {
            Map<String, Object> ruleMetrics = new HashMap<>();
            ruleMetrics.put("cpuUsage", cpuUsage);
            ruleMetrics.put("heapUsageRatio", heapUsageRatio);
            ruleMetrics.put("gcCount", gcCount);

            List<String> actions = new ArrayList<>();
            actions.add("Check GarbageCollectorTime metrics");
            actions.add("Review heap size configuration");
            actions.add("Analyze GC logs");
            actions.add("Consider increasing heap size or optimizing object allocation");

            suggestions.add(
                    new DiagnosticSuggestion(
                            "warning",
                            "High CPU Usage with High Memory Consumption",
                            "High CPU may be caused by frequent GC. Check GC logs or increase heap size.",
                            ruleMetrics,
                            actions));
        }

        // Rule 2: High CPU + Normal Heap -> Heavy computation or backpressure
        else if (cpuUsage > HIGH_CPU_THRESHOLD && heapUsageRatio < HIGH_HEAP_THRESHOLD) {
            Map<String, Object> ruleMetrics = new HashMap<>();
            ruleMetrics.put("cpuUsage", cpuUsage);
            ruleMetrics.put("heapUsageRatio", heapUsageRatio);
            ruleMetrics.put("maxBackpressureRatio", maxBackpressureRatio);

            List<String> actions = new ArrayList<>();
            actions.add("Check backpressure metrics");
            actions.add("Review operator implementations for optimization opportunities");
            actions.add("Analyze task execution time breakdown");

            suggestions.add(
                    new DiagnosticSuggestion(
                            "info",
                            "High CPU Usage with Normal Memory",
                            "High CPU is likely caused by heavy user computation. Check backpressure.",
                            ruleMetrics,
                            actions));
        }

        // Rule 3: Low CPU + High Backpressure -> I/O bottleneck
        if (cpuUsage < LOW_CPU_THRESHOLD && maxBackpressureRatio > HIGH_BACKPRESSURE_THRESHOLD) {
            Map<String, Object> ruleMetrics = new HashMap<>();
            ruleMetrics.put("cpuUsage", cpuUsage);
            ruleMetrics.put("maxBackpressureRatio", maxBackpressureRatio);

            List<String> actions = new ArrayList<>();
            actions.add("Check external system connectivity");
            actions.add("Review source/sink performance");
            actions.add("Verify network configuration");

            suggestions.add(
                    new DiagnosticSuggestion(
                            "warning",
                            "Low CPU with High Backpressure",
                            "Possible I/O bottleneck or external dependency delay.",
                            ruleMetrics,
                            actions));
        }

        // Rule 4: High GC count warning
        if (gcCount > 10000) {
            Map<String, Object> ruleMetrics = new HashMap<>();
            ruleMetrics.put("gcCount", gcCount);

            List<String> actions = new ArrayList<>();
            actions.add("Review object allocation patterns");
            actions.add("Consider using object pooling");
            actions.add("Check for memory leaks");

            suggestions.add(
                    new DiagnosticSuggestion(
                            "warning",
                            "High Garbage Collection Count",
                            "Job is experiencing excessive GC activity. This may impact performance.",
                            ruleMetrics,
                            actions));
        }

        return suggestions;
    }
}
