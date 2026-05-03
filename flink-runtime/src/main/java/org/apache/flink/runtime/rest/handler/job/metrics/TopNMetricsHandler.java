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
import org.apache.flink.runtime.metrics.MetricNames;
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
import org.apache.flink.runtime.rest.messages.job.metrics.TopNQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Request handler that returns Top N metrics for a job, including:
 *
 * <ul>
 *   <li><b>CPU consumers</b> &mdash; aggregated at the TaskManager level, scored by {@code
 *       Status.JVM.CPU.Load} of the JVM process. CPU is not a per-subtask quantity in Flink, so
 *       results are TaskManager-scoped.
 *   <li><b>Backpressured operators</b> &mdash; aggregated at the subtask level, scored by {@code
 *       backPressuredTimeMsPerSecond} (0&ndash;1000 ms/s), exposed as a ratio in [0, 1].
 *   <li><b>GC-intensive TaskManagers</b> &mdash; aggregated at the TaskManager level, scored by the
 *       sum of {@code Status.JVM.GarbageCollector.*.Time} across all known garbage collectors
 *       reported by that TaskManager (cumulative milliseconds since TM start).
 * </ul>
 *
 * <p>This handler does not filter TaskManagers by job assignment: JVM metrics are inherently
 * cluster-wide and per-job attribution would require slot-to-TM resolution that the metric store
 * does not currently expose.
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

    /** TaskManager-scoped JVM CPU load, range [0, 1] (fraction of available cores). */
    private static final String METRIC_CPU_LOAD = "Status.JVM.CPU.Load";

    /** Per-collector GC time in milliseconds: {@code Status.JVM.GarbageCollector.<name>.Time}. */
    private static final String METRIC_GC_TIME_PREFIX = "Status.JVM.GarbageCollector.";

    private static final String METRIC_GC_TIME_SUFFIX = ".Time";

    /**
     * Per-subtask back-pressure time in milliseconds per second, range [0, 1000]. See {@link
     * MetricNames#TASK_BACK_PRESSURED_TIME}.
     */
    private static final String METRIC_BACKPRESSURED_TIME_MS_PER_SECOND =
            MetricNames.TASK_BACK_PRESSURED_TIME;

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
        final List<Integer> topNValues = request.getQueryParameter(TopNQueryParameter.class);
        final int topN = topNValues.isEmpty() ? DEFAULT_TOP_N : topNValues.get(0);

        try {
            List<TopNMetricsResponseBody.CpuConsumerInfo> topCpuConsumers =
                    getTopCpuConsumers(metricStore, topN);
            List<TopNMetricsResponseBody.BackpressureOperatorInfo> topBackpressureOperators =
                    getTopBackpressureOperators(metricStore, jobId, topN);
            List<TopNMetricsResponseBody.GcTaskInfo> topGcIntensiveTasks =
                    getTopGcIntensiveTaskManagers(metricStore, topN);

            return CompletableFuture.completedFuture(
                    new TopNMetricsResponseBody(
                            topCpuConsumers, topBackpressureOperators, topGcIntensiveTasks));

        } catch (Exception e) {
            LOG.warn("Could not retrieve Top N metrics for job {}.", jobId, e);
            throw new RestHandlerException(
                    "Could not retrieve Top N metrics for job " + jobId,
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    e);
        }
    }

    /**
     * Collect Top N TaskManagers by JVM CPU load. CPU is TaskManager-scoped in Flink, so the {@code
     * subtaskId} field of the response is set to {@code -1} (not applicable) and {@code
     * operatorName} carries the literal {@code "TaskManager"}.
     */
    private List<TopNMetricsResponseBody.CpuConsumerInfo> getTopCpuConsumers(
            MetricStore metricStore, int topN) {
        List<TopNMetricsResponseBody.CpuConsumerInfo> result = new ArrayList<>();
        for (Map.Entry<String, MetricStore.TaskManagerMetricStore> entry :
                metricStore.getTaskManagers().entrySet()) {
            final String tmId = entry.getKey();
            final MetricStore.TaskManagerMetricStore tmStore = entry.getValue();
            final Double cpuLoad = parseDoubleOrNull(tmStore.getMetric(METRIC_CPU_LOAD), tmId);
            if (cpuLoad == null) {
                continue;
            }
            // JVM CPU load is reported as a fraction in [0, 1]; expose as percentage.
            result.add(
                    new TopNMetricsResponseBody.CpuConsumerInfo(
                            -1, tmId, "TaskManager", cpuLoad * 100.0, tmId));
        }
        return result.stream()
                .sorted(
                        Comparator.comparingDouble(
                                        TopNMetricsResponseBody.CpuConsumerInfo::getCpuPercentage)
                                .reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    /**
     * Collect Top N subtasks of the given job by back-pressure time (ms/s). If the job has no
     * metrics in the store yet (e.g. just submitted), an empty list is returned.
     */
    private List<TopNMetricsResponseBody.BackpressureOperatorInfo> getTopBackpressureOperators(
            MetricStore metricStore, String jobId, int topN) {
        List<TopNMetricsResponseBody.BackpressureOperatorInfo> result = new ArrayList<>();
        if (metricStore.getJobMetricStore(jobId) == null) {
            return result;
        }
        final Map<String, Map<Integer, Integer>> jobVertices =
                metricStore.getRepresentativeAttempts().getOrDefault(jobId, Collections.emptyMap());

        for (String vertexId : jobVertices.keySet()) {
            final MetricStore.TaskMetricStore taskStore =
                    metricStore.getTaskMetricStore(jobId, vertexId);
            if (taskStore == null) {
                continue;
            }
            for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> subtask :
                    taskStore.getAllSubtaskMetricStores().entrySet()) {
                final int subtaskIndex = subtask.getKey();
                final Double bpMsPerSec =
                        parseDoubleOrNull(
                                subtask.getValue()
                                        .getMetric(METRIC_BACKPRESSURED_TIME_MS_PER_SECOND),
                                vertexId + "." + subtaskIndex);
                if (bpMsPerSec == null) {
                    continue;
                }
                // Expose as a ratio in [0, 1] to keep the field name "backpressureRatio" honest.
                final double ratio = Math.max(0.0, Math.min(1.0, bpMsPerSec / 1000.0));
                result.add(
                        new TopNMetricsResponseBody.BackpressureOperatorInfo(
                                vertexId, vertexId, ratio, subtaskIndex));
            }
        }
        return result.stream()
                .sorted(
                        Comparator.comparingDouble(
                                        TopNMetricsResponseBody.BackpressureOperatorInfo
                                                ::getBackpressureRatio)
                                .reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    /**
     * Collect Top N TaskManagers by total GC time. For each TaskManager we sum {@code
     * Status.JVM.GarbageCollector.<name>.Time} across all known GC names. The result carries
     * cumulative GC time in milliseconds in the {@code gcTimePercentage} slot (the wire field is
     * kept stable for compatibility; see the response-body doc for the semantic contract).
     */
    private List<TopNMetricsResponseBody.GcTaskInfo> getTopGcIntensiveTaskManagers(
            MetricStore metricStore, int topN) {
        List<TopNMetricsResponseBody.GcTaskInfo> result = new ArrayList<>();
        for (Map.Entry<String, MetricStore.TaskManagerMetricStore> entry :
                metricStore.getTaskManagers().entrySet()) {
            final String tmId = entry.getKey();
            final MetricStore.TaskManagerMetricStore tmStore = entry.getValue();
            if (tmStore.garbageCollectorNames.isEmpty()) {
                continue;
            }
            double totalGcMillis = 0.0;
            boolean hasValue = false;
            for (String gcName : tmStore.garbageCollectorNames) {
                final String metricKey = METRIC_GC_TIME_PREFIX + gcName + METRIC_GC_TIME_SUFFIX;
                final Double gcMillis =
                        parseDoubleOrNull(tmStore.getMetric(metricKey), tmId + "." + gcName);
                if (gcMillis != null) {
                    totalGcMillis += gcMillis;
                    hasValue = true;
                }
            }
            if (hasValue) {
                result.add(
                        new TopNMetricsResponseBody.GcTaskInfo(
                                tmId, "TaskManager", totalGcMillis, tmId));
            }
        }
        return result.stream()
                .sorted(
                        Comparator.comparingDouble(
                                        TopNMetricsResponseBody.GcTaskInfo::getGcTimePercentage)
                                .reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    private static Double parseDoubleOrNull(String value, String contextForLog) {
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            LOG.debug(
                    "Ignoring non-numeric metric value '{}' (context: {}).", value, contextForLog);
            return null;
        }
    }
}
