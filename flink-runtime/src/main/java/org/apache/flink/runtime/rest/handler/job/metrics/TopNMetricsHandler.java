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
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNQueryParameter;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Request handler that returns Top N metrics for a job, organized into a "primary triage" tier and
 * a "JVM diagnostics" tier:
 *
 * <ul>
 *   <li><b>Primary triage (per-subtask / per-vertex, directly actionable)</b>
 *       <ul>
 *         <li><b>Backpressured subtasks</b> &mdash; scored by {@code backPressuredTimeMsPerSecond}
 *             (0&ndash;1000&nbsp;ms/s), exposed as a ratio in [0,&nbsp;1].
 *         <li><b>Busy subtasks</b> &mdash; scored by {@code busyTimeMsPerSecond}
 *             (0&ndash;1000&nbsp;ms/s), exposed as a ratio in [0,&nbsp;1]. This is the
 *             operator-level "where is CPU actually being spent" indicator; it complements the
 *             JVM-level CPU load below.
 *         <li><b>Lagging sources</b> &mdash; vertices reporting at least one of {@code
 *             pendingRecords}, {@code currentFetchEventTimeLag}, or {@code
 *             currentEmitEventTimeLag}. {@code pendingRecords} is summed across subtasks; the two
 *             lag metrics are taken as the per-vertex maximum. Vertices are ranked by pending
 *             records first, then by fetch-lag, then by emit-lag.
 *       </ul>
 *   <li><b>JVM diagnostics (TaskManager-scoped, coarser)</b>
 *       <ul>
 *         <li><b>CPU consumers</b> &mdash; scored by {@code Status.JVM.CPU.Load} of the JVM
 *             process. CPU is not a per-subtask quantity in Flink, so results are
 *             TaskManager-scoped.
 *         <li><b>GC-intensive TaskManagers</b> &mdash; scored by the sum of {@code
 *             Status.JVM.GarbageCollector.*.Time} across all known garbage collectors reported by
 *             that TaskManager (cumulative milliseconds since TM start).
 *       </ul>
 * </ul>
 *
 * <p>This handler does not filter TaskManagers by job assignment: JVM metrics are inherently
 * cluster-wide and per-job attribution would require slot-to-TM resolution that the metric store
 * does not currently expose.
 */
@Internal
public class TopNMetricsHandler
        extends AbstractExecutionGraphHandler<
                TopNMetricsResponseBody, TopNMetricsMessageParameters> {

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

    /**
     * Per-subtask busy time in milliseconds per second, range [0, 1000]. See {@link
     * MetricNames#TASK_BUSY_TIME}. Task-scoped metric (lives at the same level as back-pressure in
     * the metric store), so it can be read via {@code SubtaskMetricStore#getMetric(name)}.
     */
    private static final String METRIC_BUSY_TIME_MS_PER_SECOND = MetricNames.TASK_BUSY_TIME;

    /**
     * FLIP-33 source metric names. These are operator-scoped and therefore keyed as {@code
     * <operatorName>.<metricName>} inside the {@link MetricStore.SubtaskMetricStore}, so they must
     * be discovered by suffix match rather than by exact-name lookup.
     */
    private static final String METRIC_PENDING_RECORDS = MetricNames.PENDING_RECORDS;

    private static final String METRIC_FETCH_EVENT_TIME_LAG =
            MetricNames.CURRENT_FETCH_EVENT_TIME_LAG;

    private static final String METRIC_EMIT_EVENT_TIME_LAG =
            MetricNames.CURRENT_EMIT_EVENT_TIME_LAG;

    private final MetricFetcher metricFetcher;

    public TopNMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> headers,
            MetricFetcher metricFetcher,
            ExecutionGraphCache executionGraphCache,
            Executor executor) {
        super(
                leaderRetriever,
                timeout,
                headers,
                TopNMetricsHeaders.getInstance(),
                executionGraphCache,
                executor);
        this.metricFetcher = requireNonNull(metricFetcher, "metricFetcher must not be null");
    }

    @Override
    protected TopNMetricsResponseBody handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {
        metricFetcher.update();

        final MetricStore metricStore = metricFetcher.getMetricStore();
        final String jobId = request.getPathParameter(JobIDPathParameter.class).toString();
        final List<Integer> topNValues = request.getQueryParameter(TopNQueryParameter.class);
        final int topN = topNValues.isEmpty() ? DEFAULT_TOP_N : topNValues.get(0);

        // Build a mapping from vertex ID to friendly vertex name
        final Map<String, String> vertexIdToName = buildVertexIdToNameMap(executionGraphInfo);

        try {
            List<TopNMetricsResponseBody.CpuConsumerInfo> topCpuConsumers =
                    getTopCpuConsumers(metricStore, topN);
            List<TopNMetricsResponseBody.BackpressureOperatorInfo> topBackpressureOperators =
                    getTopBackpressureOperators(metricStore, jobId, topN, vertexIdToName);
            List<TopNMetricsResponseBody.GcTaskInfo> topGcIntensiveTasks =
                    getTopGcIntensiveTaskManagers(metricStore, topN);
            List<TopNMetricsResponseBody.BusyOperatorInfo> topBusyOperators =
                    getTopBusyOperators(metricStore, jobId, topN, vertexIdToName);
            List<TopNMetricsResponseBody.SourceLagInfo> topLaggingSources =
                    getTopLaggingSources(metricStore, jobId, topN, vertexIdToName);

            return new TopNMetricsResponseBody(
                    topCpuConsumers,
                    topBackpressureOperators,
                    topGcIntensiveTasks,
                    topBusyOperators,
                    topLaggingSources);

        } catch (Exception e) {
            LOG.warn("Could not retrieve Top N metrics for job {}.", jobId, e);
            throw new RestHandlerException(
                    "Could not retrieve Top N metrics for job " + jobId,
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    e);
        }
    }

    /**
     * Build a mapping from vertex ID to friendly vertex name.
     *
     * @param executionGraphInfo the execution graph info
     * @return a map from vertex ID to vertex name
     */
    private Map<String, String> buildVertexIdToNameMap(ExecutionGraphInfo executionGraphInfo) {
        final Map<String, String> vertexIdToName = new HashMap<>();
        for (AccessExecutionJobVertex vertex :
                executionGraphInfo.getArchivedExecutionGraph().getVerticesTopologically()) {
            vertexIdToName.put(vertex.getJobVertexId().toString(), vertex.getName());
        }
        return vertexIdToName;
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
            // The TM ID already encodes host:port-<suffix>; no friendlier alias is exported by
            // TaskManagers today, so we surface it for both the display name and the TM id
            // fields. subtaskId is null because the reading is TaskManager-scoped.
            result.add(
                    new TopNMetricsResponseBody.CpuConsumerInfo(
                            null, tmId, tmId, cpuLoad * 100.0, tmId));
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
            MetricStore metricStore, String jobId, int topN, Map<String, String> vertexIdToName) {
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
            // Get friendly vertex name
            final String vertexName = vertexIdToName.getOrDefault(vertexId, vertexId);

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
                                vertexId, vertexName, ratio, subtaskIndex));
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
     * Collect Top N subtasks of the given job by busy time (ms/s). Structurally identical to {@link
     * #getTopBackpressureOperators}: {@code busyTimeMsPerSecond} is a task-scoped metric emitted by
     * the runtime and therefore reachable through {@code SubtaskMetricStore#getMetric}. Values are
     * clamped and exposed as a ratio in [0,&nbsp;1].
     */
    private List<TopNMetricsResponseBody.BusyOperatorInfo> getTopBusyOperators(
            MetricStore metricStore, String jobId, int topN, Map<String, String> vertexIdToName) {
        List<TopNMetricsResponseBody.BusyOperatorInfo> result = new ArrayList<>();
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
            final String vertexName = vertexIdToName.getOrDefault(vertexId, vertexId);

            for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> subtask :
                    taskStore.getAllSubtaskMetricStores().entrySet()) {
                final int subtaskIndex = subtask.getKey();
                final Double busyMsPerSec =
                        parseDoubleOrNull(
                                subtask.getValue().getMetric(METRIC_BUSY_TIME_MS_PER_SECOND),
                                vertexId + "." + subtaskIndex);
                if (busyMsPerSec == null) {
                    continue;
                }
                final double ratio = Math.max(0.0, Math.min(1.0, busyMsPerSec / 1000.0));
                result.add(
                        new TopNMetricsResponseBody.BusyOperatorInfo(
                                vertexId, vertexName, ratio, subtaskIndex));
            }
        }
        return result.stream()
                .sorted(
                        Comparator.comparingDouble(
                                        TopNMetricsResponseBody.BusyOperatorInfo::getBusyRatio)
                                .reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    /**
     * Collect Top N lagging source vertices of the given job. A vertex is eligible if at least one
     * of its subtasks exposes {@code pendingRecords}, {@code currentFetchEventTimeLag}, or {@code
     * currentEmitEventTimeLag}. Because these metrics are operator-scoped, they are keyed inside
     * {@link MetricStore.SubtaskMetricStore} as {@code <operatorName>.<metricName>}; we locate them
     * by suffix match on the raw metrics map.
     *
     * <p>Aggregation across subtasks of a vertex:
     *
     * <ul>
     *   <li>{@code pendingRecords} &mdash; <i>sum</i> (cluster-level backlog).
     *   <li>fetch / emit event-time lag &mdash; <i>max</i> (worst-case freshness).
     * </ul>
     *
     * <p>Missing dimensions are reported as {@code null} so the UI can render them as "n/a".
     * Ranking prefers {@code pendingRecords} (when known), then fetch lag, then emit lag.
     */
    private List<TopNMetricsResponseBody.SourceLagInfo> getTopLaggingSources(
            MetricStore metricStore, String jobId, int topN, Map<String, String> vertexIdToName) {
        List<TopNMetricsResponseBody.SourceLagInfo> result = new ArrayList<>();
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

            double pendingSum = 0.0;
            boolean pendingSeen = false;
            Double maxFetchLag = null;
            Double maxEmitLag = null;

            for (Map.Entry<Integer, MetricStore.SubtaskMetricStore> subtask :
                    taskStore.getAllSubtaskMetricStores().entrySet()) {
                final MetricStore.SubtaskMetricStore subtaskStore = subtask.getValue();

                final Double pending =
                        findOperatorMetric(subtaskStore, METRIC_PENDING_RECORDS, vertexId);
                if (pending != null) {
                    pendingSum += pending;
                    pendingSeen = true;
                }

                final Double fetchLag =
                        findOperatorMetric(subtaskStore, METRIC_FETCH_EVENT_TIME_LAG, vertexId);
                if (fetchLag != null && (maxFetchLag == null || fetchLag > maxFetchLag)) {
                    maxFetchLag = fetchLag;
                }

                final Double emitLag =
                        findOperatorMetric(subtaskStore, METRIC_EMIT_EVENT_TIME_LAG, vertexId);
                if (emitLag != null && (maxEmitLag == null || emitLag > maxEmitLag)) {
                    maxEmitLag = emitLag;
                }
            }

            if (!pendingSeen && maxFetchLag == null && maxEmitLag == null) {
                continue;
            }

            final String vertexName = vertexIdToName.getOrDefault(vertexId, vertexId);
            result.add(
                    new TopNMetricsResponseBody.SourceLagInfo(
                            vertexId,
                            vertexName,
                            pendingSeen ? pendingSum : null,
                            maxFetchLag,
                            maxEmitLag));
        }

        // Unavailable (null) metrics rank as the smallest so vertices with real data float to
        // the top; reversed() then flips the overall order to descending.
        final Comparator<Double> nullsSmallest = Comparator.nullsFirst(Comparator.naturalOrder());
        return result.stream()
                .sorted(
                        Comparator.comparing(
                                        (TopNMetricsResponseBody.SourceLagInfo v) ->
                                                v.getPendingRecords(),
                                        nullsSmallest)
                                .thenComparing(
                                        TopNMetricsResponseBody.SourceLagInfo
                                                ::getMaxFetchEventTimeLagMs,
                                        nullsSmallest)
                                .thenComparing(
                                        TopNMetricsResponseBody.SourceLagInfo
                                                ::getMaxEmitEventTimeLagMs,
                                        nullsSmallest)
                                .reversed())
                .limit(topN)
                .collect(Collectors.toList());
    }

    /**
     * Resolve an operator-scoped metric stored under {@code <operatorName>.<metricName>} in the
     * given subtask store. If multiple operators within the same subtask expose the same metric
     * (chained sources), the numeric values are summed; this matches the "cluster-level backlog"
     * semantics for {@code pendingRecords} and is a reasonable over-estimate for lag values (a
     * chained source's lag is typically dominated by a single operator anyway).
     */
    private Double findOperatorMetric(
            MetricStore.SubtaskMetricStore subtaskStore, String metricName, String vertexId) {
        if (subtaskStore == null) {
            return null;
        }
        // FLIP-33 source metrics are operator-scoped and therefore keyed as
        // "<operatorName>.<metricName>" in the MetricStore subtask view. We sum across all
        // matching operator keys within the subtask to tolerate chained operators that each
        // expose their own source metrics.
        final String suffix = "." + metricName;
        Double aggregated = null;
        for (Map.Entry<String, String> entry : subtaskStore.metrics.entrySet()) {
            final String key = entry.getKey();
            if (!key.endsWith(suffix)) {
                continue;
            }
            final Double value = parseDoubleOrNull(entry.getValue(), vertexId + "." + key);
            if (value != null) {
                aggregated = aggregated == null ? value : aggregated + value;
            }
        }
        return aggregated;
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
                // Try to get a friendlier TM name
                // TM ID already carries host:port-<suffix>; no friendlier alias is reported.
                final String tmName = tmId;
                result.add(
                        new TopNMetricsResponseBody.GcTaskInfo(tmId, tmName, totalGcMillis, tmId));
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
