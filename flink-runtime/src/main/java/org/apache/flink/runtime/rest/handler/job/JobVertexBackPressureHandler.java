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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore.ComponentMetricStore;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore.SubtaskMetricStore;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore.TaskMetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo.SubtaskBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Request handler for the job vertex back pressure. */
public class JobVertexBackPressureHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                JobVertexBackPressureInfo,
                JobVertexMessageParameters> {

    private final MetricFetcher metricFetcher;

    public JobVertexBackPressureHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobVertexBackPressureInfo, JobVertexMessageParameters>
                    messageHeaders,
            MetricFetcher metricFetcher) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.metricFetcher = metricFetcher;
    }

    @Override
    protected CompletableFuture<JobVertexBackPressureInfo> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        metricFetcher.update();

        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final JobVertexID jobVertexId = request.getPathParameter(JobVertexIdPathParameter.class);

        TaskMetricStore taskMetricStore =
                metricFetcher
                        .getMetricStore()
                        .getTaskMetricStore(jobId.toString(), jobVertexId.toString());
        Map<String, Map<Integer, Integer>> jobRepresentativeExecutions =
                metricFetcher.getMetricStore().getRepresentativeAttempts().get(jobId.toString());
        Map<Integer, Integer> representativeAttempts =
                jobRepresentativeExecutions != null
                        ? jobRepresentativeExecutions.get(jobVertexId.toString())
                        : null;

        return CompletableFuture.completedFuture(
                taskMetricStore != null
                        ? createJobVertexBackPressureInfo(taskMetricStore, representativeAttempts)
                        : JobVertexBackPressureInfo.deprecated());
    }

    private JobVertexBackPressureInfo createJobVertexBackPressureInfo(
            TaskMetricStore taskMetricStore, Map<Integer, Integer> representativeAttempts) {
        List<SubtaskBackPressureInfo> subtaskBackPressureInfos =
                createSubtaskBackPressureInfo(taskMetricStore, representativeAttempts);
        return new JobVertexBackPressureInfo(
                JobVertexBackPressureInfo.VertexBackPressureStatus.OK,
                getBackPressureLevel(getMaxBackPressureRatio(subtaskBackPressureInfos)),
                metricFetcher.getLastUpdateTime(),
                subtaskBackPressureInfos);
    }

    private List<SubtaskBackPressureInfo> createSubtaskBackPressureInfo(
            TaskMetricStore taskMetricStore, Map<Integer, Integer> representativeAttempts) {
        Map<Integer, SubtaskMetricStore> subtaskMetricStores =
                taskMetricStore.getAllSubtaskMetricStores();
        List<SubtaskBackPressureInfo> result = new ArrayList<>(subtaskMetricStores.size());
        for (Map.Entry<Integer, SubtaskMetricStore> entry : subtaskMetricStores.entrySet()) {
            int subtaskIndex = entry.getKey();
            SubtaskMetricStore subtaskMetricStore = entry.getValue();
            Map<Integer, ComponentMetricStore> allAttemptsMetricStores =
                    subtaskMetricStore.getAllAttemptsMetricStores();
            if (allAttemptsMetricStores.isEmpty() || allAttemptsMetricStores.size() == 1) {
                result.add(
                        createSubtaskAttemptBackpressureInfo(
                                subtaskIndex, null, subtaskMetricStore, null));
            } else {
                int representativeAttempt =
                        representativeAttempts == null
                                ? -1
                                : representativeAttempts.getOrDefault(subtaskIndex, -1);
                if (!allAttemptsMetricStores.containsKey(representativeAttempt)) {
                    // allAttemptsMetricStores is not empty here
                    representativeAttempt = allAttemptsMetricStores.keySet().iterator().next();
                }
                List<SubtaskBackPressureInfo> otherConcurrentAttempts =
                        new ArrayList<>(allAttemptsMetricStores.size() - 1);
                for (Map.Entry<Integer, ComponentMetricStore> attemptStore :
                        allAttemptsMetricStores.entrySet()) {
                    if (attemptStore.getKey() == representativeAttempt) {
                        continue;
                    }
                    otherConcurrentAttempts.add(
                            createSubtaskAttemptBackpressureInfo(
                                    subtaskIndex,
                                    attemptStore.getKey(),
                                    attemptStore.getValue(),
                                    null));
                }
                result.add(
                        createSubtaskAttemptBackpressureInfo(
                                subtaskIndex,
                                representativeAttempt,
                                allAttemptsMetricStores.get(representativeAttempt),
                                otherConcurrentAttempts));
            }
        }
        result.sort(Comparator.comparingInt(SubtaskBackPressureInfo::getSubtask));
        return result;
    }

    private SubtaskBackPressureInfo createSubtaskAttemptBackpressureInfo(
            int subtaskIndex,
            @Nullable Integer attemptNumber,
            ComponentMetricStore metricStore,
            @Nullable List<SubtaskBackPressureInfo> otherConcurrentAttempts) {
        double backPressureRatio = getBackPressureRatio(metricStore);
        double idleRatio = getIdleRatio(metricStore);
        double busyRatio = getBusyRatio(metricStore);
        return new SubtaskBackPressureInfo(
                subtaskIndex,
                attemptNumber,
                getBackPressureLevel(backPressureRatio),
                backPressureRatio,
                idleRatio,
                busyRatio,
                otherConcurrentAttempts);
    }

    private double getMaxBackPressureRatio(List<SubtaskBackPressureInfo> subtaskBackPressureInfos) {
        return subtaskBackPressureInfos.stream()
                .mapToDouble(backPressureInfo -> backPressureInfo.getBackPressuredRatio())
                .max()
                .getAsDouble();
    }

    private double getBackPressureRatio(ComponentMetricStore metricStore) {
        return getMsPerSecondMetricAsRatio(metricStore, MetricNames.TASK_BACK_PRESSURED_TIME);
    }

    private double getIdleRatio(ComponentMetricStore metricStore) {
        return getMsPerSecondMetricAsRatio(metricStore, MetricNames.TASK_IDLE_TIME);
    }

    private double getBusyRatio(ComponentMetricStore metricStore) {
        return getMsPerSecondMetricAsRatio(metricStore, MetricNames.TASK_BUSY_TIME);
    }

    private double getMsPerSecondMetricAsRatio(
            ComponentMetricStore metricStore, String metricName) {
        return Double.valueOf(metricStore.getMetric(metricName, "0")) / 1_000;
    }

    /**
     * Returns the back pressure level as a String.
     *
     * @param backPressureRatio Ratio of back pressures samples to total number of samples.
     * @return Back pressure level ('ok', 'low', or 'high')
     */
    private static JobVertexBackPressureInfo.VertexBackPressureLevel getBackPressureLevel(
            double backPressureRatio) {
        if (backPressureRatio <= 0.10) {
            return JobVertexBackPressureInfo.VertexBackPressureLevel.OK;
        } else if (backPressureRatio <= 0.5) {
            return JobVertexBackPressureInfo.VertexBackPressureLevel.LOW;
        } else {
            return JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH;
        }
    }
}
