/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.JobInitializationMetrics.SumMaxDuration;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.runtime.checkpoint.JobInitializationMetrics.UNSET;
import static org.apache.flink.util.Preconditions.checkState;

class JobInitializationMetricsBuilder {
    private static final Logger LOG =
            LoggerFactory.getLogger(JobInitializationMetricsBuilder.class);

    private final Map<ExecutionAttemptID, SubTaskInitializationMetrics> reportedMetrics =
            new HashMap<>();
    private final Set<ExecutionAttemptID> toInitialize;
    private final long startTs;
    private Optional<Long> stateSize = Optional.empty();
    private Optional<Long> checkpointId = Optional.empty();
    private Optional<CheckpointProperties> checkpointProperties = Optional.empty();
    private Optional<String> externalPath = Optional.empty();

    /**
     * The {@code JobInitializationMetricsBuilder} handles the initialization metrics for deployed
     * {@link Execution Executions}. Building the final {@link JobInitializationMetrics} will be
     * only possible if all the subtasks reported their {@link SubTaskInitializationMetrics} back.
     *
     * @param toInitialize The {@link ExecutionAttemptID} of the subtasks that are about to be
     *     deployed.
     * @param startTs The time the initialization was started.
     * @see #isComplete()
     * @see #reportInitializationMetrics(ExecutionAttemptID, SubTaskInitializationMetrics)
     */
    JobInitializationMetricsBuilder(Set<ExecutionAttemptID> toInitialize, long startTs) {
        this.toInitialize = new HashSet<>(toInitialize);
        this.startTs = startTs;
    }

    public boolean isComplete() {
        return toInitialize.isEmpty();
    }

    public long getStartTs() {
        return startTs;
    }

    public Optional<RestoredCheckpointStats> buildRestoredCheckpointStats() {
        if (checkpointId.isPresent() && checkpointProperties.isPresent() && stateSize.isPresent()) {
            return Optional.of(
                    new RestoredCheckpointStats(
                            checkpointId.get(),
                            checkpointProperties.get(),
                            startTs,
                            externalPath.orElse(null),
                            stateSize.get()));
        }
        return Optional.empty();
    }

    public JobInitializationMetrics build() {
        checkState(isComplete());

        long initializationEndTimestamp = 0;
        Map<String, SumMaxDuration> duationMetrics = new HashMap<>();
        InitializationStatus status = InitializationStatus.COMPLETED;

        for (SubTaskInitializationMetrics reportedMetric : reportedMetrics.values()) {
            initializationEndTimestamp =
                    Math.max(reportedMetric.getEndTs(), initializationEndTimestamp);
            aggregateMetrics(duationMetrics, reportedMetric.getDurationMetrics());
            switch (reportedMetric.getStatus()) {
                case COMPLETED:
                    break;
                case FAILED:
                    status = InitializationStatus.FAILED;
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown reported "
                                    + InitializationStatus.class.getSimpleName()
                                    + " = ["
                                    + reportedMetric.getStatus()
                                    + "]");
            }
        }

        return new JobInitializationMetrics(
                checkpointId.orElse(UNSET),
                stateSize.orElse(UNSET),
                status,
                startTs,
                initializationEndTimestamp,
                duationMetrics);
    }

    private static void aggregateMetrics(
            Map<String, SumMaxDuration> target, Map<String, Long> sourceDurationMetrics) {
        for (Map.Entry<String, Long> durationMetric : sourceDurationMetrics.entrySet()) {
            String name = durationMetric.getKey();
            long value = durationMetric.getValue();
            SumMaxDuration targetDuration =
                    target.computeIfAbsent(name, (k) -> new SumMaxDuration(k));
            targetDuration.addDuration(value);
        }
    }

    /**
     * Reports the {@link SubTaskInitializationMetrics} for a currently deployed subtask based on
     * the provided {@link ExecutionAttemptID}.
     */
    public void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics) {
        LOG.debug(
                "Reported SubTaskInitializationMetrics={} for execution attempt {}.",
                initializationMetrics,
                executionAttemptId);
        if (toInitialize.remove(executionAttemptId)) {
            reportedMetrics.put(executionAttemptId, initializationMetrics);
        } else {
            if (reportedMetrics.containsKey(executionAttemptId)) {
                LOG.warn(
                        "Reported more than one SubTaskInitializationMetrics instance for execution attempt {}.",
                        executionAttemptId);
            } else {
                LOG.warn(
                        "Reported SubTaskInitializationMetrics instance for unknown execution attempt {}.",
                        executionAttemptId);
            }
        }
    }

    public JobInitializationMetricsBuilder setRestoredCheckpointStats(
            long checkpointId,
            long stateSize,
            CheckpointProperties checkpointProperties,
            String externalPath) {
        this.checkpointId = Optional.of(checkpointId);
        this.stateSize = Optional.of(stateSize);
        this.checkpointProperties = Optional.ofNullable(checkpointProperties);
        this.externalPath = Optional.ofNullable(externalPath);
        return this;
    }
}
