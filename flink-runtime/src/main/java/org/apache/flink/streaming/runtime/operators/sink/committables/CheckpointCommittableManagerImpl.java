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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

class CheckpointCommittableManagerImpl<CommT> implements CheckpointCommittableManager<CommT> {
    /** Mapping of subtask id to {@link SubtaskCommittableManager}. */
    private final Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers;

    private final long checkpointId;
    private final int subtaskId;
    private final int numberOfSubtasks;
    private final SinkCommitterMetricGroup metricGroup;

    private static final Logger LOG =
            LoggerFactory.getLogger(CheckpointCommittableManagerImpl.class);

    CheckpointCommittableManagerImpl(
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            SinkCommitterMetricGroup metricGroup) {
        this(new HashMap<>(), subtaskId, numberOfSubtasks, checkpointId, metricGroup);
    }

    CheckpointCommittableManagerImpl(
            Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers,
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            SinkCommitterMetricGroup metricGroup) {
        this.subtasksCommittableManagers = checkNotNull(subtasksCommittableManagers);
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.metricGroup = metricGroup;
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    Collection<SubtaskCommittableManager<CommT>> getSubtaskCommittableManagers() {
        return subtasksCommittableManagers.values();
    }

    void addSummary(CommittableSummary<CommT> summary) {
        long checkpointId = summary.getCheckpointIdOrEOI();
        SubtaskCommittableManager<CommT> manager =
                new SubtaskCommittableManager<>(
                        summary.getNumberOfCommittables(), subtaskId, checkpointId, metricGroup);
        if (checkpointId == CommittableMessage.EOI) {
            SubtaskCommittableManager<CommT> merged =
                    subtasksCommittableManagers.merge(
                            summary.getSubtaskId(), manager, SubtaskCommittableManager::merge);
            LOG.debug("Adding EOI summary (new={}}, merged={}}).", manager, merged);
        } else {
            SubtaskCommittableManager<CommT> existing =
                    subtasksCommittableManagers.putIfAbsent(summary.getSubtaskId(), manager);
            if (existing != null) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Received duplicate committable summary for checkpoint %s + subtask %s (new=%s, old=%s). Please check the status of FLINK-25920",
                                checkpointId, summary.getSubtaskId(), manager, existing));
            } else {
                LOG.debug(
                        "Setting the summary for checkpointId {} with {}",
                        this.checkpointId,
                        manager);
            }
        }
    }

    void addCommittable(CommittableWithLineage<CommT> committable) {
        getSubtaskCommittableManager(committable.getSubtaskId()).add(committable);
    }

    SubtaskCommittableManager<CommT> getSubtaskCommittableManager(int subtaskId) {
        SubtaskCommittableManager<CommT> committables =
                this.subtasksCommittableManagers.get(subtaskId);
        return checkNotNull(committables, "Unknown subtask for %s", subtaskId);
    }

    @Override
    public CommittableSummary<CommT> getSummary() {
        return new CommittableSummary<>(
                subtaskId,
                numberOfSubtasks,
                checkpointId,
                subtasksCommittableManagers.values().stream()
                        .mapToInt(SubtaskCommittableManager::getNumCommittables)
                        .sum(),
                subtasksCommittableManagers.values().stream()
                        .mapToInt(SubtaskCommittableManager::getNumPending)
                        .sum(),
                subtasksCommittableManagers.values().stream()
                        .mapToInt(SubtaskCommittableManager::getNumFailed)
                        .sum());
    }

    boolean isFinished() {
        return subtasksCommittableManagers.values().stream()
                .allMatch(SubtaskCommittableManager::isFinished);
    }

    @Override
    public Collection<CommittableWithLineage<CommT>> commit(Committer<CommT> committer)
            throws IOException, InterruptedException {
        Collection<CommitRequestImpl<CommT>> requests = getPendingRequests(true);
        requests.forEach(CommitRequestImpl::setSelected);
        committer.commit(new ArrayList<>(requests));
        requests.forEach(CommitRequestImpl::setCommittedIfNoError);
        Collection<CommittableWithLineage<CommT>> committed = drainFinished();
        metricGroup.setCurrentPendingCommittablesGauge(() -> getPendingRequests(false).size());
        return committed;
    }

    Collection<CommitRequestImpl<CommT>> getPendingRequests(boolean onlyIfFullyReceived) {
        return subtasksCommittableManagers.values().stream()
                .filter(subtask -> !onlyIfFullyReceived || subtask.hasReceivedAll())
                .flatMap(SubtaskCommittableManager::getPendingRequests)
                .collect(Collectors.toList());
    }

    Collection<CommittableWithLineage<CommT>> drainFinished() {
        return subtasksCommittableManagers.values().stream()
                .flatMap(subtask -> subtask.drainCommitted().stream())
                .collect(Collectors.toList());
    }

    CheckpointCommittableManagerImpl<CommT> merge(CheckpointCommittableManagerImpl<CommT> other) {
        checkArgument(other.checkpointId == checkpointId);
        CheckpointCommittableManagerImpl<CommT> merged = copy();
        for (Map.Entry<Integer, SubtaskCommittableManager<CommT>> subtaskEntry :
                other.subtasksCommittableManagers.entrySet()) {
            merged.subtasksCommittableManagers.merge(
                    subtaskEntry.getKey(),
                    subtaskEntry.getValue(),
                    SubtaskCommittableManager::merge);
        }
        return merged;
    }

    CheckpointCommittableManagerImpl<CommT> copy() {
        return new CheckpointCommittableManagerImpl<>(
                subtasksCommittableManagers.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().copy())),
                subtaskId,
                numberOfSubtasks,
                checkpointId,
                metricGroup);
    }
}
