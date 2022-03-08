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
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

class CheckpointCommittableManagerImpl<CommT> implements CheckpointCommittableManager<CommT> {
    /** Mapping of subtask id to {@link SubtaskCommittableManager}. */
    private final Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers;

    @Nullable private final Long checkpointId;
    private final int subtaskId;
    private final int numberOfSubtasks;

    CheckpointCommittableManagerImpl(
            int subtaskId, int numberOfSubtasks, @Nullable Long checkpointId) {
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.subtasksCommittableManagers = new HashMap<>();
    }

    CheckpointCommittableManagerImpl(
            Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers,
            @Nullable Long checkpointId) {
        this.subtasksCommittableManagers = checkNotNull(subtasksCommittableManagers);
        this.subtaskId = 0;
        this.numberOfSubtasks = 1;
        this.checkpointId = checkpointId;
    }

    @Override
    public long getCheckpointId() {
        checkNotNull(checkpointId);
        return checkpointId;
    }

    Collection<SubtaskCommittableManager<CommT>> getSubtaskCommittableManagers() {
        return subtasksCommittableManagers.values();
    }

    void upsertSummary(CommittableSummary<CommT> summary) {
        SubtaskCommittableManager<CommT> existing =
                subtasksCommittableManagers.putIfAbsent(
                        summary.getSubtaskId(),
                        new SubtaskCommittableManager<>(
                                summary.getNumberOfCommittables(),
                                subtaskId,
                                summary.getCheckpointId().isPresent()
                                        ? summary.getCheckpointId().getAsLong()
                                        : null));
        if (existing != null) {
            throw new UnsupportedOperationException(
                    "Currently it is not supported to update the CommittableSummary for a checkpoint coming from the same subtask. Please check the status of FLINK-25920");
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
    public Collection<CommittableWithLineage<CommT>> commit(
            boolean fullyReceived, Committer<CommT> committer)
            throws IOException, InterruptedException {
        Collection<CommitRequestImpl<CommT>> requests = getPendingRequests(fullyReceived);
        requests.forEach(CommitRequestImpl::setSelected);
        committer.commit(new ArrayList<>(requests));
        requests.forEach(CommitRequestImpl::setCommittedIfNoError);
        return drainFinished();
    }

    Collection<CommitRequestImpl<CommT>> getPendingRequests(boolean fullyReceived) {
        return subtasksCommittableManagers.values().stream()
                .filter(subtask -> !fullyReceived || subtask.hasReceivedAll())
                .flatMap(SubtaskCommittableManager::getPendingRequests)
                .collect(Collectors.toList());
    }

    Collection<CommittableWithLineage<CommT>> drainFinished() {
        return subtasksCommittableManagers.values().stream()
                .flatMap(subtask -> subtask.drainCommitted().stream())
                .collect(Collectors.toList());
    }

    CheckpointCommittableManagerImpl<CommT> merge(CheckpointCommittableManagerImpl<CommT> other) {
        checkArgument(Objects.equals(other.checkpointId, checkpointId));
        for (Map.Entry<Integer, SubtaskCommittableManager<CommT>> subtaskEntry :
                other.subtasksCommittableManagers.entrySet()) {
            subtasksCommittableManagers.merge(
                    subtaskEntry.getKey(),
                    subtaskEntry.getValue(),
                    SubtaskCommittableManager::merge);
        }
        return this;
    }

    CheckpointCommittableManagerImpl<CommT> copy() {
        return new CheckpointCommittableManagerImpl<>(
                subtasksCommittableManagers.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().copy())),
                checkpointId);
    }
}
