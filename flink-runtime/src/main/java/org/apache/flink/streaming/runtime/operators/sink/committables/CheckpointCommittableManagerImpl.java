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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

class CheckpointCommittableManagerImpl<CommT> implements CheckpointCommittableManager<CommT> {
    /** Mapping of subtask id to {@link SubtaskCommittableManager}. */
    private final Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers;

    private final long checkpointId;
    private final int numberOfSubtasks;
    private final SinkCommitterMetricGroup metricGroup;

    private static final Logger LOG =
            LoggerFactory.getLogger(CheckpointCommittableManagerImpl.class);

    @VisibleForTesting
    CheckpointCommittableManagerImpl(
            Map<Integer, SubtaskCommittableManager<CommT>> subtasksCommittableManagers,
            int numberOfSubtasks,
            long checkpointId,
            SinkCommitterMetricGroup metricGroup) {
        this.subtasksCommittableManagers = checkNotNull(subtasksCommittableManagers);
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.metricGroup = metricGroup;
    }

    public static <CommT> CheckpointCommittableManagerImpl<CommT> forSummary(
            CommittableSummary<CommT> summary, SinkCommitterMetricGroup metricGroup) {
        return new CheckpointCommittableManagerImpl<>(
                new HashMap<>(),
                summary.getNumberOfSubtasks(),
                summary.getCheckpointIdOrEOI(),
                metricGroup);
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    Collection<SubtaskCommittableManager<CommT>> getSubtaskCommittableManagers() {
        return subtasksCommittableManagers.values();
    }

    void addSummary(CommittableSummary<CommT> summary) {
        long checkpointId = summary.getCheckpointIdOrEOI();
        SubtaskCommittableManager<CommT> manager =
                new SubtaskCommittableManager<>(
                        summary.getNumberOfCommittables(),
                        summary.getSubtaskId(),
                        checkpointId,
                        metricGroup);
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
    public boolean isFinished() {
        return subtasksCommittableManagers.values().stream()
                .allMatch(SubtaskCommittableManager::isFinished);
    }

    @Override
    public boolean hasGloballyReceivedAll() {
        return subtasksCommittableManagers.size() == numberOfSubtasks
                && subtasksCommittableManagers.values().stream()
                        .allMatch(SubtaskCommittableManager::hasReceivedAll);
    }

    @Override
    public void commit(Committer<CommT> committer, int maxRetries)
            throws IOException, InterruptedException {
        Collection<CommitRequestImpl<CommT>> requests =
                getPendingRequests().collect(Collectors.toList());
        for (int retry = 0; !requests.isEmpty() && retry <= maxRetries; retry++) {
            requests.forEach(CommitRequestImpl::setSelected);
            committer.commit(Collections.unmodifiableCollection(requests));
            requests.forEach(CommitRequestImpl::setCommittedIfNoError);
            requests = requests.stream().filter(r -> !r.isFinished()).collect(Collectors.toList());
        }
        if (!requests.isEmpty()) {
            throw new IOException(
                    String.format(
                            "Failed to commit %s committables after %s retries: %s",
                            requests.size(), maxRetries, requests));
        }
    }

    @Override
    public Collection<CommT> getSuccessfulCommittables() {
        return subtasksCommittableManagers.values().stream()
                .flatMap(SubtaskCommittableManager::getSuccessfulCommittables)
                .collect(Collectors.toList());
    }

    @Override
    public int getNumFailed() {
        return subtasksCommittableManagers.values().stream()
                .mapToInt(SubtaskCommittableManager::getNumFailed)
                .sum();
    }

    Stream<CommitRequestImpl<CommT>> getPendingRequests() {
        return subtasksCommittableManagers.values().stream()
                .peek(this::assertReceivedAll)
                .flatMap(SubtaskCommittableManager::getPendingRequests);
    }

    /**
     * For committers: Sinks don't use unaligned checkpoints, so we receive all committables of a
     * given upstream task before the respective barrier. Thus, when the barrier reaches the
     * committer, all committables of a specific checkpoint must have been received. Committing
     * happens even later on notifyCheckpointComplete.
     *
     * <p>Global committers need to ensure that all committables of all subtasks have been received
     * with {@link #hasGloballyReceivedAll()} before trying to commit. Naturally, this method then
     * becomes a no-op.
     *
     * <p>Note that by transitivity, the assertion also holds for committables of subsumed
     * checkpoints.
     *
     * <p>This assertion will fail in case of bugs in the writer or in the pre-commit topology if
     * present.
     */
    private void assertReceivedAll(SubtaskCommittableManager<CommT> subtask) {
        Preconditions.checkArgument(
                subtask.hasReceivedAll(),
                "Trying to commit incomplete batch of committables subtask=%s, manager=%s",
                subtask.getSubtaskId(),
                this);
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
                numberOfSubtasks,
                checkpointId,
                metricGroup);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckpointCommittableManagerImpl<?> that = (CheckpointCommittableManagerImpl<?>) o;
        return checkpointId == that.checkpointId
                && numberOfSubtasks == that.numberOfSubtasks
                && Objects.equals(subtasksCommittableManagers, that.subtasksCommittableManagers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtasksCommittableManagers, checkpointId, numberOfSubtasks);
    }

    @Override
    public String toString() {
        return "CheckpointCommittableManagerImpl{"
                + "numberOfSubtasks="
                + numberOfSubtasks
                + ", checkpointId="
                + checkpointId
                + ", subtasksCommittableManagers="
                + subtasksCommittableManagers
                + '}';
    }
}
