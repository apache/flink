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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible to book-keep the committing progress across checkpoints and subtasks.
 * It handles the emission of committables and the {@link CommittableSummary}.
 *
 * @param <CommT> type of committable
 */
@Internal
public class CommittableCollector<CommT> {
    private static final long EOI = Long.MAX_VALUE;
    /** Mapping of checkpoint id to {@link CheckpointCommittableManagerImpl}. */
    private final NavigableMap<Long, CheckpointCommittableManagerImpl<CommT>>
            checkpointCommittables;
    /** Denotes the subtask id the collector is running. */
    private final int subtaskId;

    private final int numberOfSubtasks;

    public CommittableCollector(int subtaskId, int numberOfSubtasks) {
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointCommittables = new TreeMap<>();
    }

    /** For deep-copy. */
    CommittableCollector(
            Map<Long, CheckpointCommittableManagerImpl<CommT>> checkpointCommittables,
            int subtaskId,
            int numberOfSubtasks) {
        this.checkpointCommittables = new TreeMap<>(checkNotNull(checkpointCommittables));
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
    }

    /**
     * Creates a {@link CommittableCollector} based on the current runtime information. This method
     * should be used for to instantiate a collector for all Sink V2.
     *
     * @param context holding runtime of information
     * @param <CommT> type of the committable
     * @return {@link CommittableCollector}
     */
    public static <CommT> CommittableCollector<CommT> of(RuntimeContext context) {
        return new CommittableCollector<>(
                context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    /**
     * Creates a {@link CommittableCollector} for a list of committables. This method is mainly used
     * to create a collector from the state of Sink V1.
     *
     * @param committables list of committables
     * @param <CommT> type of committables
     * @return {@link CommittableCollector}
     */
    static <CommT> CommittableCollector<CommT> ofLegacy(List<CommT> committables) {
        CommittableCollector<CommT> committableCollector = new CommittableCollector<>(0, 1);
        // add a checkpoint with the lowest checkpoint id, this will be merged into the next
        // checkpoint data, subtask id is arbitrary
        CommittableSummary<CommT> summary =
                new CommittableSummary<>(
                        0,
                        1,
                        InitContext.INITIAL_CHECKPOINT_ID,
                        committables.size(),
                        committables.size(),
                        0);
        committableCollector.addSummary(summary);
        committables.forEach(
                c -> {
                    final CommittableWithLineage<CommT> committableWithLineage =
                            new CommittableWithLineage<>(c, InitContext.INITIAL_CHECKPOINT_ID, 0);
                    committableCollector.addCommittable(committableWithLineage);
                });
        return committableCollector;
    }

    /**
     * Adds a {@link CommittableMessage} to the collector to hold it until emission.
     *
     * @param message either {@link CommittableSummary} or {@link CommittableWithLineage}
     */
    public void addMessage(CommittableMessage<CommT> message) {
        if (message instanceof CommittableSummary) {
            addSummary((CommittableSummary<CommT>) message);
        } else if (message instanceof CommittableWithLineage) {
            addCommittable((CommittableWithLineage<CommT>) message);
        }
    }

    /**
     * Returns all {@link CheckpointCommittableManager} until the requested checkpoint id.
     *
     * @param checkpointId counter
     * @return collection of {@link CheckpointCommittableManager}
     */
    public Collection<? extends CheckpointCommittableManager<CommT>> getCheckpointCommittablesUpTo(
            long checkpointId) {
        // clean up fully committed previous checkpoints
        // this wouldn't work with concurrent unaligned checkpoints
        Collection<CheckpointCommittableManagerImpl<CommT>> checkpoints =
                checkpointCommittables.headMap(checkpointId, true).values();
        checkpoints.removeIf(CheckpointCommittableManagerImpl::isFinished);
        return checkpoints;
    }

    /**
     * Returns {@link CheckpointCommittableManager} that is currently hold by the collector and
     * associated with the {@link CommittableCollector#EOI} checkpoint id.
     *
     * @return {@link CheckpointCommittableManager}
     */
    @Nullable
    public CommittableManager<CommT> getEndOfInputCommittable() {
        return checkpointCommittables.get(EOI);
    }

    /**
     * Returns whether all {@link CheckpointCommittableManager} currently hold by the collector are
     * either committed or failed.
     *
     * @return state of the {@link CheckpointCommittableManager}
     */
    public boolean isFinished() {
        return checkpointCommittables.values().stream()
                .allMatch(CheckpointCommittableManagerImpl::isFinished);
    }

    /**
     * Merges all information from an external collector into this collector.
     *
     * <p>This method is important during recovery from existing state.
     *
     * @param cc other {@link CommittableCollector}
     */
    public void merge(CommittableCollector<CommT> cc) {
        for (Entry<Long, CheckpointCommittableManagerImpl<CommT>> checkpointEntry :
                cc.checkpointCommittables.entrySet()) {
            checkpointCommittables.merge(
                    checkpointEntry.getKey(),
                    checkpointEntry.getValue(),
                    CheckpointCommittableManagerImpl::merge);
        }
    }

    /**
     * Returns number of subtasks.
     *
     * @return number of subtasks
     */
    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    /**
     * Returns subtask id.
     *
     * @return subtask id.
     */
    public int getSubtaskId() {
        return subtaskId;
    }

    /**
     * Returns a new committable collector that deep copies all internals.
     *
     * @return {@link CommittableCollector}
     */
    public CommittableCollector<CommT> copy() {
        return new CommittableCollector<>(
                checkpointCommittables.entrySet().stream()
                        .map(e -> Tuple2.of(e.getKey(), e.getValue().copy()))
                        .collect(Collectors.toMap((t) -> t.f0, (t) -> t.f1)),
                subtaskId,
                numberOfSubtasks);
    }

    Collection<CheckpointCommittableManagerImpl<CommT>> getCheckpointCommittables() {
        return checkpointCommittables.values();
    }

    private void addSummary(CommittableSummary<CommT> summary) {
        checkpointCommittables
                .computeIfAbsent(
                        summary.getCheckpointId().orElse(EOI),
                        key ->
                                new CheckpointCommittableManagerImpl<>(
                                        subtaskId,
                                        numberOfSubtasks,
                                        summary.getCheckpointId().orElse(EOI)))
                .upsertSummary(summary);
    }

    private void addCommittable(CommittableWithLineage<CommT> committable) {
        getCheckpointCommittables(committable).addCommittable(committable);
    }

    private CheckpointCommittableManagerImpl<CommT> getCheckpointCommittables(
            CommittableMessage<CommT> committable) {
        CheckpointCommittableManagerImpl<CommT> committables =
                this.checkpointCommittables.get(committable.getCheckpointId().orElse(EOI));
        return checkNotNull(committables, "Unknown checkpoint for %s", committable);
    }
}
