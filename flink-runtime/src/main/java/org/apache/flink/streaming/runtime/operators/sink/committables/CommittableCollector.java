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
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible to book-keep the committing progress across checkpoints and upstream
 * subtasks.
 *
 * <p>Each checkpoint in turn is handled by a {@link CheckpointCommittableManager}.
 *
 * @param <CommT> type of committable
 */
@Internal
public class CommittableCollector<CommT> {
    private static final long EOI = Long.MAX_VALUE;
    /** Mapping of checkpoint id to {@link CheckpointCommittableManagerImpl}. */
    private final NavigableMap<Long, CheckpointCommittableManagerImpl<CommT>>
            checkpointCommittables;

    private final SinkCommitterMetricGroup metricGroup;

    public CommittableCollector(SinkCommitterMetricGroup metricGroup) {
        this(new TreeMap<>(), metricGroup);
    }

    /** For deep-copy. */
    CommittableCollector(
            Map<Long, CheckpointCommittableManagerImpl<CommT>> checkpointCommittables,
            SinkCommitterMetricGroup metricGroup) {
        this.checkpointCommittables = new TreeMap<>(checkNotNull(checkpointCommittables));
        this.metricGroup = metricGroup;
        this.metricGroup.setCurrentPendingCommittablesGauge(this::getNumPending);
    }

    private int getNumPending() {
        return checkpointCommittables.values().stream()
                .mapToInt(m -> (int) m.getPendingRequests().count())
                .sum();
    }

    /**
     * Creates a {@link CommittableCollector} based on the current runtime information. This method
     * should be used for to instantiate a collector for all Sink V2.
     *
     * @param metricGroup storing the committable metrics
     * @param <CommT> type of the committable
     * @return {@link CommittableCollector}
     */
    public static <CommT> CommittableCollector<CommT> of(SinkCommitterMetricGroup metricGroup) {
        return new CommittableCollector<>(metricGroup);
    }

    /**
     * Creates a {@link CommittableCollector} for a list of committables. This method is mainly used
     * to create a collector from the state of Sink V1.
     *
     * @param committables list of committables
     * @param metricGroup storing the committable metrics
     * @param <CommT> type of committables
     * @return {@link CommittableCollector}
     */
    static <CommT> CommittableCollector<CommT> ofLegacy(
            List<CommT> committables, SinkCommitterMetricGroup metricGroup) {
        CommittableCollector<CommT> committableCollector = new CommittableCollector<>(metricGroup);
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
        return new ArrayList<>(checkpointCommittables.headMap(checkpointId, true).values());
    }

    /**
     * Returns {@link CheckpointCommittableManager} belonging to the last input.
     *
     * @return {@link CheckpointCommittableManager}
     */
    public Optional<CheckpointCommittableManager<CommT>> getEndOfInputCommittable() {
        return Optional.ofNullable(checkpointCommittables.get(EOI));
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
     * Returns a new committable collector that deep copies all internals.
     *
     * @return {@link CommittableCollector}
     */
    public CommittableCollector<CommT> copy() {
        return new CommittableCollector<>(
                checkpointCommittables.entrySet().stream()
                        .map(e -> Tuple2.of(e.getKey(), e.getValue().copy()))
                        .collect(Collectors.toMap((t) -> t.f0, (t) -> t.f1)),
                metricGroup);
    }

    Collection<CheckpointCommittableManagerImpl<CommT>> getCheckpointCommittables() {
        return checkpointCommittables.values();
    }

    private void addSummary(CommittableSummary<CommT> summary) {
        checkpointCommittables
                .computeIfAbsent(
                        summary.getCheckpointIdOrEOI(),
                        key -> CheckpointCommittableManagerImpl.forSummary(summary, metricGroup))
                .addSummary(summary);
    }

    private void addCommittable(CommittableWithLineage<CommT> committable) {
        getCheckpointCommittables(committable).addCommittable(committable);
    }

    private CheckpointCommittableManagerImpl<CommT> getCheckpointCommittables(
            CommittableMessage<CommT> committable) {
        CheckpointCommittableManagerImpl<CommT> committables =
                this.checkpointCommittables.get(committable.getCheckpointIdOrEOI());
        return checkNotNull(committables, "Unknown checkpoint for %s", committable);
    }

    /** Removes the manager for a specific checkpoint and all it's metadata. */
    public void remove(CheckpointCommittableManager<CommT> manager) {
        checkpointCommittables.remove(manager.getCheckpointId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommittableCollector<?> that = (CommittableCollector<?>) o;
        return Objects.equals(checkpointCommittables, that.checkpointCommittables);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(checkpointCommittables);
    }

    @Override
    public String toString() {
        return "CommittableCollector{" + "checkpointCommittables=" + checkpointCommittables + '}';
    }
}
