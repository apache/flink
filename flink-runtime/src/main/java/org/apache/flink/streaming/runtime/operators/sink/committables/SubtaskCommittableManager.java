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
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.apache.flink.shaded.guava32.com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Manages the committables coming from one upstream subtask. */
class SubtaskCommittableManager<CommT> {
    private final Deque<CommitRequestImpl<CommT>> requests;
    private final int numExpectedCommittables;
    private final long checkpointId;
    private final int subtaskId;
    private int numDrained;
    private int numFailed;
    private final SinkCommitterMetricGroup metricGroup;

    SubtaskCommittableManager(
            int numExpectedCommittables,
            int subtaskId,
            long checkpointId,
            SinkCommitterMetricGroup metricGroup) {
        this(
                Collections.emptyList(),
                numExpectedCommittables,
                0,
                0,
                subtaskId,
                checkpointId,
                metricGroup);
    }

    SubtaskCommittableManager(
            Collection<CommitRequestImpl<CommT>> requests,
            int numExpectedCommittables,
            int numDrained,
            int numFailed,
            int subtaskId,
            long checkpointId,
            SinkCommitterMetricGroup metricGroup) {
        this.checkpointId = checkpointId;
        this.subtaskId = subtaskId;
        this.numExpectedCommittables = numExpectedCommittables;
        this.requests = new ArrayDeque<>(checkNotNull(requests));
        this.numDrained = numDrained;
        this.numFailed = numFailed;
        this.metricGroup = metricGroup;
    }

    void add(CommittableWithLineage<CommT> committable) {
        add(committable.getCommittable());
    }

    void add(CommT committable) {
        checkState(requests.size() < numExpectedCommittables, "Already received all committables.");
        requests.add(new CommitRequestImpl<>(committable, metricGroup));
        metricGroup.getNumCommittablesTotalCounter().inc();
    }

    /**
     * Returns whether the received number of committables matches the expected number.
     *
     * @return if all committables have been received
     */
    boolean hasReceivedAll() {
        return getNumCommittables() == numExpectedCommittables;
    }

    /**
     * Returns the number of committables that has been received so far.
     *
     * @return number of so far received committables
     */
    int getNumCommittables() {
        return requests.size() + numDrained + numFailed;
    }

    /**
     * Returns the number of still expected commits.
     *
     * <p>Either the committables are not yet received or the commit is still pending.
     *
     * @return number of still expected committables
     */
    int getNumPending() {
        return numExpectedCommittables - (numDrained + numFailed);
    }

    int getNumFailed() {
        return numFailed;
    }

    boolean isFinished() {
        return getNumPending() == 0;
    }

    /**
     * Returns a list of {@link CommitRequestImpl} that are not in a final state {@link
     * CommitRequestState#finalState}.
     *
     * @return {@link CommitRequestImpl}
     */
    Stream<CommitRequestImpl<CommT>> getPendingRequests() {
        return requests.stream().filter(c -> !c.isFinished());
    }

    /**
     * Iterates through all currently registered {@link #requests} and returns all {@link
     * CommittableWithLineage} that could be successfully committed.
     *
     * <p>Invoking this method does not yield the same {@link CommittableWithLineage} again. Once
     * retrieved they are not part of {@link #requests} anymore.
     *
     * @return list of {@link CommittableWithLineage}
     */
    List<CommittableWithLineage<CommT>> drainCommitted() {
        List<CommittableWithLineage<CommT>> committed = new ArrayList<>(requests.size());
        for (Iterator<CommitRequestImpl<CommT>> iterator = requests.iterator();
                iterator.hasNext(); ) {
            CommitRequestImpl<CommT> request = iterator.next();
            if (!request.isFinished()) {
                continue;
            }
            if (request.getState() == CommitRequestState.FAILED) {
                numFailed += 1;
                iterator.remove();
                continue;
            } else {
                committed.add(
                        new CommittableWithLineage<>(
                                request.getCommittable(), checkpointId, subtaskId));
            }
            iterator.remove();
        }

        numDrained += committed.size();
        return committed;
    }

    int getNumDrained() {
        return numDrained;
    }

    int getSubtaskId() {
        return subtaskId;
    }

    @VisibleForTesting
    long getCheckpointId() {
        return checkpointId;
    }

    Deque<CommitRequestImpl<CommT>> getRequests() {
        return requests;
    }

    SubtaskCommittableManager<CommT> merge(SubtaskCommittableManager<CommT> other) {
        checkArgument(other.getSubtaskId() == this.getSubtaskId(), "Different subtasks.");
        checkArgument(other.getCheckpointId() == this.getCheckpointId(), "Different checkpoints.");
        return new SubtaskCommittableManager<>(
                Stream.concat(requests.stream(), other.requests.stream())
                        .collect(Collectors.toList()),
                numExpectedCommittables + other.numExpectedCommittables,
                numDrained + other.numDrained,
                numFailed + other.numFailed,
                subtaskId,
                checkpointId,
                metricGroup);
    }

    SubtaskCommittableManager<CommT> copy() {
        return new SubtaskCommittableManager<>(
                requests.stream().map(CommitRequestImpl::copy).collect(Collectors.toList()),
                numExpectedCommittables,
                numDrained,
                numFailed,
                subtaskId,
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
        SubtaskCommittableManager<?> that = (SubtaskCommittableManager<?>) o;
        return numExpectedCommittables == that.numExpectedCommittables
                && checkpointId == that.checkpointId
                && subtaskId == that.subtaskId
                && numDrained == that.numDrained
                && numFailed == that.numFailed
                && Iterables.elementsEqual(requests, that.requests);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                requests, numExpectedCommittables, checkpointId, subtaskId, numDrained, numFailed);
    }

    @Override
    public String toString() {
        return "SubtaskCommittableManager{"
                + "requests="
                + requests
                + ", numExpectedCommittables="
                + numExpectedCommittables
                + ", checkpointId="
                + checkpointId
                + ", subtaskId="
                + subtaskId
                + ", numDrained="
                + numDrained
                + ", numFailed="
                + numFailed
                + '}';
    }
}
