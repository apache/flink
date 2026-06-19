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

package org.apache.flink.api.connector.source.mocks;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.SupportsBatchSnapshot;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A mock {@link SplitEnumerator} for unit tests. */
public class MockSplitEnumerator
        implements SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>>, SupportsBatchSnapshot {
    private final Map<Integer, Set<MockSourceSplit>> pendingSplitAssignment;
    private final Map<String, Integer> globalSplitAssignment;
    private final SplitEnumeratorContext<MockSourceSplit> enumContext;
    private final List<SourceEvent> handledSourceEvent;
    private final List<Long> successfulCheckpoints;
    private volatile boolean started;
    private volatile boolean closed;

    public MockSplitEnumerator(int numSplits, SplitEnumeratorContext<MockSourceSplit> enumContext) {
        this(new HashSet<>(), enumContext);
        List<MockSourceSplit> unassignedSplits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            unassignedSplits.add(new MockSourceSplit(i));
        }
        recalculateAssignments(unassignedSplits);
    }

    public MockSplitEnumerator(
            Set<MockSourceSplit> unassignedSplits,
            SplitEnumeratorContext<MockSourceSplit> enumContext) {
        this.pendingSplitAssignment = new HashMap<>();
        this.globalSplitAssignment = new HashMap<>();
        this.enumContext = enumContext;
        this.handledSourceEvent = new ArrayList<>();
        this.successfulCheckpoints = new ArrayList<>();
        this.started = false;
        this.closed = false;
        recalculateAssignments(unassignedSplits);
    }

    @Override
    public void start() {
        this.started = true;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        handledSourceEvent.add(sourceEvent);
    }

    @Override
    public void addSplitsBack(List<MockSourceSplit> splits, int subtaskId) {
        // add back to same subtaskId.
        putPendingAssignments(subtaskId, splits);
    }

    @Override
    public void addReader(int subtaskId) {
        ReaderInfo readerInfo = enumContext.registeredReaders().get(subtaskId);
        List<MockSourceSplit> splitsOnRecovery = readerInfo.getReportedSplitsOnRegistration();

        List<MockSourceSplit> redistributedSplits = new ArrayList<>();
        List<MockSourceSplit> addBackSplits = new ArrayList<>();
        for (MockSourceSplit split : splitsOnRecovery) {
            if (!globalSplitAssignment.containsKey(split.splitId())) {
                // if the split is not present in globalSplitAssignment, it means that this split is
                // being registered for the first time and is eligible for redistribution.
                redistributedSplits.add(split);
            } else if (!globalSplitAssignment.containsKey(split.splitId())) {
                //  if split is already assigned to other sub-task, just ignore it. Otherwise, add
                // back to this sub-task again.
                addBackSplits.add(split);
            }
        }
        recalculateAssignments(redistributedSplits);
        putPendingAssignments(subtaskId, addBackSplits);
        assignAllSplits();
    }

    @Override
    public Set<MockSourceSplit> snapshotState(long checkpointId) {
        return getUnassignedSplits();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        successfulCheckpoints.add(checkpointId);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    // --------------------

    public boolean started() {
        return started;
    }

    public boolean closed() {
        return closed;
    }

    public Set<MockSourceSplit> getUnassignedSplits() {
        return pendingSplitAssignment.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public List<SourceEvent> getHandledSourceEvent() {
        return handledSourceEvent;
    }

    public List<Long> getSuccessfulCheckpoints() {
        return successfulCheckpoints;
    }

    // --------------------

    private void assignAllSplits() {
        Map<Integer, List<MockSourceSplit>> assignment = new HashMap<>();
        for (Map.Entry<Integer, Set<MockSourceSplit>> iter : pendingSplitAssignment.entrySet()) {
            Integer subtaskId = iter.getKey();
            if (enumContext.registeredReaders().containsKey(subtaskId)) {
                assignment.put(subtaskId, new ArrayList<>(iter.getValue()));
            }
        }
        enumContext.assignSplits(new SplitsAssignment<>(assignment));
        assignment.keySet().forEach(pendingSplitAssignment::remove);
    }

    private void recalculateAssignments(Collection<MockSourceSplit> newSplits) {
        for (MockSourceSplit split : newSplits) {
            int subtaskId = Integer.parseInt(split.splitId()) % enumContext.currentParallelism();
            putPendingAssignments(subtaskId, Collections.singletonList(split));
        }
    }

    private void putPendingAssignments(int subtaskId, Collection<MockSourceSplit> splits) {
        Set<MockSourceSplit> pendingSplits =
                pendingSplitAssignment.computeIfAbsent(subtaskId, HashSet::new);
        pendingSplits.addAll(splits);
        splits.forEach(split -> globalSplitAssignment.put(split.splitId(), subtaskId));
    }
}
