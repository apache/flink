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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/** A mock {@link SplitEnumerator} for unit tests. */
public class MockSplitEnumerator implements SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> {
    private final SortedSet<MockSourceSplit> unassignedSplits;
    private final SplitEnumeratorContext<MockSourceSplit> enumContext;
    private final List<SourceEvent> handledSourceEvent;
    private final List<Long> successfulCheckpoints;
    private volatile boolean started;
    private volatile boolean closed;

    public MockSplitEnumerator(int numSplits, SplitEnumeratorContext<MockSourceSplit> enumContext) {
        this(new HashSet<>(), enumContext);
        for (int i = 0; i < numSplits; i++) {
            unassignedSplits.add(new MockSourceSplit(i));
        }
    }

    public MockSplitEnumerator(
            Set<MockSourceSplit> unassignedSplits,
            SplitEnumeratorContext<MockSourceSplit> enumContext) {
        this.unassignedSplits =
                new TreeSet<>(Comparator.comparingInt(o -> Integer.parseInt(o.splitId())));
        this.unassignedSplits.addAll(unassignedSplits);
        this.enumContext = enumContext;
        this.handledSourceEvent = new ArrayList<>();
        this.successfulCheckpoints = new ArrayList<>();
        this.started = false;
        this.closed = false;
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
        unassignedSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        List<MockSourceSplit> assignment = new ArrayList<>();
        for (MockSourceSplit split : unassignedSplits) {
            if (Integer.parseInt(split.splitId()) % enumContext.currentParallelism() == subtaskId) {
                assignment.add(split);
            }
        }
        enumContext.assignSplits(
                new SplitsAssignment<>(Collections.singletonMap(subtaskId, assignment)));
        unassignedSplits.removeAll(assignment);
    }

    @Override
    public Set<MockSourceSplit> snapshotState() {
        return unassignedSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        successfulCheckpoints.add(checkpointId);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    public void addNewSplits(List<MockSourceSplit> newSplits) {
        unassignedSplits.addAll(newSplits);
        assignAllSplits();
    }

    // --------------------

    public boolean started() {
        return started;
    }

    public boolean closed() {
        return closed;
    }

    public Set<MockSourceSplit> getUnassignedSplits() {
        return unassignedSplits;
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
        unassignedSplits.forEach(
                split -> {
                    int subtaskId =
                            Integer.parseInt(split.splitId()) % enumContext.currentParallelism();
                    if (enumContext.registeredReaders().containsKey(subtaskId)) {
                        assignment
                                .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                                .add(split);
                    }
                });
        enumContext.assignSplits(new SplitsAssignment<>(assignment));
        assignment.values().forEach(l -> unassignedSplits.removeAll(l));
    }
}
