/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.flink.runtime.state.changelog.StateChange.META_KEY_GROUP;

@NotThreadSafe
class InMemoryStateChangelogWriter implements StateChangelogWriter<InMemoryChangelogStateHandle> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryStateChangelogWriter.class);
    private static final SequenceNumber INITIAL_SQN = SequenceNumber.of(0L);

    private final Map<Integer, NavigableMap<SequenceNumber, byte[]>> changesByKeyGroup =
            new HashMap<>();
    private final KeyGroupRange keyGroupRange;
    private SequenceNumber sqn = INITIAL_SQN;
    private boolean closed;

    public InMemoryStateChangelogWriter(KeyGroupRange keyGroupRange) {
        this.keyGroupRange = keyGroupRange;
    }

    @Override
    public void appendMeta(byte[] value) throws IOException {
        LOG.trace("append metadata: {} bytes", value.length);
        if (closed) {
            LOG.warn("LogWriter is closed.");
            return;
        }
        changesByKeyGroup
                .computeIfAbsent(META_KEY_GROUP, unused -> new TreeMap<>())
                .put(sqn, value);
        sqn = sqn.next();
    }

    @Override
    public void append(int keyGroup, byte[] value) {
        LOG.trace("append, keyGroup={}, {} bytes", keyGroup, value.length);
        if (closed) {
            LOG.warn("LogWriter is closed.");
            return;
        }
        changesByKeyGroup.computeIfAbsent(keyGroup, unused -> new TreeMap<>()).put(sqn, value);
        sqn = sqn.next();
    }

    @Override
    public SequenceNumber initialSequenceNumber() {
        return INITIAL_SQN;
    }

    @Override
    public SequenceNumber nextSequenceNumber() {
        return sqn;
    }

    @Override
    public CompletableFuture<SnapshotResult<InMemoryChangelogStateHandle>> persist(
            SequenceNumber from, long checkpointId) {
        LOG.debug("Persist after {}", from);
        Preconditions.checkNotNull(from);
        return completedFuture(
                SnapshotResult.of(
                        new InMemoryChangelogStateHandle(
                                collectChanges(from), from, sqn, keyGroupRange)));
    }

    private List<StateChange> collectChanges(SequenceNumber after) {
        return changesByKeyGroup.entrySet().stream()
                .flatMap(e -> toChangeStream(e.getValue(), after, e.getKey()))
                .sorted(Comparator.comparing(sqnAndChange -> sqnAndChange.f0))
                .map(t -> t.f1)
                .collect(Collectors.toList());
    }

    private Stream<Tuple2<SequenceNumber, StateChange>> toChangeStream(
            NavigableMap<SequenceNumber, byte[]> changeMap, SequenceNumber after, int keyGroup) {
        if (keyGroup == META_KEY_GROUP) {
            return changeMap.tailMap(after, true).entrySet().stream()
                    .map(e2 -> Tuple2.of(e2.getKey(), StateChange.ofMetadataChange(e2.getValue())));
        }
        return changeMap.tailMap(after, true).entrySet().stream()
                .map(
                        e2 ->
                                Tuple2.of(
                                        e2.getKey(),
                                        StateChange.ofDataChange(keyGroup, e2.getValue())));
    }

    @Override
    public void close() {
        Preconditions.checkState(!closed);
        closed = true;
    }

    @Override
    public void truncate(SequenceNumber to) {
        changesByKeyGroup.forEach((kg, changesBySqn) -> changesBySqn.headMap(to, false).clear());
    }

    @Override
    public void truncateAndClose(SequenceNumber from) {
        close();
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to, long checkpointID) {}

    @Override
    public void reset(SequenceNumber from, SequenceNumber to, long checkpointID) {}
}
