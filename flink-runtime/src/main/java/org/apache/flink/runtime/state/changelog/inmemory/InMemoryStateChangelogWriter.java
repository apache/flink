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
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

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
    public void append(int keyGroup, byte[] value) {
        Preconditions.checkState(!closed, "LogWriter is closed");
        LOG.trace("append, keyGroup={}, {} bytes", keyGroup, value.length);
        sqn = sqn.next();
        changesByKeyGroup.computeIfAbsent(keyGroup, unused -> new TreeMap<>()).put(sqn, value);
    }

    @Override
    public SequenceNumber initialSequenceNumber() {
        return INITIAL_SQN;
    }

    @Override
    public SequenceNumber lastAppendedSequenceNumber() {
        return sqn;
    }

    @Override
    public CompletableFuture<InMemoryChangelogStateHandle> persist(SequenceNumber from) {
        LOG.debug("Persist after {}", from);
        Preconditions.checkNotNull(from);
        return completedFuture(
                new InMemoryChangelogStateHandle(collectChanges(from), from, sqn, keyGroupRange));
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
        return changeMap.tailMap(after, true).entrySet().stream()
                .map(e2 -> Tuple2.of(e2.getKey(), new StateChange(keyGroup, e2.getValue())));
    }

    @Override
    public void close() {
        Preconditions.checkState(!closed);
        closed = true;
    }

    @Override
    public void truncate(SequenceNumber before) {
        changesByKeyGroup.forEach(
                (kg, changesBySqn) -> changesBySqn.headMap(before, false).clear());
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to) {}

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {}
}
