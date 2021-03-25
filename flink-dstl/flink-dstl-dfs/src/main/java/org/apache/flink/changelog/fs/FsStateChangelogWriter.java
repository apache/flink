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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeStore.StoreTask;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.changelog.StateChangelogWriterFactory.ChangelogCallbackExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Basic implementation of {@link StateChangelogWriter}. It's main purpose is to keep track which
 * changes are required for the given {@link #persist(SequenceNumber)} call. Especially it takes
 * care of re-uploading changes from the previous {@link #persist(SequenceNumber)} call, if those
 * changes haven't been yet {@link #confirm(SequenceNumber, SequenceNumber)}'ed. This is crucial as
 * until changes are {@link #confirm(SequenceNumber, SequenceNumber)}, they still can be aborted and
 * removed/deleted.
 *
 * <p>For example if checkpoint N-1 fails and is disposed, after checkpoint N has already started.
 * In this case, when we are persisting {@link StateChangeSet}s for checkpoint N, we need to
 * re-upload {@link StateChangeSet}s that belonged to checkpoint N-1.
 */
@NotThreadSafe
class FsStateChangelogWriter implements StateChangelogWriter<StateChangelogHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogWriter.class);

    private final UUID logId;
    private final KeyGroupRange keyGroupRange;
    private final StateChangeStore store;
    private final NavigableMap<SequenceNumber, StateChangeSet> changeSets = new TreeMap<>();
    private final NavigableMap<SequenceNumber, StoreResult> uploaded = new TreeMap<>();
    private final NavigableMap<SequenceNumber, StoreResult> confirmed = new TreeMap<>();
    private List<StateChange> activeChangeSet = new ArrayList<>();
    private SequenceNumber lastAppendedSequenceNumber = SequenceNumber.of(0L);
    private boolean closed;
    private final ChangelogCallbackExecutor executor;

    FsStateChangelogWriter(
            UUID logId,
            KeyGroupRange keyGroupRange,
            StateChangeStore store,
            ChangelogCallbackExecutor executor) {
        this.logId = logId;
        this.keyGroupRange = keyGroupRange;
        this.store = store;
        this.executor = checkNotNull(executor);
    }

    @Override
    public void append(int keyGroup, byte[] value) {
        LOG.trace("append to {}: keyGroup={} {} bytes", logId, keyGroup, value.length);
        checkState(!closed, "%s is closed", logId);
        activeChangeSet.add(new StateChange(keyGroup, value));
        // size threshold could be added to call persist when reached. considerations:
        // 0. can actually degrade performance by amplifying number of requests
        // 1. which range to persist?
        // 2. how to deal with retries/aborts?
    }

    @Override
    public SequenceNumber lastAppendedSequenceNumber() {
        rollover();
        LOG.trace("query {} sqn: {}", logId, lastAppendedSequenceNumber);
        return lastAppendedSequenceNumber;
    }

    @Override
    public CompletableFuture<StateChangelogHandleStreamImpl> persist(SequenceNumber from)
            throws IOException {
        LOG.debug("persist {} from {}", logId, from);
        checkNotNull(from);
        // todo: check range

        rollover();
        Collection<StoreResult> readyToReturn = confirmed.tailMap(from, true).values();
        Collection<StateChangeSet> toUpload = changeSets.tailMap(from, true).values();
        LOG.debug("collected readyToReturn: {}, toUpload: {}", readyToReturn, toUpload);

        StoreTask task = new StoreTask(toUpload);
        // Handle the result result in supplied executor (mailbox) thread
        CompletableFuture<StateChangelogHandleStreamImpl> resultFuture =
                task.getResultFuture()
                        .thenApplyAsync(
                                (results) -> {
                                    results.forEach(e -> uploaded.put(e.sequenceNumber, e));
                                    return buildHandle(results, readyToReturn);
                                },
                                executor::execute);
        store.save(task);
        return resultFuture;
    }

    @Override
    public void close() {
        LOG.debug("close {}", logId);
        checkState(!closed);
        closed = true;
        activeChangeSet.clear();
        // todo in MVP or later: cleanup if transition succeeded and had non-shared state
        changeSets.clear();
        uploaded.clear();
        confirmed.clear();
        // the store is closed from the owning FsStateChangelogClient
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to) {
        LOG.debug("confirm {} from {} to {}", logId, from, to);
        uploaded.subMap(from, true, to, false)
                .forEach(
                        (sequenceNumber, changeSetAndResult) -> {
                            changeSets.remove(sequenceNumber);
                            uploaded.remove(sequenceNumber);
                            confirmed.put(sequenceNumber, changeSetAndResult);
                        });
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {
        LOG.debug("reset {} from {} to {}", logId, from, to);
        // todo abort uploading?
        // todo in MVP or later: cleanup if change to aborted succeeded and had non-shared state
        // For now, relying on manual cleanup.
        // If a checkpoint that is aborted uses the changes uploaded for another checkpoint
        // which was completed on JM but not confirmed to this TM
        // then discarding those changes would invalidate that previously completed checkpoint.
        // Solution:
        // 1. pass last completed checkpoint id in barriers, trigger RPC, and abort RPC
        // 2. confirm for the id above
        // 3. make sure that at most 1 checkpoint in flight (CC config)
    }

    @Override
    public void truncate(SequenceNumber to) {
        LOG.debug("truncate {} to {}", logId, to);
        if (to.compareTo(lastAppendedSequenceNumber) > 0) {
            // can happen if client calls truncate(prevSqn.next())
            rollover();
        }
        changeSets.headMap(to, false).clear();
        uploaded.headMap(to, false).clear();
        confirmed.headMap(to, false).clear();
    }

    private void rollover() {
        if (activeChangeSet.isEmpty()) {
            return;
        }
        lastAppendedSequenceNumber = lastAppendedSequenceNumber.next();
        changeSets.put(
                lastAppendedSequenceNumber,
                new StateChangeSet(logId, lastAppendedSequenceNumber, activeChangeSet));
        activeChangeSet = new ArrayList<>();
    }

    @SafeVarargs
    private final StateChangelogHandleStreamImpl buildHandle(Collection<StoreResult>... results) {
        List<Tuple2<StreamStateHandle, Long>> sorted =
                Arrays.stream(results)
                        .flatMap(Collection::stream)
                        // can't assume order across different handles because of retries and aborts
                        .sorted(Comparator.comparing(StoreResult::getSequenceNumber))
                        .map(up -> Tuple2.of(up.getStreamStateHandle(), up.getOffset()))
                        .collect(Collectors.toList());
        // todo in MVP: replace old handles with placeholders (but maybe in the backend)
        return new StateChangelogHandleStreamImpl(sorted, keyGroupRange);
    }

    @VisibleForTesting
    SequenceNumber lastAppendedSqnUnsafe() {
        return lastAppendedSequenceNumber;
    }
}
