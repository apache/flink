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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** {@link ChangelogStateHandle} implementation based on {@link StreamStateHandle}. */
@Internal
public final class ChangelogStateHandleStreamImpl implements ChangelogStateHandle {

    private static final long serialVersionUID = -8070326169926626355L;

    private final KeyGroupRange keyGroupRange;
    /** NOTE: order is important as it reflects the order of changes. */
    private final List<Tuple2<StreamStateHandle, Long>> handlesAndOffsets;

    private final long size;
    private final long incrementalSize;
    private final StateHandleID stateHandleID;

    private final String storageIdentifier;

    public ChangelogStateHandleStreamImpl(
            List<Tuple2<StreamStateHandle, Long>> handlesAndOffsets,
            KeyGroupRange keyGroupRange,
            long size,
            long incrementalSize,
            String storageIdentifier) {
        this(
                handlesAndOffsets,
                keyGroupRange,
                size,
                incrementalSize,
                storageIdentifier,
                new StateHandleID(UUID.randomUUID().toString()));
    }

    private ChangelogStateHandleStreamImpl(
            List<Tuple2<StreamStateHandle, Long>> handlesAndOffsets,
            KeyGroupRange keyGroupRange,
            long size,
            long incrementalSize,
            String storageIdentifier,
            StateHandleID stateHandleId) {
        this.handlesAndOffsets = handlesAndOffsets;
        this.keyGroupRange = keyGroupRange;
        this.size = size;
        this.incrementalSize = incrementalSize;
        this.storageIdentifier = storageIdentifier;
        this.stateHandleID = stateHandleId;
    }

    public static ChangelogStateHandleStreamImpl restore(
            List<Tuple2<StreamStateHandle, Long>> handlesAndOffsets,
            KeyGroupRange keyGroupRange,
            long size,
            long incrementalSize,
            String storageIdentifier,
            StateHandleID stateHandleID) {
        return new ChangelogStateHandleStreamImpl(
                handlesAndOffsets,
                keyGroupRange,
                size,
                incrementalSize,
                storageIdentifier,
                stateHandleID);
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        handlesAndOffsets.forEach(
                handleAndOffset ->
                        stateRegistry.registerReference(
                                SharedStateRegistryKey.forStreamStateHandle(handleAndOffset.f0),
                                handleAndOffset.f0,
                                checkpointID));
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        KeyGroupRange offsets = this.keyGroupRange.getIntersection(keyGroupRange);
        if (offsets.getNumberOfKeyGroups() == 0) {
            return null;
        }
        return new ChangelogStateHandleStreamImpl(
                handlesAndOffsets, offsets, 0L, 0L /* unknown */, storageIdentifier);
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleID;
    }

    @Override
    public void discardState() throws Exception {
        // Do nothing: state will be discarded by SharedStateRegistry once JM receives it and a
        // newer checkpoint completes without using it. JM might not receive the handle in the
        // following cases:
        // 1. hard TM failure
        // 2. job termination
        // 3. materialization of changes written pre-emptively
        // The above cases will be addressed by FLINK-23139 and/or FLINK-24852
    }

    @Override
    public long getStateSize() {
        return size;
    }

    @Override
    public long getCheckpointedSize() {
        return incrementalSize;
    }

    @Override
    public String getStorageIdentifier() {
        return storageIdentifier;
    }

    public List<Tuple2<StreamStateHandle, Long>> getHandlesAndOffsets() {
        return Collections.unmodifiableList(handlesAndOffsets);
    }
}
