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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** In-memory {@link ChangelogStateHandle}. */
@Internal
public class InMemoryChangelogStateHandle implements ChangelogStateHandle {

    private static final long serialVersionUID = 1L;

    private final List<StateChange> changes;
    private final SequenceNumber from; // for debug purposes
    private final SequenceNumber to; // for debug purposes
    private final KeyGroupRange keyGroupRange;
    private final StateHandleID stateHandleID;

    public InMemoryChangelogStateHandle(
            List<StateChange> changes,
            SequenceNumber from,
            SequenceNumber to,
            KeyGroupRange keyGroupRange) {
        this(changes, from, to, keyGroupRange, new StateHandleID(UUID.randomUUID().toString()));
    }

    private InMemoryChangelogStateHandle(
            List<StateChange> changes,
            SequenceNumber from,
            SequenceNumber to,
            KeyGroupRange keyGroupRange,
            StateHandleID stateHandleId) {
        this.changes = changes;
        this.from = from;
        this.to = to;
        this.keyGroupRange = keyGroupRange;
        this.stateHandleID = stateHandleId;
    }

    public static InMemoryChangelogStateHandle restore(
            List<StateChange> changes,
            SequenceNumber from,
            SequenceNumber to,
            KeyGroupRange keyGroupRange,
            StateHandleID stateHandleId) {
        return new InMemoryChangelogStateHandle(changes, from, to, keyGroupRange, stateHandleId);
    }

    @Override
    public void discardState() {}

    @Override
    public long getStateSize() {
        return changes.stream().mapToLong(change -> change.getChange().length).sum();
    }

    @Override
    public long getCheckpointedSize() {
        // memory changelog state handle would be counted as checkpoint each time.
        return getStateSize();
    }

    public List<StateChange> getChanges() {
        return Collections.unmodifiableList(changes);
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        return changes.stream().mapToInt(StateChange::getKeyGroup).anyMatch(keyGroupRange::contains)
                ? this
                : null;
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleID;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        // do nothing
    }

    @Override
    public String toString() {
        return String.format("from %s to %s: %s", from, to, changes);
    }

    public long getFrom() {
        return ((SequenceNumber.GenericSequenceNumber) from).number;
    }

    public long getTo() {
        return ((SequenceNumber.GenericSequenceNumber) to).number;
    }

    @Override
    public String getStorageIdentifier() {
        return InMemoryStateChangelogStorageFactory.IDENTIFIER;
    }
}
