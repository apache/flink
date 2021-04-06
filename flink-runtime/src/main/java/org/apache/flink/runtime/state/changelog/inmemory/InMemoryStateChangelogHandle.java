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

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandle;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

class InMemoryStateChangelogHandle implements StateChangelogHandle<Void> {

    private static final long serialVersionUID = 1L;

    private final Map<Integer, List<byte[]>> changes;

    public InMemoryStateChangelogHandle(Map<Integer, List<byte[]>> changes) {
        this.changes = changes;
    }

    @Override
    public void discardState() {}

    @Override
    public long getStateSize() {
        return 0;
    }

    @Override
    public CloseableIterator<StateChange> getChanges(Void unused) {
        return CloseableIterator.fromList(
                changes.entrySet().stream().flatMap(this::mapEntryToChangeStream).collect(toList()),
                change -> {});
    }

    private Stream<StateChange> mapEntryToChangeStream(Map.Entry<Integer, List<byte[]>> entry) {
        int keyGroup = entry.getKey();
        return entry.getValue().stream().map(bytes -> new StateChange(keyGroup, bytes));
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry) {
        throw new UnsupportedOperationException();
    }
}
