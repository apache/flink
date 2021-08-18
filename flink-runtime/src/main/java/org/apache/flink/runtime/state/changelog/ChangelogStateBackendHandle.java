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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A handle to ChangelogStateBackend state. Consists of the base and delta parts. Base part
 * references materialized state (e.g. SST files), while delta part references state changes that
 * were not not materialized at the time of the snapshot. Both are potentially empty lists as there
 * can be no state or multiple states (e.g. after rescaling).
 */
@Internal
public interface ChangelogStateBackendHandle extends KeyedStateHandle {
    List<KeyedStateHandle> getMaterializedStateHandles();

    List<ChangelogStateHandle> getNonMaterializedStateHandles();

    class ChangelogStateBackendHandleImpl implements ChangelogStateBackendHandle {
        private static final long serialVersionUID = 1L;
        private final List<KeyedStateHandle> materialized;
        private final List<ChangelogStateHandle> nonMaterialized;
        private final KeyGroupRange keyGroupRange;

        public ChangelogStateBackendHandleImpl(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange) {
            this.materialized = unmodifiableList(materialized);
            this.nonMaterialized = unmodifiableList(nonMaterialized);
            this.keyGroupRange = keyGroupRange;
            checkArgument(keyGroupRange.getNumberOfKeyGroups() > 0);
        }

        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry) {
            stateRegistry.registerAll(materialized);
            stateRegistry.registerAll(nonMaterialized);
        }

        @Override
        public void discardState() throws Exception {
            try (Closer closer = Closer.create()) {
                materialized.forEach(h -> closer.register(asCloseable(h)));
                nonMaterialized.forEach(h -> closer.register(asCloseable(h)));
            }
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Nullable
        @Override
        public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
            // todo: revisit/review
            KeyGroupRange intersection = this.keyGroupRange.getIntersection(keyGroupRange);
            if (intersection.getNumberOfKeyGroups() == 0) {
                return null;
            }
            List<KeyedStateHandle> basePart =
                    this.materialized.stream()
                            .map(handle -> handle.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            List<ChangelogStateHandle> deltaPart =
                    this.nonMaterialized.stream()
                            .map(
                                    handle ->
                                            (ChangelogStateHandle)
                                                    handle.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            return new ChangelogStateBackendHandleImpl(basePart, deltaPart, intersection);
        }

        @Override
        public long getStateSize() {
            return materialized.stream().mapToLong(StateObject::getStateSize).sum()
                    + nonMaterialized.stream().mapToLong(StateObject::getStateSize).sum();
        }

        @Override
        public List<KeyedStateHandle> getMaterializedStateHandles() {
            return materialized;
        }

        @Override
        public List<ChangelogStateHandle> getNonMaterializedStateHandles() {
            return nonMaterialized;
        }

        @Override
        public String toString() {
            return String.format(
                    "keyGroupRange=%s, basePartSize=%d, deltaPartSize=%d",
                    keyGroupRange, materialized.size(), nonMaterialized.size());
        }

        private static Closeable asCloseable(KeyedStateHandle h) {
            return () -> {
                try {
                    h.discardState();
                } catch (Exception e) {
                    ExceptionUtils.rethrowIOException(e);
                }
            };
        }
    }
}
