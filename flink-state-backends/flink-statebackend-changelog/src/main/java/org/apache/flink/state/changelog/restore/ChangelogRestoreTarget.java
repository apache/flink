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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;
import org.apache.flink.state.changelog.ChangelogState;

import javax.annotation.Nonnull;

/** Maintains metadata operation related to Changelog recovery. */
@Internal
public interface ChangelogRestoreTarget<K> {

    /** Returns the key groups which this restore procedure covers. */
    KeyGroupRange getKeyGroupRange();

    /**
     * Creates a keyed state which could be retrieved by {@link #getExistingState(String,
     * BackendStateType)} in the restore procedure. The interface comes from {@link
     * KeyedStateBackend#getOrCreateKeyedState(TypeSerializer, StateDescriptor)}.
     */
    <N, S extends State, V> S createKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception;

    /**
     * Creates a {@link KeyGroupedInternalPriorityQueue} which could be retrieved by {@link
     * #getExistingState(String, BackendStateType)} in the restore procedure. The interface comes
     * from {@link PriorityQueueSetFactory#create(String, TypeSerializer)}.
     */
    @Nonnull
    <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> createPqState(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer);

    /**
     * Returns the existing state created by {@link #createKeyedState(TypeSerializer,
     * StateDescriptor)} or {@link #createPqState(String, TypeSerializer)} in the restore procedure.
     */
    ChangelogState getExistingState(String name, BackendStateType type);

    /** Returns keyed state backend restored finally. */
    CheckpointableKeyedStateBackend<K> getRestoredKeyedStateBackend();
}
