/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Set;

/** This interface contains methods for registering operator state with a managed store. */
@PublicEvolving
public interface OperatorStateStore {

    /**
     * Creates (or restores) a {@link BroadcastState broadcast state}. This type of state can only
     * be created to store the state of a {@code BroadcastStream}. Each state is registered under a
     * unique name. The provided serializer is used to de/serialize the state in case of
     * checkpointing (snapshot/restore). The returned broadcast state has {@code key-value} format.
     *
     * <p><b>CAUTION: the user has to guarantee that all task instances store the same elements in
     * this type of state.</b>
     *
     * <p>Each operator instance individually maintains and stores elements in the broadcast state.
     * The fact that the incoming stream is a broadcast one guarantees that all instances see all
     * the elements. Upon recovery or re-scaling, the same state is given to each of the instances.
     * To avoid hotspots, each task reads its previous partition, and if there are more tasks (scale
     * up), then the new instances read from the old instances in a round robin fashion. This is why
     * each instance has to guarantee that it stores the same elements as the rest. If not, upon
     * recovery or rescaling you may have unpredictable redistribution of the partitions, thus
     * unpredictable results.
     *
     * @param stateDescriptor The descriptor for this state, providing a name, a serializer for the
     *     keys and one for the values.
     * @param <K> The type of the keys in the broadcast state.
     * @param <V> The type of the values in the broadcast state.
     * @return The Broadcast State
     */
    <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)
            throws Exception;

    /**
     * Creates (or restores) a list state. Each state is registered under a unique name. The
     * provided serializer is used to de/serialize the state in case of checkpointing
     * (snapshot/restore).
     *
     * <p>Note the semantic differences between an operator list state and a keyed list state (see
     * {@link KeyedStateStore#getListState(ListStateDescriptor)}). Under the context of operator
     * state, the list is a collection of state items that are independent from each other and
     * eligible for redistribution across operator instances in case of changed operator
     * parallelism. In other words, these state items are the finest granularity at which non-keyed
     * state can be redistributed, and should not be correlated with each other.
     *
     * <p>The redistribution scheme of this list state upon operator rescaling is a round-robin
     * pattern, such that the logical whole state (a concatenation of all the lists of state
     * elements previously managed by each operator before the restore) is evenly divided into as
     * many sublists as there are parallel operators.
     *
     * @param stateDescriptor The descriptor for this state, providing a name and serializer.
     * @param <S> The generic type of the state
     * @return A list for all state partitions.
     */
    <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception;

    /**
     * Creates (or restores) a list state. Each state is registered under a unique name. The
     * provided serializer is used to de/serialize the state in case of checkpointing
     * (snapshot/restore).
     *
     * <p>Note the semantic differences between an operator list state and a keyed list state (see
     * {@link KeyedStateStore#getListState(ListStateDescriptor)}). Under the context of operator
     * state, the list is a collection of state items that are independent from each other and
     * eligible for redistribution across operator instances in case of changed operator
     * parallelism. In other words, these state items are the finest granularity at which non-keyed
     * state can be redistributed, and should not be correlated with each other.
     *
     * <p>The redistribution scheme of this list state upon operator rescaling is a broadcast
     * pattern, such that the logical whole state (a concatenation of all the lists of state
     * elements previously managed by each operator before the restore) is restored to all parallel
     * operators so that each of them will get the union of all state items before the restore.
     *
     * @param stateDescriptor The descriptor for this state, providing a name and serializer.
     * @param <S> The generic type of the state
     * @return A list for all state partitions.
     */
    <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception;

    /**
     * Returns a set with the names of all currently registered states.
     *
     * @return set of names for all registered states.
     */
    Set<String> getRegisteredStateNames();

    /**
     * Returns a set with the names of all currently registered broadcast states.
     *
     * @return set of names for all registered broadcast states.
     */
    Set<String> getRegisteredBroadcastStateNames();
}
