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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

import java.util.stream.Stream;

/**
 * Base class for state maps.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateMap<K, N, S> implements Iterable<StateEntry<K, N, S>> {

    // Main interface methods of StateMap -------------------------------------------------------

    /**
     * Returns whether this {@link StateMap} is empty.
     *
     * @return {@code true} if this {@link StateMap} has no elements, {@code false} otherwise.
     * @see #size()
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the total number of entries in this {@link StateMap}.
     *
     * @return the number of entries in this {@link StateMap}.
     */
    public abstract int size();

    /**
     * Returns the state for the composite of active key and given namespace.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @return the state of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    public abstract S get(K key, N namespace);

    /**
     * Returns whether this map contains the specified key/namespace composite key.
     *
     * @param key the key in the composite key to search for. Not null.
     * @param namespace the namespace in the composite key to search for. Not null.
     * @return {@code true} if this map contains the specified key/namespace composite key, {@code
     *     false} otherwise.
     */
    public abstract boolean containsKey(K key, N namespace);

    /**
     * Maps the specified key/namespace composite key to the specified value. This method should be
     * preferred over {@link #putAndGetOld(K, N, S)} (key, Namespace, State) when the caller is not
     * interested in the old state.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     */
    public abstract void put(K key, N namespace, S state);

    /**
     * Maps the composite of active key and given namespace to the specified state. Returns the
     * previous state that was registered under the composite key.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     * @return the state of any previous mapping with the specified key or {@code null} if there was
     *     no such mapping.
     */
    public abstract S putAndGetOld(K key, N namespace, S state);

    /**
     * Removes the mapping for the composite of active key and given namespace. This method should
     * be preferred over {@link #removeAndGetOld(K, N)} when the caller is not interested in the old
     * state.
     *
     * @param key the key of the mapping to remove. Not null.
     * @param namespace the namespace of the mapping to remove. Not null.
     */
    public abstract void remove(K key, N namespace);

    /**
     * Removes the mapping for the composite of active key and given namespace, returning the state
     * that was found under the entry.
     *
     * @param key the key of the mapping to remove. Not null.
     * @param namespace the namespace of the mapping to remove. Not null.
     * @return the state of the removed mapping or {@code null} if no mapping for the specified key
     *     was found.
     */
    public abstract S removeAndGetOld(K key, N namespace);

    /**
     * Applies the given {@link StateTransformationFunction} to the state (1st input argument),
     * using the given value as second input argument. The result of {@link
     * StateTransformationFunction#apply(Object, Object)} is then stored as the new state. This
     * function is basically an optimization for get-update-put pattern.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @param value the value to use in transforming the state. Can be null.
     * @param transformation the transformation function.
     * @throws Exception if some exception happens in the transformation function.
     */
    public abstract <T> void transform(
            K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception;

    // For queryable state ------------------------------------------------------------------------

    public abstract Stream<K> getKeys(N namespace);

    public abstract InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords);

    /**
     * Creates a snapshot of this {@link StateMap}, to be written in checkpointing. Users should
     * call {@link #releaseSnapshot(StateMapSnapshot)} after using the returned object.
     *
     * @return a snapshot from this {@link StateMap}, for checkpointing.
     */
    @Nonnull
    public abstract StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot();

    /**
     * Releases a snapshot for this {@link StateMap}. This method should be called once a snapshot
     * is no more needed.
     *
     * @param snapshotToRelease the snapshot to release, which was previously created by this state
     *     map.
     */
    public void releaseSnapshot(
            StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> snapshotToRelease) {}

    // For testing --------------------------------------------------------------------------------

    @VisibleForTesting
    public abstract int sizeOfNamespace(Object namespace);
}
