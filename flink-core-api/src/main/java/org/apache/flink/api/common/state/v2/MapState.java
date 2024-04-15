/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.Experimental;

import java.util.Map;

/**
 * {@link State} interface for partitioned key-value state. The key-value pair can be added, updated
 * and retrieved.
 *
 * <p>The state is accessed and modified by user functions, and checkpointed consistently by the
 * system as part of the distributed snapshots.
 *
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the key of
 * the current element. That way, the system can handle stream and state partitioning consistently
 * together.
 *
 * @param <UK> Type of the keys in the state.
 * @param <UV> Type of the values in the state.
 */
@Experimental
public interface MapState<UK, UV> extends State {

    /**
     * Returns the current value associated with the given key asynchronously. When the state is not
     * partitioned the returned value is the same for all inputs in a given operator instance. If
     * state partitioning is applied, the value returned depends on the current operator input, as
     * the operator maintains an independent state for each partition.
     *
     * @return The {@link StateFuture} that will return value corresponding to the current input.
     *     When no corresponding value for this key, the future will return {@code null}.
     */
    StateFuture<UV> asyncGet(UK key);

    /**
     * Update the current value associated with the given key asynchronously. When the state is not
     * partitioned the value is updated for all inputs in a given operator instance. If state
     * partitioning is applied, the updated value depends on the current operator input, as the
     * operator maintains an independent state for each partition. When a {@code null} value is
     * provided, the state for the given key will be removed.
     *
     * @param key The key that will be updated.
     * @param value The new value for the key.
     * @return The {@link StateFuture} that will trigger the callback when update finishes.
     */
    StateFuture<Void> asyncPut(UK key, UV value);

    /**
     * Update all of the mappings from the given map into the state asynchronously. When the state
     * is not partitioned the value is updated for all inputs in a given operator instance. If state
     * partitioning is applied, the updated mapping depends on the current operator input, as the
     * operator maintains an independent state for each partition. When a {@code null} value is
     * provided within the map, the state for the corresponding key will be removed.
     *
     * <p>If an empty map is passed in, the state value remains unchanged.
     *
     * <p>Null map pointer is not allowed.
     *
     * @param map The mappings to be stored in this state.
     * @return The {@link StateFuture} that will trigger the callback when update finishes.
     */
    StateFuture<Void> asyncPutAll(Map<UK, UV> map);

    /**
     * Delete the mapping of the given key from the state asynchronously. When the state is not
     * partitioned the deleted value is the same for all inputs in a given operator instance. If
     * state partitioning is applied, the value deleted depends on the current operator input, as
     * the operator maintains an independent state for each partition.
     *
     * @param key The key of the mapping.
     * @return The {@link StateFuture} that will trigger the callback when update finishes.
     */
    StateFuture<Void> asyncRemove(UK key);

    /**
     * Returns whether there exists the given mapping asynchronously. When the state is not
     * partitioned the returned value is the same for all inputs in a given operator instance. If
     * state partitioning is applied, the value returned depends on the current operator input, as
     * the operator maintains an independent state for each partition.
     *
     * @param key The key of the mapping.
     * @return The {@link StateFuture} that will return true if there exists a mapping whose key
     *     equals to the given key.
     */
    StateFuture<Boolean> asyncContains(UK key);

    /**
     * Returns the current iterator for all the mappings of this state asynchronously. When the
     * state is not partitioned the returned iterator is the same for all inputs in a given operator
     * instance. If state partitioning is applied, the iterator returned depends on the current
     * operator input, as the operator maintains an independent state for each partition.
     *
     * @return The {@link StateFuture} that will return mapping iterator corresponding to the
     *     current input.
     */
    StateFuture<StateIterator<Map.Entry<UK, UV>>> asyncEntries();

    /**
     * Returns the current iterator for all the keys of this state asynchronously. When the state is
     * not partitioned the returned iterator is the same for all inputs in a given operator
     * instance. If state partitioning is applied, the iterator returned depends on the current
     * operator input, as the operator maintains an independent state for each partition.
     *
     * @return The {@link StateFuture} that will return key iterator corresponding to the current
     *     input.
     */
    StateFuture<StateIterator<UK>> asyncKeys();

    /**
     * Returns the current iterator for all the values of this state asynchronously. When the state
     * is not partitioned the returned iterator is the same for all inputs in a given operator
     * instance. If state partitioning is applied, the iterator returned depends on the current
     * operator input, as the operator maintains an independent state for each partition.
     *
     * @return The {@link StateFuture} that will return value iterator corresponding to the current
     *     input.
     */
    StateFuture<StateIterator<UV>> asyncValues();

    /**
     * Returns whether this state contains no key-value mappings asynchronously. When the state is
     * not partitioned the returned value is the same for all inputs in a given operator instance.
     * If state partitioning is applied, the value returned depends on the current operator input,
     * as the operator maintains an independent state for each partition.
     *
     * @return The {@link StateFuture} that will return true if there is no key-value mapping,
     *     otherwise false.
     */
    StateFuture<Boolean> asyncIsEmpty();
}
