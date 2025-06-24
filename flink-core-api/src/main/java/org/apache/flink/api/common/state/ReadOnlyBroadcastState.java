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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

/**
 * A read-only view of the {@link BroadcastState}.
 *
 * <p>Although read-only, the user code should not modify the value returned by the {@link
 * #get(Object)} or the entries of the immutable iterator returned by the {@link
 * #immutableEntries()}, as this can lead to inconsistent states. The reason for this is that we do
 * not create extra copies of the elements for performance reasons.
 *
 * @param <K> The key type of the elements in the {@link ReadOnlyBroadcastState}.
 * @param <V> The value type of the elements in the {@link ReadOnlyBroadcastState}.
 */
@PublicEvolving
public interface ReadOnlyBroadcastState<K, V> extends State {

    /**
     * Returns the current value associated with the given key.
     *
     * <p>The user code must not modify the value returned, as this can lead to inconsistent states.
     *
     * @param key The key of the mapping
     * @return The value of the mapping with the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    V get(K key) throws Exception;

    /**
     * Returns whether there exists the given mapping.
     *
     * @param key The key of the mapping
     * @return True if there exists a mapping whose key equals to the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    boolean contains(K key) throws Exception;

    /**
     * Returns an immutable {@link Iterable} over the entries in the state.
     *
     * <p>The user code must not modify the entries of the returned immutable iterator, as this can
     * lead to inconsistent states.
     */
    Iterable<Map.Entry<K, V>> immutableEntries() throws Exception;
}
