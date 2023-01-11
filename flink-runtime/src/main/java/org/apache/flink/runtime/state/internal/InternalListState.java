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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.ListState;

import java.util.List;

/**
 * The peer to the {@link ListState} in the internal state type hierarchy.
 *
 * <p>See {@link InternalKvState} for a description of the internal state hierarchy.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> The type of elements in the list
 */
public interface InternalListState<K, N, T>
        extends InternalMergingState<K, N, T, List<T>, Iterable<T>>, ListState<T> {

    /**
     * Updates the operator state accessible by {@link #get()} by updating existing values to the
     * given list of values. The next time {@link #get()} is called (for the same state partition)
     * the returned state will represent the updated list.
     *
     * <p>If `null` or an empty list is passed in, the state value will be null
     *
     * @param values The new values for the state.
     * @throws Exception The method may forward exception thrown internally (by I/O or functions).
     */
    void update(List<T> values) throws Exception;

    /**
     * Updates the operator state accessible by {@link #get()} by adding the given values to
     * existing list of values. The next time {@link #get()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>If `null` or an empty list is passed in, the state value remains unchanged
     *
     * @param values The new values to be added to the state.
     * @throws Exception The method may forward exception thrown internally (by I/O or functions).
     */
    void addAll(List<T> values) throws Exception;
}
