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

/**
 * Base interface for partitioned state that supports adding elements and inspecting the current
 * state. Elements can either be kept in a buffer (list-like) or aggregated into one value.
 *
 * <p>The state is accessed and modified by user functions, and checkpointed consistently by the
 * system as part of the distributed snapshots.
 *
 * <p>The state is only accessible by functions applied on a {@code KeyedStream}. The key is
 * automatically supplied by the system, so the function always sees the value mapped to the key of
 * the current element. That way, the system can handle stream and state partitioning consistently
 * together.
 *
 * @param <IN> Type of the value that can be added to the state.
 * @param <OUT> Type of the value that can be retrieved from the state.
 */
@PublicEvolving
public interface AppendingState<IN, OUT> extends State {

    /**
     * Returns the current value for the state. When the state is not partitioned the returned value
     * is the same for all inputs in a given operator instance. If state partitioning is applied,
     * the value returned depends on the current operator input, as the operator maintains an
     * independent state for each partition.
     *
     * <p><b>NOTE TO IMPLEMENTERS:</b> if the state is empty, then this method should return {@code
     * null}.
     *
     * @return The operator state value corresponding to the current input or {@code null} if the
     *     state is empty.
     * @throws Exception Thrown if the system cannot access the state.
     */
    OUT get() throws Exception;

    /**
     * Updates the operator state accessible by {@link #get()} by adding the given value to the list
     * of values. The next time {@link #get()} is called (for the same state partition) the returned
     * state will represent the updated list.
     *
     * <p>If null is passed in, the state value will remain unchanged.
     *
     * @param value The new value for the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    void add(IN value) throws Exception;
}
