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

package org.apache.flink.datastream.api.extension.window.context;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.state.AggregatingStateDeclaration;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.ReducingStateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.ValueState;

import java.util.Optional;

/**
 * This interface represents a context for window operations and provides methods to interact with
 * state that is scoped to the window.
 */
@Experimental
public interface WindowContext {

    /**
     * Gets the starting timestamp of the window. This is the first timestamp that belongs to this
     * window.
     *
     * @return The starting timestamp of this window, or -1 if the window is not a time window or a
     *     session window.
     */
    long getStartTime();

    /**
     * Gets the end timestamp of this window. The end timestamp is exclusive, meaning it is the
     * first timestamp that does not belong to this window anymore.
     *
     * @return The exclusive end timestamp of this window, or -1 if the window is a session window
     *     or not a time window.
     */
    long getEndTime();

    /**
     * Retrieves a {@link ListState} object that can be used to interact with fault-tolerant state
     * that is scoped to the window.
     */
    <T> Optional<ListState<T>> getWindowState(ListStateDeclaration<T> stateDeclaration)
            throws Exception;

    /**
     * Retrieves a {@link MapState} object that can be used to interact with fault-tolerant state
     * that is scoped to the window.
     */
    <KEY, V> Optional<MapState<KEY, V>> getWindowState(MapStateDeclaration<KEY, V> stateDeclaration)
            throws Exception;

    /**
     * Retrieves a {@link ValueState} object that can be used to interact with fault-tolerant state
     * that is scoped to the window.
     */
    <T> Optional<ValueState<T>> getWindowState(ValueStateDeclaration<T> stateDeclaration)
            throws Exception;

    /**
     * Retrieves a {@link ReducingState} object that can be used to interact with fault-tolerant
     * state that is scoped to the window.
     */
    <T> Optional<ReducingState<T>> getWindowState(ReducingStateDeclaration<T> stateDeclaration)
            throws Exception;

    /**
     * Retrieves a {@link AggregatingState} object that can be used to interact with fault-tolerant
     * state that is scoped to the window.
     */
    <T, ACC, OUT> Optional<AggregatingState<T, OUT>> getWindowState(
            AggregatingStateDeclaration<T, ACC, OUT> stateDeclaration) throws Exception;
}
