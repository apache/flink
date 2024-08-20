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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.ValueState;

import java.util.List;
import java.util.Map;

/**
 * The type of processing request for {@link State} from **users' perspective**. Each interface of
 * {@link State} and its sub-interfaces will have a corresponding enum entry.
 *
 * <p>TODO: Serialization and Deserialization.
 */
public enum StateRequestType {

    /**
     * A sync point with AEC, does nothing with state, checking if the key is occupied by others and
     * blocking if needed. This is a special one that only created by the runtime framework without
     * visibility to users.
     */
    SYNC_POINT,

    /** Clear the current partition of the state, {@link State#asyncClear()}. */
    CLEAR,

    /** Get value from current partition, {@link ValueState#asyncValue()}. */
    VALUE_GET,

    /** Update value to current partition, {@link ValueState#asyncUpdate(Object)}. */
    VALUE_UPDATE,

    /** Get from list state, {@link ListState#asyncGet()}. */
    LIST_GET,

    /** Add value to list state, {@link ListState#asyncAdd(Object)}. */
    LIST_ADD,

    /** Put a list to current partition, {@link ListState#asyncUpdate(List)}. */
    LIST_UPDATE,

    /** Add multiple value to list of current partition, {@link ListState#asyncAddAll(List)}. */
    LIST_ADD_ALL,

    /** Get a value by a key from current partition, {@link MapState#asyncGet(Object)}. */
    MAP_GET,

    /** Check key existence of current partition, {@link MapState#asyncContains(Object)}}. */
    MAP_CONTAINS,

    /** Update a key-value pair of current partition, {@link MapState#asyncPut(Object, Object)}. */
    MAP_PUT,

    /** Update multiple key-value pairs of current partition, {@link MapState#asyncPutAll(Map)}. */
    MAP_PUT_ALL,

    /**
     * Get an iterator of key-value mapping within current partition, {@link
     * MapState#asyncEntries()}.
     */
    MAP_ITER,

    /** Get an iterator of keys within current partition, {@link MapState#asyncKeys()}. */
    MAP_ITER_KEY,

    /** Get an iterator of values within current partition, {@link MapState#asyncValues()}. */
    MAP_ITER_VALUE,

    /**
     * Remove a key-value mapping within current partition, {@link MapState#asyncRemove(Object)}.
     */
    MAP_REMOVE,

    /**
     * Check the existence of any key-value mapping within current partition, {@link
     * MapState#asyncIsEmpty()}.
     */
    MAP_IS_EMPTY,

    /** Continuously load elements for one iterator. */
    ITERATOR_LOADING,

    /** Get from reducing state, {@link ReducingState#asyncGet()}. */
    REDUCING_GET,

    /** Add element into reducing state, {@link ReducingState#asyncAdd(Object)}. */
    REDUCING_ADD,

    /** Get value from aggregating state by {@link AggregatingState#asyncGet()}. */
    AGGREGATING_GET,

    /** Add element to aggregating state by {@link AggregatingState#asyncAdd(Object)}. */
    AGGREGATING_ADD
}
