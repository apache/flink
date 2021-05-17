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

package org.apache.flink.table.runtime.operators.window.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.table.data.RowData;

import java.util.Iterator;
import java.util.Map;

/** A wrapper of {@link MapState} which is easier to update based on window namespace. */
public final class WindowMapState<W, UV> implements WindowState<W> {

    private final InternalMapState<RowData, W, RowData, UV> windowState;

    public WindowMapState(InternalMapState<RowData, W, RowData, UV> windowState) {
        this.windowState = windowState;
    }

    public void clear(W window) {
        windowState.setCurrentNamespace(window);
        windowState.clear();
    }

    /**
     * Returns the current value associated with the given key.
     *
     * @param key The key of the mapping
     * @return The value of the mapping with the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    public UV get(W window, RowData key) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.get(key);
    }

    /**
     * Associates a new value with the given key.
     *
     * @param key The key of the mapping
     * @param value The new value of the mapping
     * @throws Exception Thrown if the system cannot access the state.
     */
    public void put(W window, RowData key, UV value) throws Exception {
        windowState.setCurrentNamespace(window);
        windowState.put(key, value);
    }

    /**
     * Copies all of the mappings from the given map into the state.
     *
     * @param map The mappings to be stored in this state
     * @throws Exception Thrown if the system cannot access the state.
     */
    public void putAll(W window, Map<RowData, UV> map) throws Exception {
        windowState.setCurrentNamespace(window);
        windowState.putAll(map);
    }

    /**
     * Deletes the mapping of the given key.
     *
     * @param key The key of the mapping
     * @throws Exception Thrown if the system cannot access the state.
     */
    public void remove(W window, RowData key) throws Exception {
        windowState.setCurrentNamespace(window);
        windowState.remove(key);
    }

    /**
     * Returns whether there exists the given mapping.
     *
     * @param key The key of the mapping
     * @return True if there exists a mapping whose key equals to the given key
     * @throws Exception Thrown if the system cannot access the state.
     */
    public boolean contains(W window, RowData key) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.contains(key);
    }

    /**
     * Returns all the mappings in the state.
     *
     * @return An iterable view of all the key-value pairs in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    public Iterable<Map.Entry<RowData, UV>> entries(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.entries();
    }

    /**
     * Returns all the keys in the state.
     *
     * @return An iterable view of all the keys in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    public Iterable<RowData> keys(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.keys();
    }

    /**
     * Returns all the values in the state.
     *
     * @return An iterable view of all the values in the state.
     * @throws Exception Thrown if the system cannot access the state.
     */
    public Iterable<UV> values(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.values();
    }

    /**
     * Iterates over all the mappings in the state.
     *
     * @return An iterator over all the mappings in the state
     * @throws Exception Thrown if the system cannot access the state.
     */
    public Iterator<Map.Entry<RowData, UV>> iterator(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.iterator();
    }

    /**
     * Returns true if this state contains no key-value mappings, otherwise false.
     *
     * @return True if this state contains no key-value mappings, otherwise false.
     * @throws Exception Thrown if the system cannot access the state.
     */
    public boolean isEmpty(W window) throws Exception {
        windowState.setCurrentNamespace(window);
        return windowState.isEmpty();
    }
}
