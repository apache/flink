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

package org.apache.flink.connector.base.sink;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * State class for {@link DemultiplexingSink} that tracks the state of individual sink writers for
 * each route during checkpointing and recovery.
 *
 * <p>This state contains:
 *
 * <ul>
 *   <li>A mapping of route keys to their corresponding sink writer states
 *   <li>Metadata about which routes are currently active
 * </ul>
 *
 * @param <RouteT> The type of route keys
 */
public class DemultiplexingSinkState<RouteT> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Map of route keys to their serialized sink writer states. */
    private final Map<RouteT, byte[]> routeStates;

    /** Creates a new empty demultiplexing sink state. */
    public DemultiplexingSinkState() {
        this.routeStates = new HashMap<>();
    }

    /**
     * Creates a new demultiplexing sink state with the given route states.
     *
     * @param routeStates Map of route keys to their serialized sink writer states
     */
    public DemultiplexingSinkState(Map<RouteT, byte[]> routeStates) {
        this.routeStates = new HashMap<>(Preconditions.checkNotNull(routeStates));
    }

    /**
     * Gets the serialized state for a specific route.
     *
     * @param route The route key
     * @return The serialized state for the route, or null if no state exists
     */
    public byte[] getRouteState(RouteT route) {
        return routeStates.get(route);
    }

    /**
     * Sets the serialized state for a specific route.
     *
     * @param route The route key
     * @param state The serialized state data
     */
    public void setRouteState(RouteT route, byte[] state) {
        if (state != null) {
            routeStates.put(route, state);
        } else {
            routeStates.remove(route);
        }
    }

    /**
     * Gets all route keys that have associated state.
     *
     * @return An unmodifiable set of route keys
     */
    public java.util.Set<RouteT> getRoutes() {
        return Collections.unmodifiableSet(routeStates.keySet());
    }

    /**
     * Gets a copy of all route states.
     *
     * @return An unmodifiable map of route keys to their serialized states
     */
    public Map<RouteT, byte[]> getRouteStates() {
        return Collections.unmodifiableMap(routeStates);
    }

    /**
     * Checks if this state contains any route states.
     *
     * @return true if there are no route states, false otherwise
     */
    public boolean isEmpty() {
        return routeStates.isEmpty();
    }

    /**
     * Gets the number of routes with associated state.
     *
     * @return The number of routes
     */
    public int size() {
        return routeStates.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DemultiplexingSinkState<?> that = (DemultiplexingSinkState<?>) o;

        // Compare route states with proper byte array comparison
        if (routeStates.size() != that.routeStates.size()) {
            return false;
        }

        for (Map.Entry<RouteT, byte[]> entry : routeStates.entrySet()) {
            RouteT key = entry.getKey();
            byte[] value = entry.getValue();
            byte[] otherValue = that.routeStates.get(key);

            if (!java.util.Arrays.equals(value, otherValue)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (Map.Entry<RouteT, byte[]> entry : routeStates.entrySet()) {
            result = 31 * result + Objects.hashCode(entry.getKey());
            result = 31 * result + java.util.Arrays.hashCode(entry.getValue());
        }
        return result;
    }

    @Override
    public String toString() {
        return "DemultiplexingSinkState{"
                + "routeCount="
                + routeStates.size()
                + ", routes="
                + routeStates.keySet()
                + '}';
    }
}
