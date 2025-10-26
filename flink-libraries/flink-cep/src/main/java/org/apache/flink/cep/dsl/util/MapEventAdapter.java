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

package org.apache.flink.cep.dsl.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.cep.dsl.api.EventAdapter;

import java.util.Map;
import java.util.Optional;

/**
 * Event adapter for Map-based events.
 *
 * <p>This adapter supports events represented as {@code Map<String, Object>}, where attribute
 * names are map keys. This is useful for dynamic or schema-less event types.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * DataStream<Map<String, Object>> events = ...;
 * MapEventAdapter adapter = new MapEventAdapter();
 *
 * PatternStream<Map<String, Object>> pattern = DslCompiler.compile(
 *     "Alert(severity > 5 and type = 'error')",
 *     events,
 *     adapter
 * );
 * }</pre>
 *
 * <p>By default, the event type is extracted from a special key {@code _eventType}. If this key is
 * not present, the type "Event" is returned. You can customize the event type key by using {@link
 * #MapEventAdapter(String)}.
 */
@PublicEvolving
public class MapEventAdapter implements EventAdapter<Map<String, Object>> {

    private static final long serialVersionUID = 1L;

    /** Default key used to store event type in the map. */
    public static final String DEFAULT_EVENT_TYPE_KEY = "_eventType";

    private final String eventTypeKey;

    /** Creates a MapEventAdapter with the default event type key. */
    public MapEventAdapter() {
        this(DEFAULT_EVENT_TYPE_KEY);
    }

    /**
     * Creates a MapEventAdapter with a custom event type key.
     *
     * @param eventTypeKey The key in the map that contains the event type
     */
    public MapEventAdapter(String eventTypeKey) {
        this.eventTypeKey = eventTypeKey;
    }

    @Override
    public Optional<Object> getAttribute(Map<String, Object> event, String attributeName) {
        if (event == null) {
            return Optional.empty();
        }

        // Handle nested attributes (e.g., "user.name")
        if (attributeName.contains(".")) {
            return getNestedAttribute(event, attributeName);
        }

        return Optional.ofNullable(event.get(attributeName));
    }

    @Override
    public String getEventType(Map<String, Object> event) {
        if (event == null) {
            return "null";
        }

        Object typeValue = event.get(eventTypeKey);
        if (typeValue != null) {
            return typeValue.toString();
        }

        // Default to "Event" if no type key is present
        return "Event";
    }

    /**
     * Handle nested attribute access for Map events.
     *
     * <p>For example, "user.name" will first get the "user" object from the map, and then extract
     * the "name" attribute from it.
     */
    @SuppressWarnings("unchecked")
    private Optional<Object> getNestedAttribute(Map<String, Object> event, String attributeName) {
        String[] parts = attributeName.split("\\.", 2);
        String first = parts[0];
        String rest = parts[1];

        Object intermediate = event.get(first);
        if (intermediate == null) {
            return Optional.empty();
        }

        // If intermediate is also a Map, continue with MapEventAdapter
        if (intermediate instanceof Map) {
            return getNestedAttribute((Map<String, Object>) intermediate, rest);
        }

        // Otherwise, use reflection for nested object
        ReflectiveEventAdapter<Object> reflectiveAdapter = new ReflectiveEventAdapter<>();
        return reflectiveAdapter.getAttribute(intermediate, rest);
    }
}
