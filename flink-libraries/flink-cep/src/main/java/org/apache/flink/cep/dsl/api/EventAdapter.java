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

package org.apache.flink.cep.dsl.api;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Optional;

/**
 * Adapter interface for extracting attributes from events.
 *
 * <p>This interface allows the CEP DSL to work with any event type without requiring specific
 * interfaces to be implemented. Users can provide custom adapters for their event types, or use
 * the built-in {@link org.apache.flink.cep.dsl.util.ReflectiveEventAdapter} which uses Java
 * reflection to access POJO fields and getters.
 *
 * <p>Example usage with custom adapter:
 * <pre>{@code
 * EventAdapter<MyEvent> adapter = new EventAdapter<MyEvent>() {
 *     @Override
 *     public Optional<Object> getAttribute(MyEvent event, String attributeName) {
 *         return Optional.ofNullable(event.getProperty(attributeName));
 *     }
 *
 *     @Override
 *     public String getEventType(MyEvent event) {
 *         return event.getClass().getSimpleName();
 *     }
 * };
 *
 * PatternStream<MyEvent> pattern = DslCompiler.compile(
 *     "Alert(severity > 5)",
 *     dataStream,
 *     adapter
 * );
 * }</pre>
 *
 * @param <T> The type of events to adapt
 */
@PublicEvolving
public interface EventAdapter<T> extends Serializable {

    /**
     * Extract an attribute value from an event.
     *
     * <p>This method is called by the DSL evaluation engine to access event attributes referenced
     * in pattern expressions (e.g., {@code temperature > 100}).
     *
     * @param event The event to extract from
     * @param attributeName The name of the attribute to extract
     * @return The attribute value wrapped in an Optional, or {@link Optional#empty()} if the
     *     attribute doesn't exist
     */
    Optional<Object> getAttribute(T event, String attributeName);

    /**
     * Get the event type name for type matching in DSL patterns.
     *
     * <p>When strict type matching is enabled, the DSL uses this method to verify that events
     * match the expected type specified in the pattern (e.g., {@code Sensor(temperature > 100)}
     * expects events with type "Sensor").
     *
     * @param event The event
     * @return The event type name (typically the simple class name)
     */
    String getEventType(T event);
}
