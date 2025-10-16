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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;

import java.io.Serializable;

/**
 * Interface for routing elements to different sinks in a {@link DemultiplexingSink}.
 *
 * <p>The router is responsible for two key operations:
 *
 * <ul>
 *   <li>Extracting a route key from each input element that determines which sink to use
 *   <li>Creating new sink instances when a previously unseen route is encountered
 * </ul>
 *
 * <p>Route keys should be deterministic and consistent - the same logical destination should always
 * produce the same route key to ensure proper sink reuse and state management.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Route messages to different Kafka topics based on message type
 * SinkRouter<MyMessage, String> router = new SinkRouter<MyMessage, String>() {
 *     @Override
 *     public String getRoute(MyMessage element) {
 *         return element.getMessageType(); // e.g., "orders", "users", "events"
 *     }
 *
 *     @Override
 *     public Sink<MyMessage> createSink(String route, MyMessage element) {
 *         return KafkaSink.<MyMessage>builder()
 *             .setBootstrapServers("localhost:9092")
 *             .setRecordSerializer(...)
 *             .setTopics(route) // Use route as topic name
 *             .build();
 *     }
 * };
 * }</pre>
 *
 * @param <InputT> The type of input elements to route
 * @param <RouteT> The type of route keys used for sink selection and caching
 */
@PublicEvolving
public interface SinkRouter<InputT, RouteT> extends Serializable {

    /**
     * Extract the route key from an input element.
     *
     * <p>This method is called for every element and should be efficient. The returned route key is
     * used to:
     *
     * <ul>
     *   <li>Look up the appropriate sink instance in the cache
     *   <li>Create a new sink if this route hasn't been seen before
     *   <li>Group elements by route for state management during checkpointing
     * </ul>
     *
     * <p>Route keys must implement {@link Object#equals(Object)} and {@link Object#hashCode()}
     * properly as they are used as keys in hash-based collections.
     *
     * @param element The input element to route
     * @return The route key that determines which sink to use for this element
     */
    RouteT getRoute(InputT element);

    /**
     * Create a new sink instance for the given route.
     *
     * <p>This method is called when a route is encountered for the first time. The created sink
     * will be cached and reused for all subsequent elements with the same route key.
     *
     * <p>The element parameter provides access to the specific element that triggered the creation
     * of this route, which can be useful for extracting configuration information (e.g., connection
     * details, authentication credentials) that may be embedded in the element.
     *
     * <p>The created sink should be fully configured and ready to use. It will be initialized by
     * the DemultiplexingSink framework using the standard Sink API.
     *
     * @param route The route key for which to create a sink
     * @param element The element that triggered the creation of this route (for configuration
     *     extraction)
     * @return A new sink instance configured for the given route
     */
    Sink<InputT> createSink(RouteT route, InputT element);
}
