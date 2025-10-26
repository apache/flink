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
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.dsl.util.ReflectiveEventAdapter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Builder for configuring and compiling DSL expressions.
 *
 * <p>This builder provides a fluent API for configuring DSL compilation options before compiling
 * an expression into a PatternStream.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PatternStream<Event> pattern = DslCompiler.<Event>builder()
 *     .withStrictTypeMatching()
 *     .withEventAdapter(customAdapter)
 *     .compile("A(x > 10) -> B(y < 5)", dataStream);
 * }</pre>
 *
 * @param <T> The type of events in the stream
 */
@PublicEvolving
public class DslCompilerBuilder<T> {

    private EventAdapter<T> eventAdapter;
    private boolean strictTypeMatching = false;

    /** Package-private constructor, use {@link DslCompiler#builder()} to create instances. */
    DslCompilerBuilder() {}

    /**
     * Set a custom event adapter for attribute extraction.
     *
     * <p>If not set, {@link ReflectiveEventAdapter} will be used by default.
     *
     * @param adapter The event adapter to use
     * @return this builder for fluent chaining
     */
    public DslCompilerBuilder<T> withEventAdapter(EventAdapter<T> adapter) {
        this.eventAdapter = adapter;
        return this;
    }

    /**
     * Enable strict event type matching.
     *
     * <p>When enabled, the DSL will check that the event type name (obtained from {@link
     * EventAdapter#getEventType(Object)}) matches the pattern name in the DSL expression.
     *
     * <p>For example, with strict matching enabled, the pattern {@code Sensor(temperature > 100)}
     * will only match events where {@code getEventType()} returns "Sensor".
     *
     * <p>Default is false (lenient matching).
     *
     * @return this builder for fluent chaining
     */
    public DslCompilerBuilder<T> withStrictTypeMatching() {
        this.strictTypeMatching = true;
        return this;
    }

    /**
     * Disable strict event type matching (default behavior).
     *
     * <p>When disabled, the DSL will not check event types, allowing patterns to match any event
     * regardless of its type.
     *
     * @return this builder for fluent chaining
     */
    public DslCompilerBuilder<T> withLenientTypeMatching() {
        this.strictTypeMatching = false;
        return this;
    }

    /**
     * Compile the DSL expression with the configured options.
     *
     * @param dslExpression The DSL pattern expression to compile
     * @param dataStream The input data stream
     * @return A PatternStream configured with the compiled pattern
     */
    public PatternStream<T> compile(String dslExpression, DataStream<T> dataStream) {
        EventAdapter<T> adapter =
                eventAdapter != null ? eventAdapter : new ReflectiveEventAdapter<>();

        return DslCompiler.compile(dslExpression, dataStream, adapter, strictTypeMatching);
    }
}
