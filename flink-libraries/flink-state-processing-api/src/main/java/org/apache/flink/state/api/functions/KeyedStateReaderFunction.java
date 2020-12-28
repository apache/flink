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

package org.apache.flink.state.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * A function that processes keys from a restored operator
 *
 * <p>For every key {@link #readKey(Object, Context, Collector)} is invoked. This can produce zero
 * or more elements as output.
 *
 * <p><b>NOTE:</b> State descriptors must be eagerly registered in {@code open(Configuration)}. Any
 * attempt to dynamically register states inside of {@code readKey} will result in a {@code
 * RuntimeException}.
 *
 * <p><b>NOTE:</b> A {@code KeyedStateReaderFunction} is always a {@link
 * org.apache.flink.api.common.functions.RichFunction}. Therefore, access to the {@link
 * org.apache.flink.api.common.functions.RuntimeContext} is always available and setup and teardown
 * methods can be implemented. See {@link
 * org.apache.flink.api.common.functions.RichFunction#open(Configuration)} and {@link
 * org.apache.flink.api.common.functions.RichFunction#close()}.
 *
 * @param <K> Type of the keys
 * @param <OUT> Type of the output elements.
 */
@PublicEvolving
public abstract class KeyedStateReaderFunction<K, OUT> extends AbstractRichFunction {

    private static final long serialVersionUID = 3873843034140417407L;

    /**
     * Initialization method for the function. It is called before {@link #readKey(Object, Context,
     * Collector)} and thus suitable for one time setup work.
     *
     * <p>This is the only method that my register state descriptors within a {@code
     * KeyedStateReaderFunction}.
     */
    public abstract void open(Configuration parameters) throws Exception;

    /**
     * Process one key from the restored state backend.
     *
     * <p>This function can read partitioned state from the restored state backend and output zero
     * or more elements using the {@link Collector} parameter.
     *
     * @param key The input value.
     * @param out The collector for returning result values.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public abstract void readKey(K key, Context ctx, Collector<OUT> out) throws Exception;

    /**
     * Context that {@link KeyedStateReaderFunction}'s can use for getting additional data about an
     * input record.
     *
     * <p>The context is only valid for the duration of a {@link
     * KeyedStateReaderFunction#readKey(Object, Context, Collector)} call. Do not store the context
     * and use afterwards!
     */
    public interface Context {

        /** @return All registered event time timers for the current key. */
        Set<Long> registeredEventTimeTimers() throws Exception;

        /** @return All registered processing time timers for the current key. */
        Set<Long> registeredProcessingTimeTimers() throws Exception;
    }
}
