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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.util.Collector;

/**
 * Processes (key, namespace) pairs from a restored operator's namespaced (e.g. window-scoped)
 * state, invoking {@link #readKey} once per pair.
 *
 * <p><b>NOTE:</b> Unlike {@link KeyedStateReaderFunction}, state must NOT be accessed via {@link
 * #getRuntimeContext()}: the runtime context is always bound to {@code VoidNamespace}, which is
 * wrong for namespace-varying reads. Use {@link Context#getState(StateDescriptor)} instead, which
 * is bound to the namespace of the (key, namespace) pair currently being processed.
 */
@Internal
public abstract class WindowKeyedStateReaderFunction<K, OUT> extends AbstractRichFunction {

    private static final long serialVersionUID = 1L;

    public abstract void readKey(K key, Object namespace, Context ctx, Collector<OUT> out)
            throws Exception;

    /**
     * Context that {@link WindowKeyedStateReaderFunction}s can use to access state scoped to the
     * (key, namespace) pair currently being processed. Only valid for the duration of a single
     * {@link #readKey} call.
     */
    public interface Context {

        <S extends State> S getState(StateDescriptor<S, ?> descriptor) throws Exception;
    }
}
