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

package org.apache.flink.state.api.input.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.state.api.functions.WindowKeyedStateReaderFunction;
import org.apache.flink.state.api.input.MultiStateKeyAndNamespaceIterator;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * A {@link StateReaderOperator} for executing a {@link WindowKeyedStateReaderFunction} against
 * state registered under an arbitrary (e.g. window) namespace.
 *
 * <p>Unlike {@link KeyedStateReaderOperator}, the set of state names to read is known upfront (from
 * the calling {@code SavepointStateMapping}), so {@link #getKeysAndNamespaces} does not need to go
 * through {@code SavepointRuntimeContext}'s state-descriptor registration tracking: it builds a
 * {@link MultiStateKeyAndNamespaceIterator} directly from the configured state names.
 */
@Internal
public class WindowKeyedStateReaderOperator<KEY, OUT>
        extends StateReaderOperator<WindowKeyedStateReaderFunction<KEY, OUT>, KEY, Object, OUT> {

    private final List<String> stateNames;

    private transient Context context;

    public WindowKeyedStateReaderOperator(
            WindowKeyedStateReaderFunction<KEY, OUT> function,
            TypeInformation<KEY> keyType,
            TypeSerializer<Object> namespaceSerializer,
            List<String> stateNames) {
        super(function, keyType, namespaceSerializer);
        this.stateNames =
                Preconditions.checkNotNull(stateNames, "The state names must not be null");
    }

    @Override
    public void open() throws Exception {
        super.open();
        context = new Context();
    }

    @Override
    public void processElement(KEY key, Object namespace, Collector<OUT> out) throws Exception {
        context.namespace = namespace;
        function.readKey(key, namespace, context, out);
    }

    @Override
    public CloseableIterator<Tuple3<KEY, Object, Integer>> getKeysAndNamespaces(
            SavepointRuntimeContext ctx) throws Exception {
        return new MultiStateKeyAndNamespaceIterator<>(stateNames, getKeyedStateBackend());
    }

    private class Context implements WindowKeyedStateReaderFunction.Context {

        private Object namespace;

        @Override
        public <S extends State> S getState(StateDescriptor<S, ?> descriptor) throws Exception {
            return getKeyedStateBackend()
                    .getPartitionedState(namespace, namespaceSerializer, descriptor);
        }
    }
}
