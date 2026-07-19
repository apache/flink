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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.MultiStateKeyIterator;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Set;

/**
 * A {@link StateReaderOperator} for executing a {@link KeyedStateReaderFunction}.
 *
 * @param <KEY> The key type read from the state backend.
 * @param <OUT> The output type of the function.
 */
@Internal
public class KeyedStateReaderOperator<KEY, OUT>
        extends StateReaderOperator<KeyedStateReaderFunction<KEY, OUT>, KEY, VoidNamespace, OUT> {

    private static final String USER_TIMERS_NAME = "user-timers";

    private transient Context context;

    public KeyedStateReaderOperator(
            KeyedStateReaderFunction<KEY, OUT> function, TypeInformation<KEY> keyType) {
        super(function, keyType, VoidNamespaceSerializer.INSTANCE);
    }

    @Override
    public void open() throws Exception {
        super.open();

        TimerRegistration timerRegistration =
                registerTimers(
                        getInternalTimerService(USER_TIMERS_NAME),
                        USER_TIMERS_NAME,
                        VoidNamespace.INSTANCE::equals);
        context = new Context(timerRegistration);
    }

    @Override
    public void processElement(KEY key, VoidNamespace namespace, Collector<OUT> out)
            throws Exception {
        function.readKey(key, context, out);
    }

    @Override
    public CloseableIterator<Tuple3<KEY, VoidNamespace, Integer>> getKeysAndNamespaces(
            SavepointRuntimeContext ctx) throws Exception {
        ctx.disableStateRegistration();
        List<StateDescriptor<?, ?>> stateDescriptors = ctx.getStateDescriptors();
        MultiStateKeyIterator<KEY> keys =
                new MultiStateKeyIterator<>(stateDescriptors, getKeyedStateBackend());
        return new NamespaceDecorator<>(keys);
    }

    private static class Context implements KeyedStateReaderFunction.Context {

        private final TimerRegistration timerRegistration;

        private Context(TimerRegistration timerRegistration) {
            this.timerRegistration = timerRegistration;
        }

        @Override
        public Set<Long> registeredEventTimeTimers() throws Exception {
            return timerRegistration.registeredEventTimeTimers();
        }

        @Override
        public Set<Long> registeredProcessingTimeTimers() throws Exception {
            return timerRegistration.registeredProcessingTimeTimers();
        }
    }

    private static class NamespaceDecorator<KEY>
            implements CloseableIterator<Tuple3<KEY, VoidNamespace, Integer>> {

        private final CloseableIterator<Tuple2<KEY, Integer>> keys;

        private NamespaceDecorator(CloseableIterator<Tuple2<KEY, Integer>> keys) {
            this.keys = keys;
        }

        @Override
        public boolean hasNext() {
            return keys.hasNext();
        }

        @Override
        public Tuple3<KEY, VoidNamespace, Integer> next() {
            Tuple2<KEY, Integer> keyAndKeyGroup = keys.next();
            return Tuple3.of(keyAndKeyGroup.f0, VoidNamespace.INSTANCE, keyAndKeyGroup.f1);
        }

        @Override
        public void remove() {
            keys.remove();
        }

        @Override
        public void close() throws Exception {
            keys.close();
        }
    }
}
