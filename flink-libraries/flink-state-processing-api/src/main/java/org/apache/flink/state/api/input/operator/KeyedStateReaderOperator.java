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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.input.MultiStateKeyIterator;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    private transient Context<KEY> context;

    public KeyedStateReaderOperator(
            KeyedStateReaderFunction<KEY, OUT> function, TypeInformation<KEY> keyType) {
        super(function, keyType, VoidNamespaceSerializer.INSTANCE);
    }

    @Override
    public void open() throws Exception {
        super.open();

        InternalTimerService<VoidNamespace> timerService =
                getInternalTimerService(USER_TIMERS_NAME);
        context = new Context<>(getKeyedStateBackend(), timerService);
    }

    @Override
    public void processElement(KEY key, VoidNamespace namespace, Collector<OUT> out)
            throws Exception {
        function.readKey(key, context, out);
    }

    @Override
    public CloseableIterator<Tuple2<KEY, VoidNamespace>> getKeysAndNamespaces(
            SavepointRuntimeContext ctx) throws Exception {
        ctx.disableStateRegistration();
        List<StateDescriptor<?, ?>> stateDescriptors = ctx.getStateDescriptors();
        MultiStateKeyIterator<KEY> keys =
                new MultiStateKeyIterator<>(stateDescriptors, getKeyedStateBackend());
        return new NamespaceDecorator<>(keys);
    }

    private static class Context<K> implements KeyedStateReaderFunction.Context {

        private static final String EVENT_TIMER_STATE = "event-time-timers";

        private static final String PROC_TIMER_STATE = "proc-time-timers";

        ListState<Long> eventTimers;

        ListState<Long> procTimers;

        private Context(
                KeyedStateBackend<K> keyedStateBackend,
                InternalTimerService<VoidNamespace> timerService)
                throws Exception {
            eventTimers =
                    keyedStateBackend.getPartitionedState(
                            USER_TIMERS_NAME,
                            StringSerializer.INSTANCE,
                            new ListStateDescriptor<>(EVENT_TIMER_STATE, Types.LONG));

            timerService.forEachEventTimeTimer(
                    (namespace, timer) -> {
                        if (namespace.equals(VoidNamespace.INSTANCE)) {
                            eventTimers.add(timer);
                        }
                    });

            procTimers =
                    keyedStateBackend.getPartitionedState(
                            USER_TIMERS_NAME,
                            StringSerializer.INSTANCE,
                            new ListStateDescriptor<>(PROC_TIMER_STATE, Types.LONG));

            timerService.forEachProcessingTimeTimer(
                    (namespace, timer) -> {
                        if (namespace.equals(VoidNamespace.INSTANCE)) {
                            procTimers.add(timer);
                        }
                    });
        }

        @Override
        public Set<Long> registeredEventTimeTimers() throws Exception {
            Iterable<Long> timers = eventTimers.get();
            if (timers == null) {
                return Collections.emptySet();
            }

            return StreamSupport.stream(timers.spliterator(), false).collect(Collectors.toSet());
        }

        @Override
        public Set<Long> registeredProcessingTimeTimers() throws Exception {
            Iterable<Long> timers = procTimers.get();
            if (timers == null) {
                return Collections.emptySet();
            }

            return StreamSupport.stream(timers.spliterator(), false).collect(Collectors.toSet());
        }
    }

    private static class NamespaceDecorator<KEY>
            implements CloseableIterator<Tuple2<KEY, VoidNamespace>> {

        private final CloseableIterator<KEY> keys;

        private NamespaceDecorator(CloseableIterator<KEY> keys) {
            this.keys = keys;
        }

        @Override
        public boolean hasNext() {
            return keys.hasNext();
        }

        @Override
        public Tuple2<KEY, VoidNamespace> next() {
            KEY key = keys.next();
            return Tuple2.of(key, VoidNamespace.INSTANCE);
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
