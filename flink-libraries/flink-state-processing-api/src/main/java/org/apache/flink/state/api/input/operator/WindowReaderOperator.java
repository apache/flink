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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.state.api.input.operator.window.WindowContents;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link StateReaderOperator} for reading {@code WindowOperator} state.
 *
 * @param <S> The state type.
 * @param <KEY> The key type.
 * @param <IN> The type read from state.
 * @param <W> The window type.
 * @param <OUT> The output type of the reader.
 */
@Internal
public class WindowReaderOperator<S extends State, KEY, IN, W extends Window, OUT>
        extends StateReaderOperator<WindowReaderFunction<IN, OUT, KEY, W>, KEY, W, OUT> {

    private static final String WINDOW_STATE_NAME = "window-contents";

    private static final String WINDOW_TIMER_NAME = "window-timers";

    private final WindowContents<S, IN> contents;

    private final StateDescriptor<S, ?> descriptor;

    private transient Context ctx;

    public static <KEY, T, W extends Window, OUT> WindowReaderOperator<?, KEY, T, W, OUT> reduce(
            ReduceFunction<T> function,
            WindowReaderFunction<T, OUT, KEY, W> reader,
            TypeInformation<KEY> keyType,
            TypeSerializer<W> windowSerializer,
            TypeInformation<T> inputType) {

        StateDescriptor<ReducingState<T>, T> descriptor =
                new ReducingStateDescriptor<>(WINDOW_STATE_NAME, function, inputType);
        return new WindowReaderOperator<>(
                reader, keyType, windowSerializer, WindowContents.reducingState(), descriptor);
    }

    public static <KEY, T, ACC, R, OUT, W extends Window>
            WindowReaderOperator<?, KEY, R, W, OUT> aggregate(
                    AggregateFunction<T, ACC, R> function,
                    WindowReaderFunction<R, OUT, KEY, W> readerFunction,
                    TypeInformation<KEY> keyType,
                    TypeSerializer<W> windowSerializer,
                    TypeInformation<ACC> accumulatorType) {

        StateDescriptor<AggregatingState<T, R>, ACC> descriptor =
                new AggregatingStateDescriptor<>(WINDOW_STATE_NAME, function, accumulatorType);
        return new WindowReaderOperator<>(
                readerFunction,
                keyType,
                windowSerializer,
                WindowContents.aggregatingState(),
                descriptor);
    }

    public static <KEY, T, W extends Window, OUT> WindowReaderOperator<?, KEY, T, W, OUT> process(
            WindowReaderFunction<T, OUT, KEY, W> readerFunction,
            TypeInformation<KEY> keyType,
            TypeSerializer<W> windowSerializer,
            TypeInformation<T> stateType) {

        StateDescriptor<ListState<T>, List<T>> descriptor =
                new ListStateDescriptor<>(WINDOW_STATE_NAME, stateType);
        return new WindowReaderOperator<>(
                readerFunction, keyType, windowSerializer, WindowContents.listState(), descriptor);
    }

    public static <KEY, T, W extends Window, OUT>
            WindowReaderOperator<?, KEY, StreamRecord<T>, W, OUT> evictingWindow(
                    WindowReaderFunction<StreamRecord<T>, OUT, KEY, W> readerFunction,
                    TypeInformation<KEY> keyType,
                    TypeSerializer<W> windowSerializer,
                    TypeInformation<T> stateType,
                    ExecutionConfig config) {

        @SuppressWarnings({"unchecked", "rawtypes"})
        TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                (TypeSerializer<StreamRecord<T>>)
                        new StreamElementSerializer(
                                stateType.createSerializer(config.getSerializerConfig()));

        StateDescriptor<ListState<StreamRecord<T>>, List<StreamRecord<T>>> descriptor =
                new ListStateDescriptor<>(WINDOW_STATE_NAME, streamRecordSerializer);

        return new WindowReaderOperator<>(
                readerFunction, keyType, windowSerializer, WindowContents.listState(), descriptor);
    }

    private WindowReaderOperator(
            WindowReaderFunction<IN, OUT, KEY, W> function,
            TypeInformation<KEY> keyType,
            TypeSerializer<W> namespaceSerializer,
            WindowContents<S, IN> contents,
            StateDescriptor<S, ?> descriptor) {
        super(function, keyType, namespaceSerializer);

        Preconditions.checkNotNull(contents, "WindowContents must not be null");
        Preconditions.checkNotNull(descriptor, "The state descriptor must not be null");

        this.contents = contents;
        this.descriptor = descriptor;
    }

    @Override
    public void open() throws Exception {
        super.open();

        ctx =
                new Context(
                        registerTimers(
                                getInternalTimerService(WINDOW_TIMER_NAME),
                                WINDOW_TIMER_NAME,
                                namespace -> true));
    }

    @Override
    public void processElement(KEY key, W namespace, Collector<OUT> out) throws Exception {
        ctx.window = namespace;
        S state =
                getKeyedStateBackend()
                        .getPartitionedState(namespace, namespaceSerializer, descriptor);
        function.readWindow(key, ctx, contents.contents(state), out);
    }

    @Override
    public CloseableIterator<Tuple3<KEY, W, Integer>> getKeysAndNamespaces(
            SavepointRuntimeContext ctx) throws Exception {
        // getKeysAndNamespaces(String) does not expose the physically stored key-group.
        Stream<Tuple3<KEY, W, Integer>> keysAndWindows =
                getKeyedStateBackend()
                        .<W>getKeysAndNamespaces(descriptor.getName())
                        .map(t -> Tuple3.<KEY, W, Integer>of(t.f0, t.f1, UNKNOWN_KEY_GROUP));

        return new IteratorWithRemove<>(keysAndWindows);
    }

    private class Context implements WindowReaderFunction.Context<W> {

        W window;

        final PerWindowKeyedStateStore perWindowKeyedStateStore;

        final DefaultKeyedStateStore keyedStateStore;

        final TimerRegistration timerRegistration;

        private Context(TimerRegistration timerRegistration) {
            KeyedStateBackend<KEY> keyedStateBackend = getKeyedStateBackend();
            keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getSerializerFactory());
            perWindowKeyedStateStore = new PerWindowKeyedStateStore(keyedStateBackend);
            this.timerRegistration = timerRegistration;
        }

        @Override
        public W window() {
            return window;
        }

        @Override
        public <TS extends State> TS triggerState(StateDescriptor<TS, ?> descriptor) {
            try {
                return getKeyedStateBackend()
                        .getPartitionedState(window, namespaceSerializer, descriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve trigger state", e);
            }
        }

        @Override
        public KeyedStateStore windowState() {
            perWindowKeyedStateStore.window = window;
            return perWindowKeyedStateStore;
        }

        @Override
        public KeyedStateStore globalState() {
            return keyedStateStore;
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

    private class PerWindowKeyedStateStore extends DefaultKeyedStateStore {

        W window;

        PerWindowKeyedStateStore(KeyedStateBackend<?> keyedStateBackend) {
            super(keyedStateBackend, WindowReaderOperator.this.getSerializerFactory());
        }

        @Override
        protected <SS extends State> SS getPartitionedState(StateDescriptor<SS, ?> stateDescriptor)
                throws Exception {
            return keyedStateBackend.getPartitionedState(
                    window, namespaceSerializer, stateDescriptor);
        }
    }

    private static class IteratorWithRemove<T> implements CloseableIterator<T> {

        private final Iterator<T> iterator;

        private final AutoCloseable resource;

        private IteratorWithRemove(Stream<T> stream) {
            this.iterator = stream.iterator();
            this.resource = stream;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }

        @Override
        public void remove() {}

        @Override
        public void close() throws Exception {
            resource.close();
        }
    }
}
