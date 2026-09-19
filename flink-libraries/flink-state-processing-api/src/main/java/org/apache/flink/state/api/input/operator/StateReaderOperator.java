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
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.state.api.runtime.VoidTriggerable;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Base class for executing functions that read keyed state.
 *
 * @param <F> The type of the user function.
 * @param <KEY> The key type.
 * @param <N> The namespace type.
 * @param <OUT> The output type.
 */
@Internal
public abstract class StateReaderOperator<F extends Function, KEY, N, OUT>
        implements KeyContext, AutoCloseable, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Sentinel for {@link #setCurrentKeyAndKeyGroup} meaning the key-group is unknown, in which
     * case it is derived from {@code key.hashCode()} instead. Used by backend lookup APIs that do
     * not expose the physically stored key-group.
     */
    public static final int UNKNOWN_KEY_GROUP = -1;

    private static final String EVENT_TIMER_STATE = "event-time-timers";

    private static final String PROC_TIMER_STATE = "proc-time-timers";

    protected final F function;

    private final TypeInformation<KEY> keyType;

    protected final TypeSerializer<N> namespaceSerializer;

    private transient SerializerFactory serializerFactory;

    private transient KeyedStateBackend<KEY> keyedStateBackend;

    private transient TypeSerializer<KEY> keySerializer;

    private transient InternalTimeServiceManager<KEY> timerServiceManager;

    protected StateReaderOperator(
            F function, TypeInformation<KEY> keyType, TypeSerializer<N> namespaceSerializer) {
        Preconditions.checkNotNull(function, "The user function must not be null");
        Preconditions.checkNotNull(keyType, "The key type must not be null");
        Preconditions.checkNotNull(
                namespaceSerializer, "The namespace serializer must not be null");

        this.function = function;
        this.keyType = keyType;
        this.namespaceSerializer = namespaceSerializer;
    }

    public abstract void processElement(KEY key, N namespace, Collector<OUT> out) throws Exception;

    public abstract CloseableIterator<Tuple3<KEY, N, Integer>> getKeysAndNamespaces(
            SavepointRuntimeContext ctx) throws Exception;

    public final void setup(
            SerializerFactory serializerFactory,
            KeyedStateBackend<KEY> keyKeyedStateBackend,
            InternalTimeServiceManager<KEY> timerServiceManager,
            SavepointRuntimeContext ctx) {

        this.serializerFactory = serializerFactory;
        this.keyedStateBackend = keyKeyedStateBackend;
        this.timerServiceManager = timerServiceManager;
        this.keySerializer = serializerFactory.createSerializer(keyType);

        FunctionUtils.setFunctionRuntimeContext(function, ctx);
    }

    protected final InternalTimerService<N> getInternalTimerService(String name) {
        return timerServiceManager.getInternalTimerService(
                name, keySerializer, namespaceSerializer, VoidTriggerable.instance());
    }

    /**
     * Snapshots the timers currently registered in {@code timerService} into keyed list state under
     * {@code timerStateName} and returns a {@link TimerRegistration} exposing them.
     *
     * <p>Only timers whose namespace matches {@code namespaceFilter} are snapshotted: namespaced
     * readers (e.g. window state) and non-namespaced ones (plain keyed state, registered under
     * {@link org.apache.flink.runtime.state.VoidNamespace}) differ in which timers belong to the
     * state being read.
     */
    protected final TimerRegistration registerTimers(
            InternalTimerService<N> timerService,
            String timerStateName,
            Predicate<N> namespaceFilter)
            throws Exception {
        ListState<Long> eventTimers =
                keyedStateBackend.getPartitionedState(
                        timerStateName,
                        StringSerializer.INSTANCE,
                        new ListStateDescriptor<>(EVENT_TIMER_STATE, Types.LONG));

        timerService.forEachEventTimeTimer(
                (namespace, timer) -> {
                    if (namespaceFilter.test(namespace)) {
                        eventTimers.add(timer);
                    }
                });

        ListState<Long> procTimers =
                keyedStateBackend.getPartitionedState(
                        timerStateName,
                        StringSerializer.INSTANCE,
                        new ListStateDescriptor<>(PROC_TIMER_STATE, Types.LONG));

        timerService.forEachProcessingTimeTimer(
                (namespace, timer) -> {
                    if (namespaceFilter.test(namespace)) {
                        procTimers.add(timer);
                    }
                });

        return new TimerRegistration(eventTimers, procTimers);
    }

    /** Read-only view over the timers snapshotted by {@link #registerTimers}. */
    protected static final class TimerRegistration {

        private final ListState<Long> eventTimers;

        private final ListState<Long> procTimers;

        private TimerRegistration(ListState<Long> eventTimers, ListState<Long> procTimers) {
            this.eventTimers = eventTimers;
            this.procTimers = procTimers;
        }

        public Set<Long> registeredEventTimeTimers() throws Exception {
            return toSet(eventTimers.get());
        }

        public Set<Long> registeredProcessingTimeTimers() throws Exception {
            return toSet(procTimers.get());
        }

        private static Set<Long> toSet(Iterable<Long> timers) {
            if (timers == null) {
                return Collections.emptySet();
            }
            return StreamSupport.stream(timers.spliterator(), false).collect(Collectors.toSet());
        }
    }

    public void open() throws Exception {
        FunctionUtils.openFunction(function, DefaultOpenContext.INSTANCE);
    }

    public void close() throws Exception {
        Exception exception = null;

        try {
            FunctionUtils.closeFunction(function);
        } catch (Exception e) {
            // The state backend must always be closed
            // to release native resources.
            exception = e;
        }

        if (keyedStateBackend != null) {
            keyedStateBackend.dispose();
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void setCurrentKey(Object key) {
        keyedStateBackend.setCurrentKey((KEY) key);
    }

    /** Restores the reading context for the given key. See {@link #UNKNOWN_KEY_GROUP}. */
    @SuppressWarnings("unchecked")
    public final void setCurrentKeyAndKeyGroup(Object key, int keyGroup) {
        if (keyGroup == UNKNOWN_KEY_GROUP) {
            keyedStateBackend.setCurrentKey((KEY) key);
        } else {
            keyedStateBackend.setCurrentKeyAndKeyGroup((KEY) key, keyGroup);
        }
    }

    @Override
    public final Object getCurrentKey() {
        return keyedStateBackend.getCurrentKey();
    }

    public final KeyedStateBackend<KEY> getKeyedStateBackend() {
        return keyedStateBackend;
    }

    public final TypeInformation<KEY> getKeyType() {
        return keyType;
    }

    public final SerializerFactory getSerializerFactory() {
        return this.serializerFactory;
    }
}
