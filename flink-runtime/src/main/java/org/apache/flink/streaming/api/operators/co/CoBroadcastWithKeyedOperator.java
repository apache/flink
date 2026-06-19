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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction.ReadOnlyContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TwoInputStreamOperator} for executing {@link KeyedBroadcastProcessFunction
 * KeyedBroadcastProcessFunctions}.
 *
 * @param <KS> The key type of the input keyed stream.
 * @param <IN1> The input type of the keyed (non-broadcast) side.
 * @param <IN2> The input type of the broadcast side.
 * @param <OUT> The output type of the operator.
 */
@Internal
public class CoBroadcastWithKeyedOperator<KS, IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable<KS, VoidNamespace> {

    private static final long serialVersionUID = 5926499536290284870L;

    private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

    private transient TimestampedCollector<OUT> collector;

    private transient Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates;

    private transient ReadWriteContextImpl rwContext;

    private transient ReadOnlyContextImpl rContext;

    private transient OnTimerContextImpl onTimerContext;

    public CoBroadcastWithKeyedOperator(
            final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
        super(function);
        this.broadcastStateDescriptors = Preconditions.checkNotNull(broadcastStateDescriptors);
    }

    @Override
    public void open() throws Exception {
        super.open();

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        collector = new TimestampedCollector<>(output);

        this.broadcastStates =
                CollectionUtil.newHashMapWithExpectedSize(broadcastStateDescriptors.size());
        for (MapStateDescriptor<?, ?> descriptor : broadcastStateDescriptors) {
            broadcastStates.put(
                    descriptor, getOperatorStateBackend().getBroadcastState(descriptor));
        }

        rwContext =
                new ReadWriteContextImpl(
                        getExecutionConfig(),
                        getKeyedStateBackend(),
                        userFunction,
                        broadcastStates,
                        timerService);
        rContext =
                new ReadOnlyContextImpl(
                        getExecutionConfig(), userFunction, broadcastStates, timerService);
        onTimerContext =
                new OnTimerContextImpl(
                        getExecutionConfig(), userFunction, broadcastStates, timerService);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        rContext.setElement(element);
        userFunction.processElement(element.getValue(), rContext, collector);
        rContext.setElement(null);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        rwContext.setElement(element);
        userFunction.processBroadcastElement(element.getValue(), rwContext, collector);
        rwContext.setElement(null);
    }

    @Override
    public void onEventTime(InternalTimer<KS, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    @Override
    public void onProcessingTime(InternalTimer<KS, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    private class ReadWriteContextImpl
            extends KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>.Context {

        private final ExecutionConfig config;

        private final KeyedStateBackend<KS> keyedStateBackend;

        private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

        private final TimerService timerService;

        private StreamRecord<IN2> element;

        ReadWriteContextImpl(
                final ExecutionConfig executionConfig,
                final KeyedStateBackend<KS> keyedStateBackend,
                final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
                final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
                final TimerService timerService) {

            function.super();
            this.config = Preconditions.checkNotNull(executionConfig);
            this.keyedStateBackend = Preconditions.checkNotNull(keyedStateBackend);
            this.states = Preconditions.checkNotNull(broadcastStates);
            this.timerService = Preconditions.checkNotNull(timerService);
        }

        void setElement(StreamRecord<IN2> e) {
            this.element = e;
        }

        @Override
        public Long timestamp() {
            checkState(element != null);
            return element.getTimestamp();
        }

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) {
            Preconditions.checkNotNull(stateDescriptor);

            stateDescriptor.initializeSerializerUnlessSet(config);
            BroadcastState<K, V> state = (BroadcastState<K, V>) states.get(stateDescriptor);
            if (state == null) {
                throw new IllegalArgumentException(
                        "The requested state does not exist. "
                                + "Check for typos in your state descriptor, or specify the state descriptor "
                                + "in the datastream.broadcast(...) call if you forgot to register it.");
            }
            return state;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            checkArgument(outputTag != null, "OutputTag must not be null.");
            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return timerService.currentWatermark();
        }

        @Override
        public <VS, S extends State> void applyToKeyedState(
                final StateDescriptor<S, VS> stateDescriptor,
                final KeyedStateFunction<KS, S> function)
                throws Exception {

            keyedStateBackend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    Preconditions.checkNotNull(stateDescriptor),
                    Preconditions.checkNotNull(function));
        }
    }

    private class ReadOnlyContextImpl extends ReadOnlyContext {

        private final ExecutionConfig config;

        private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

        private final TimerService timerService;

        private StreamRecord<IN1> element;

        ReadOnlyContextImpl(
                final ExecutionConfig executionConfig,
                final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
                final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
                final TimerService timerService) {

            function.super();
            this.config = Preconditions.checkNotNull(executionConfig);
            this.states = Preconditions.checkNotNull(broadcastStates);
            this.timerService = Preconditions.checkNotNull(timerService);
        }

        void setElement(StreamRecord<IN1> e) {
            this.element = e;
        }

        @Override
        public Long timestamp() {
            checkState(element != null);
            return element.hasTimestamp() ? element.getTimestamp() : null;
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return timerService.currentWatermark();
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            checkArgument(outputTag != null, "OutputTag must not be null.");
            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        public <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) {
            Preconditions.checkNotNull(stateDescriptor);

            stateDescriptor.initializeSerializerUnlessSet(config);
            ReadOnlyBroadcastState<K, V> state =
                    (ReadOnlyBroadcastState<K, V>) states.get(stateDescriptor);
            if (state == null) {
                throw new IllegalArgumentException(
                        "The requested state does not exist. "
                                + "Check for typos in your state descriptor, or specify the state descriptor "
                                + "in the datastream.broadcast(...) call if you forgot to register it.");
            }
            return state;
        }

        @Override
        @SuppressWarnings("unchecked")
        public KS getCurrentKey() {
            return (KS) CoBroadcastWithKeyedOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl
            extends KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>.OnTimerContext {

        private final ExecutionConfig config;

        private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<KS, VoidNamespace> timer;

        OnTimerContextImpl(
                final ExecutionConfig executionConfig,
                final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
                final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
                final TimerService timerService) {

            function.super();
            this.config = Preconditions.checkNotNull(executionConfig);
            this.states = Preconditions.checkNotNull(broadcastStates);
            this.timerService = Preconditions.checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(timer != null);
            return timer.getTimestamp();
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain != null);
            return timeDomain;
        }

        @Override
        public KS getCurrentKey() {
            return timer.getKey();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return timerService.currentWatermark();
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            checkArgument(outputTag != null, "OutputTag must not be null.");
            output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
        }

        @Override
        public <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) {
            Preconditions.checkNotNull(stateDescriptor);

            stateDescriptor.initializeSerializerUnlessSet(config);
            ReadOnlyBroadcastState<K, V> state =
                    (ReadOnlyBroadcastState<K, V>) states.get(stateDescriptor);
            if (state == null) {
                throw new IllegalArgumentException(
                        "The requested state does not exist. "
                                + "Check for typos in your state descriptor, or specify the state descriptor "
                                + "in the datastream.broadcast(...) call if you forgot to register it.");
            }
            return state;
        }
    }
}
