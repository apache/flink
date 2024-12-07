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
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.Context;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TwoInputStreamOperator} for executing {@link BroadcastProcessFunction
 * BroadcastProcessFunctions}.
 *
 * @param <IN1> The input type of the keyed (non-broadcast) side.
 * @param <IN2> The input type of the broadcast side.
 * @param <OUT> The output type of the operator.
 */
@Internal
public class CoBroadcastWithNonKeyedOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, BroadcastProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = -1869740381935471752L;

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private long currentWatermark = Long.MIN_VALUE;

    private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

    private transient TimestampedCollector<OUT> collector;

    private transient Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates;

    private transient ReadWriteContextImpl rwContext;

    private transient ReadOnlyContextImpl rContext;

    public CoBroadcastWithNonKeyedOperator(
            final BroadcastProcessFunction<IN1, IN2, OUT> function,
            final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
        super(function);
        this.broadcastStateDescriptors = Preconditions.checkNotNull(broadcastStateDescriptors);
    }

    @Override
    public void open() throws Exception {
        super.open();

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
                        userFunction,
                        broadcastStates,
                        getProcessingTimeService());
        rContext =
                new ReadOnlyContextImpl(
                        getExecutionConfig(),
                        userFunction,
                        broadcastStates,
                        getProcessingTimeService());
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
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    private class ReadWriteContextImpl extends Context {

        private final ExecutionConfig config;

        private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

        private final ProcessingTimeService timerService;

        private StreamRecord<IN2> element;

        ReadWriteContextImpl(
                final ExecutionConfig executionConfig,
                final BroadcastProcessFunction<IN1, IN2, OUT> function,
                final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
                final ProcessingTimeService timerService) {

            function.super();
            this.config = Preconditions.checkNotNull(executionConfig);
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
            return timerService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }
    }

    private class ReadOnlyContextImpl
            extends BroadcastProcessFunction<IN1, IN2, OUT>.ReadOnlyContext {

        private final ExecutionConfig config;

        private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

        private final ProcessingTimeService timerService;

        private StreamRecord<IN1> element;

        ReadOnlyContextImpl(
                final ExecutionConfig executionConfig,
                final BroadcastProcessFunction<IN1, IN2, OUT> function,
                final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
                final ProcessingTimeService timerService) {

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
        public <X> void output(OutputTag<X> outputTag, X value) {
            checkArgument(outputTag != null, "OutputTag must not be null.");
            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        public long currentProcessingTime() {
            return timerService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
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
