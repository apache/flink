/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.asyncprocessing.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.functions.DeclaringAsyncKeyedCoProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.runtime.asyncprocessing.operators.TimestampedCollectorWithDeclaredVariable;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.ThrowingConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed {@link
 * KeyedCoProcessFunction KeyedCoProcessFunction}.
 */
@Internal
public class AsyncKeyedCoProcessOperator<K, IN1, IN2, OUT>
        extends AbstractAsyncStateUdfStreamOperator<OUT, KeyedCoProcessFunction<K, IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    // Shared timestamp variable for collector, context and onTimerContext.
    private transient DeclaredVariable<Long> sharedTimestamp;

    private transient TimestampedCollectorWithDeclaredVariable<OUT> collector;

    private transient ContextImpl<K, IN1, IN2, OUT> context;

    private transient OnTimerContextImpl<K, IN1, IN2, OUT> onTimerContext;

    private transient ThrowingConsumer<IN1, Exception> processor1;
    private transient ThrowingConsumer<IN2, Exception> processor2;
    private transient ThrowingConsumer<Long, Exception> timerProcessor;

    public AsyncKeyedCoProcessOperator(
            KeyedCoProcessFunction<K, IN1, IN2, OUT> keyedCoProcessFunction) {
        super(keyedCoProcessFunction);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void open() throws Exception {
        super.open();
        sharedTimestamp =
                declarationContext.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncCoKeyedProcessOperator$sharedTimestamp",
                        null);

        collector = new TimestampedCollectorWithDeclaredVariable<>(output, sharedTimestamp);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl<>(userFunction, timerService, sharedTimestamp);
        onTimerContext = new OnTimerContextImpl<>(userFunction, timerService, declarationContext);
        if (userFunction instanceof DeclaringAsyncKeyedCoProcessFunction) {
            DeclaringAsyncKeyedCoProcessFunction declaringFunction =
                    (DeclaringAsyncKeyedCoProcessFunction) userFunction;
            declaringFunction.declareVariables(declarationContext);
            processor1 = declaringFunction.declareProcess1(declarationContext, context, collector);
            processor2 = declaringFunction.declareProcess2(declarationContext, context, collector);
            timerProcessor =
                    declaringFunction.declareOnTimer(declarationContext, onTimerContext, collector);
        } else {
            processor1 = (in) -> userFunction.processElement1(in, context, collector);
            processor2 = (in) -> userFunction.processElement2(in, context, collector);
            timerProcessor = (in) -> userFunction.onTimer(in, onTimerContext, collector);
        }
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestamp(element);
        processor1.accept(element.getValue());
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestamp(element);
        processor2.accept(element.getValue());
    }

    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.setAbsoluteTimestamp(timer.getTimestamp());
        invokeUserFunction(TimeDomain.EVENT_TIME, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
        collector.eraseTimestamp();
        invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<K, VoidNamespace> timer)
            throws Exception {
        onTimerContext.setTime(timer.getTimestamp(), timeDomain);
        timerProcessor.accept(timer.getTimestamp());
    }

    public class ContextImpl<K, IN1, IN2, OUT>
            extends KeyedCoProcessFunction<K, IN1, IN2, OUT>.Context {

        private final TimerService timerService;

        private final DeclaredVariable<Long> timestamp;

        ContextImpl(
                KeyedCoProcessFunction<K, IN1, IN2, OUT> function,
                TimerService timerService,
                DeclaredVariable<Long> timestamp) {
            function.super();
            this.timerService = checkNotNull(timerService);
            this.timestamp = timestamp;
        }

        @Override
        public Long timestamp() {
            return timestamp.get();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, timestamp.get()));
        }

        @Override
        public K getCurrentKey() {
            return (K) AsyncKeyedCoProcessOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl<K, IN1, IN2, OUT>
            extends KeyedCoProcessFunction<K, IN1, IN2, OUT>.OnTimerContext {

        private final TimerService timerService;

        private final DeclaredVariable<String> timeDomain;

        private final DeclaredVariable<Long> timestamp;

        OnTimerContextImpl(
                KeyedCoProcessFunction<K, IN1, IN2, OUT> function,
                TimerService timerService,
                DeclarationContext declarationContext) {
            function.super();
            this.timerService = checkNotNull(timerService);
            this.timeDomain =
                    declarationContext.declareVariable(
                            StringSerializer.INSTANCE, "_OnTimerContextImpl$timeDomain", null);
            this.timestamp =
                    declarationContext.declareVariable(
                            LongSerializer.INSTANCE, "_OnTimerContextImpl$timestamp", null);
        }

        public void setTime(long time, TimeDomain one) {
            timestamp.set(time);
            timeDomain.set(one.name());
        }

        @Override
        public Long timestamp() {
            checkState(timestamp.get() != null);
            return timestamp.get();
        }

        @Override
        public TimerService timerService() {
            return timerService;
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }

            output.collect(outputTag, new StreamRecord<>(value, timestamp()));
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain.get() != null);
            return TimeDomain.valueOf(timeDomain.get());
        }

        @Override
        public K getCurrentKey() {
            return (K) AsyncKeyedCoProcessOperator.this.getCurrentKey();
        }
    }
}
