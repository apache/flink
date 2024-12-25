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

package org.apache.flink.runtime.asyncprocessing.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.streaming.api.AsyncKeyedProcessFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.function.ThrowingConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link StreamOperator} for executing {@link AsyncKeyedProcessFunction}. */
@Internal
public class AsyncKeyedProcessOperator<K, IN, OUT>
        extends AbstractAsyncStateUdfStreamOperator<OUT, AsyncKeyedProcessFunction<K, IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    // Shared timestamp variable for collector, context and onTimerContext.
    private transient DeclaredVariable<Long> sharedTimestamp;

    private transient TimestampedCollectorWithDeclaredVariable<OUT> collector;

    private transient ContextImpl context;

    private transient OnTimerContextImpl onTimerContext;

    private transient DeclarationContext declarationContext;

    private transient ThrowingConsumer<IN, Exception> processor;
    private transient ThrowingConsumer<Long, Exception> timerProcessor;

    public AsyncKeyedProcessOperator(AsyncKeyedProcessFunction<K, IN, OUT> function) {
        super(function);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void open() throws Exception {
        super.open();
        declarationContext = new DeclarationContext(getDeclarationManager());
        userFunction.open(declarationContext);
        sharedTimestamp =
                declarationContext.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncKeyedProcessOperator$sharedTimestamp",
                        null);

        collector = new TimestampedCollectorWithDeclaredVariable<>(output, sharedTimestamp);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl(userFunction, timerService, sharedTimestamp);
        onTimerContext =
                new OnTimerContextImpl(
                        userFunction, timerService, declarationContext, sharedTimestamp);

        processor = userFunction.declareProcess(declarationContext, context, collector);
        timerProcessor = userFunction.declareOnTimer(declarationContext, onTimerContext, collector);
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

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        processor.accept(element.getValue());
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<K, VoidNamespace> timer)
            throws Exception {
        onTimerContext.setTimeDomain(timeDomain);
        sharedTimestamp.set(timer.getTimestamp());
        timerProcessor.accept(timer.getTimestamp());
    }

    private class ContextImpl extends AsyncKeyedProcessFunction<K, IN, OUT>.Context {

        private final TimerService timerService;

        private final DeclaredVariable<Long> timestamp;

        ContextImpl(
                AsyncKeyedProcessFunction<K, IN, OUT> function,
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
        @SuppressWarnings("unchecked")
        public K getCurrentKey() {
            return (K) AsyncKeyedProcessOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl extends AsyncKeyedProcessFunction<K, IN, OUT>.OnTimerContext {

        private final TimerService timerService;

        private final DeclaredVariable<String> timeDomain;

        private final DeclaredVariable<Long> timestamp;

        OnTimerContextImpl(
                AsyncKeyedProcessFunction<K, IN, OUT> function,
                TimerService timerService,
                DeclarationContext declarationContext,
                DeclaredVariable<Long> timestamp) {
            function.super();
            this.timerService = checkNotNull(timerService);
            this.timeDomain =
                    declarationContext.declareVariable(
                            StringSerializer.INSTANCE, "_OnTimerContextImpl$timeDomain", null);
            this.timestamp = timestamp;
        }

        public void setTimeDomain(TimeDomain one) {
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
        @SuppressWarnings("unchecked")
        public K getCurrentKey() {
            return (K) AsyncKeyedProcessOperator.this.getCurrentKey();
        }
    }
}
