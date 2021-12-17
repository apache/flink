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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link StreamOperator} for executing {@link KeyedProcessFunction KeyedProcessFunctions}. */
@Internal
public class KeyedProcessOperator<K, IN, OUT>
        extends AbstractUdfStreamOperator<OUT, KeyedProcessFunction<K, IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

    private static final long serialVersionUID = 1L;

    private transient TimestampedCollector<OUT> collector;

    private transient ContextImpl context;

    private transient OnTimerContextImpl onTimerContext;

    public KeyedProcessOperator(KeyedProcessFunction<K, IN, OUT> function) {
        super(function);

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        collector = new TimestampedCollector<>(output);

        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

        TimerService timerService = new SimpleTimerService(internalTimerService);

        context = new ContextImpl(userFunction, timerService);
        onTimerContext = new OnTimerContextImpl(userFunction, timerService);
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
        context.element = element;
        userFunction.processElement(element.getValue(), context, collector);
        context.element = null;
    }

    private void invokeUserFunction(TimeDomain timeDomain, InternalTimer<K, VoidNamespace> timer)
            throws Exception {
        onTimerContext.timeDomain = timeDomain;
        onTimerContext.timer = timer;
        userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
        onTimerContext.timeDomain = null;
        onTimerContext.timer = null;
    }

    private class ContextImpl extends KeyedProcessFunction<K, IN, OUT>.Context {

        private final TimerService timerService;

        private StreamRecord<IN> element;

        ContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
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

            output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
        }

        @Override
        @SuppressWarnings("unchecked")
        public K getCurrentKey() {
            return (K) KeyedProcessOperator.this.getCurrentKey();
        }
    }

    private class OnTimerContextImpl extends KeyedProcessFunction<K, IN, OUT>.OnTimerContext {

        private final TimerService timerService;

        private TimeDomain timeDomain;

        private InternalTimer<K, VoidNamespace> timer;

        OnTimerContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
            function.super();
            this.timerService = checkNotNull(timerService);
        }

        @Override
        public Long timestamp() {
            checkState(timer != null);
            return timer.getTimestamp();
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

            output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
        }

        @Override
        public TimeDomain timeDomain() {
            checkState(timeDomain != null);
            return timeDomain;
        }

        @Override
        public K getCurrentKey() {
            return timer.getKey();
        }
    }
}
