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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed
 * {@link CoProcessFunction CoProcessFunctions}.
 */
@Internal
public class KeyedCoProcessOperator<K, IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, CoProcessFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<OUT> collector;

	private transient ContextImpl<IN1, IN2, OUT> context;

	private transient OnTimerContextImpl<IN1, IN2, OUT> onTimerContext;

	public KeyedCoProcessOperator(CoProcessFunction<IN1, IN2, OUT> flatMapper) {
		super(flatMapper);
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		InternalTimerService<VoidNamespace> internalTimerService =
				getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		TimerService timerService = new SimpleTimerService(internalTimerService);

		context = new ContextImpl<>(userFunction, timerService);
		onTimerContext = new OnTimerContextImpl<>(userFunction, timerService);
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		collector.setTimestamp(element);
		context.element = element;
		userFunction.processElement1(element.getValue(), context, collector);
		context.element = null;
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		collector.setTimestamp(element);
		context.element = element;
		userFunction.processElement2(element.getValue(), context, collector);
		context.element = null;
	}

	@Override
	public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
		onTimerContext.timer = timer;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	@Override
	public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.eraseTimestamp();
		onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
		onTimerContext.timer = timer;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	protected TimestampedCollector<OUT> getCollector() {
		return collector;
	}

	private class ContextImpl<IN1, IN2, OUT> extends CoProcessFunction<IN1, IN2, OUT>.Context {

		private final TimerService timerService;

		private StreamRecord<?> element;

		ContextImpl(CoProcessFunction<IN1, IN2, OUT> function, TimerService timerService) {
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
	}

	private class OnTimerContextImpl<IN1, IN2, OUT> extends CoProcessFunction<IN1, IN2, OUT>.OnTimerContext {

		private final TimerService timerService;

		private TimeDomain timeDomain;

		private InternalTimer<?, VoidNamespace> timer;

		OnTimerContextImpl(CoProcessFunction<IN1, IN2, OUT> function, TimerService timerService) {
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
	}
}
