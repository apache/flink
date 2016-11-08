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
import org.apache.flink.streaming.api.functions.TimelyFlatMapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@Internal
public class StreamTimelyFlatMap<K, IN, OUT>
		extends AbstractUdfStreamOperator<OUT, TimelyFlatMapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<OUT> collector;

	private transient TimerService timerService;

	private transient ContextImpl<IN> context;

	private transient OnTimerContextImpl onTimerContext;

	public StreamTimelyFlatMap(TimelyFlatMapFunction<IN, OUT> flatMapper) {
		super(flatMapper);

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		InternalTimerService<VoidNamespace> internalTimerService =
				getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		this.timerService = new SimpleTimerService(internalTimerService);

		context = new ContextImpl<>(timerService);
		onTimerContext = new OnTimerContextImpl(timerService);
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
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
		onTimerContext.timer = timer;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		context.element = element;
		userFunction.flatMap(element.getValue(), context, collector);
		context.element = null;
	}

	private static class ContextImpl<T> implements TimelyFlatMapFunction.Context {

		private final TimerService timerService;

		private StreamRecord<T> element;

		ContextImpl(TimerService timerService) {
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
	}

	private static class OnTimerContextImpl implements TimelyFlatMapFunction.OnTimerContext{

		private final TimerService timerService;

		private TimeDomain timeDomain;

		private InternalTimer<?, VoidNamespace> timer;

		OnTimerContextImpl(TimerService timerService) {
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public TimeDomain timeDomain() {
			checkState(timeDomain != null);
			return timeDomain;
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
	}
}
