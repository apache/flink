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
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing
 * {@link CoProcessFunction CoProcessFunctions}.
 */
@Internal
public class CoProcessOperator<IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, CoProcessFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<OUT> collector;

	private transient ContextImpl context;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private long currentWatermark = Long.MIN_VALUE;

	public CoProcessOperator(CoProcessFunction<IN1, IN2, OUT> flatMapper) {
		super(flatMapper);
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		context = new ContextImpl(userFunction, getProcessingTimeService());
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
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}

	private class ContextImpl
			extends CoProcessFunction<IN1, IN2, OUT>.Context
			implements TimerService {

		private final ProcessingTimeService timerService;

		private StreamRecord<?> element;

		ContextImpl(CoProcessFunction<IN1, IN2, OUT> function, ProcessingTimeService timerService) {
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
		public long currentProcessingTime() {
			return timerService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
		}

		@Override
		public TimerService timerService() {
			return this;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}

			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}
	}
}
