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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stream operator that may do one or both of the following: extract timestamps from
 * events and generate watermarks.
 *
 * <p>These two responsibilities run in the same operator rather than in two different ones,
 * because the implementation of the timestamp assigner and the watermark generator is
 * frequently in the same class (and should be run in the same instance), even though the
 * separate interfaces support the use of different classes.
 *
 * @param <T> The type of the input elements
 */
public class TimestampsAndWatermarksOperator<T>
		extends AbstractStreamOperator<T>
		implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private final WatermarkStrategy<T> watermarkStrategy;

	/** The timestamp assigner. */
	private transient TimestampAssigner<T> timestampAssigner;

	/** The watermark generator, initialized during runtime. */
	private transient WatermarkGenerator<T> watermarkGenerator;

	/** The watermark output gateway, initialized during runtime. */
	private transient WatermarkOutput wmOutput;

	/** The interval (in milliseconds) for periodic watermark probes. Initialized during runtime. */
	private transient long watermarkInterval;

	public TimestampsAndWatermarksOperator(
			WatermarkStrategy<T> watermarkStrategy) {

		this.watermarkStrategy = checkNotNull(watermarkStrategy);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		timestampAssigner = watermarkStrategy.createTimestampAssigner(this::getMetricGroup);
		watermarkGenerator = watermarkStrategy.createWatermarkGenerator(this::getMetricGroup);

		wmOutput = new WatermarkEmitter(output, getContainingTask().getStreamStatusMaintainer());

		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			final long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(final StreamRecord<T> element) throws Exception {
		final T event = element.getValue();
		final long previousTimestamp = element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;
		final long newTimestamp = timestampAssigner.extractTimestamp(event, previousTimestamp);

		element.setTimestamp(newTimestamp);
		output.collect(element);
		watermarkGenerator.onEvent(event, newTimestamp, wmOutput);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		watermarkGenerator.onPeriodicEmit(wmOutput);

		final long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream, except for the "end of time" watermark.
	 */
	@Override
	public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE) {
			wmOutput.emitWatermark(Watermark.MAX_WATERMARK);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		watermarkGenerator.onPeriodicEmit(wmOutput);
	}

	// ------------------------------------------------------------------------

	/**
	 * Implementation of the {@code WatermarkEmitter}, based on the components
	 * that are available inside a stream operator.
	 */
	private static final class WatermarkEmitter implements WatermarkOutput {

		private final Output<?> output;

		private final StreamStatusMaintainer statusMaintainer;

		private long currentWatermark;

		private boolean idle;

		WatermarkEmitter(Output<?> output, StreamStatusMaintainer statusMaintainer) {
			this.output = output;
			this.statusMaintainer = statusMaintainer;
			this.currentWatermark = Long.MIN_VALUE;
		}

		@Override
		public void emitWatermark(Watermark watermark) {
			final long ts = watermark.getTimestamp();

			if (ts <= currentWatermark) {
				return;
			}

			currentWatermark = ts;

			if (idle) {
				idle = false;
				statusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
			}

			output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(ts));
		}

		@Override
		public void markIdle() {
			idle = true;
			statusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
		}
	}
}
