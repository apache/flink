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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractDataOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A subclass of {@link StreamTask} for executing the {@link SourceOperator}.
 */
@Internal
public class SourceOperatorStreamTask<T> extends StreamTask<T, SourceOperator<T, ?>> {
	private AsyncDataOutputToOutput<T> output;

	public SourceOperatorStreamTask(Environment env) throws Exception {
		super(env);
	}

	@Override
	public void init() throws Exception {
		final SourceOperator<T, ?> sourceOperator = this.mainOperator;
		// reader initialization, which cannot happen in the constructor due to the
		// lazy metric group initialization. We do this here now, rather than
		// later (in open()) so that we can access the reader when setting up the
		// input processors
		sourceOperator.initReader();

		final StreamTaskInput<T> input = new StreamTaskSourceInput<>(sourceOperator, 0, 0);

		// The SourceOperatorStreamTask doesn't have any inputs, so there is no need for
		// a WatermarkGauge on the input.
		output = new AsyncDataOutputToOutput<>(
			operatorChain.getMainOperatorOutput(),
			getStreamStatusMaintainer(),
			null);

		inputProcessor = new StreamOneInputProcessor<>(
			input,
			output,
			operatorChain);
	}

	@Override
	protected void advanceToEndOfEventTime() {
		output.emitWatermark(Watermark.MAX_WATERMARK);
	}

	@Override
	protected void afterInvoke() throws Exception {
		if (!isCanceled()) {
			advanceToEndOfEventTime();
		}
		super.afterInvoke();
	}

	/**
	 * Implementation of {@link DataOutput} that wraps a specific {@link Output}.
	 */
	public static class AsyncDataOutputToOutput<T> extends AbstractDataOutput<T> {

		private final Output<StreamRecord<T>> output;
		@Nullable private final WatermarkGauge inputWatermarkGauge;

		public AsyncDataOutputToOutput(
				Output<StreamRecord<T>> output,
				StreamStatusMaintainer streamStatusMaintainer,
				@Nullable WatermarkGauge inputWatermarkGauge) {
			super(streamStatusMaintainer);

			this.output = checkNotNull(output);
			this.inputWatermarkGauge = inputWatermarkGauge;
		}

		@Override
		public void emitRecord(StreamRecord<T> streamRecord) {
			output.collect(streamRecord);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			output.emitLatencyMarker(latencyMarker);
		}

		@Override
		public void emitWatermark(Watermark watermark) {
			if (inputWatermarkGauge != null) {
				inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			}
			output.emitWatermark(watermark);
		}
	}
}
