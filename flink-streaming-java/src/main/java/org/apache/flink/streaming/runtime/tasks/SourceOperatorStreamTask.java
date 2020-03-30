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
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractDataOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A subclass of {@link StreamTask} for executing the {@link SourceOperator}.
 */
@Internal
public class SourceOperatorStreamTask<T> extends StreamTask<T, SourceOperator<T, ?>> {

	public SourceOperatorStreamTask(Environment env) throws Exception {
		super(env);
	}

	@Override
	public void init() {
		StreamTaskInput<T> input = new StreamTaskSourceInput<>(headOperator);
		DataOutput<T> output = new StreamTaskSourceOutput<>(
			operatorChain.getChainEntryPoint(),
			getStreamStatusMaintainer());

		inputProcessor = new StreamOneInputProcessor<>(
			input,
			output,
			operatorChain);
	}

	/**
	 * Implementation of {@link DataOutput} that wraps a specific {@link Output} to emit
	 * stream elements for {@link SourceOperator}.
	 */
	private static class StreamTaskSourceOutput<T> extends AbstractDataOutput<T> implements SourceOutput<T> {

		private final Output<StreamRecord<T>> output;

		StreamTaskSourceOutput(
				Output<StreamRecord<T>> output,
				StreamStatusMaintainer streamStatusMaintainer) {
			super(streamStatusMaintainer);

			this.output = checkNotNull(output);
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
			output.emitWatermark(watermark);
		}

		// ------------------- methods from SourceOutput -------------

		@Override
		public void collect(T record) throws Exception {
			output.collect(new StreamRecord<>(record));
		}

		@Override
		public void collect(T record, long timestamp) throws Exception {
			output.collect(new StreamRecord<>(record, timestamp));
		}

		@Override
		public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {
			output.emitWatermark(new Watermark(watermark.getTimestamp()));
		}

		@Override
		public void markIdle() {
			emitStreamStatus(StreamStatus.IDLE);
		}
	}
}
