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
import org.apache.flink.streaming.api.operators.SourceReaderOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractDataOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A subclass of {@link StreamTask} for executing the {@link SourceReaderOperator}.
 */
@Internal
public class SourceReaderStreamTask<T> extends StreamTask<T, SourceReaderOperator<T>> {

	public SourceReaderStreamTask(Environment env) {
		super(env);
	}

	@Override
	public void init() {
		StreamTaskInput<T> input = new StreamTaskSourceInput<>(headOperator);
		DataOutput<T> output = new StreamTaskSourceOutput<>(
			operatorChain.getChainEntryPoint(),
			getStreamStatusMaintainer(),
			getCheckpointLock());

		inputProcessor = new StreamOneInputProcessor<>(
			input,
			output,
			getCheckpointLock(),
			operatorChain);
	}

	/**
	 * Implementation of {@link DataOutput} that wraps a specific {@link Output} to emit
	 * stream elements for {@link SourceReaderOperator}.
	 */
	private static class StreamTaskSourceOutput<T> extends AbstractDataOutput<T> {

		private final Output<StreamRecord<T>> output;

		StreamTaskSourceOutput(
				Output<StreamRecord<T>> output,
				StreamStatusMaintainer streamStatusMaintainer,
				Object lock) {
			super(streamStatusMaintainer, lock);

			this.output = checkNotNull(output);
		}

		@Override
		public void emitRecord(StreamRecord<T> streamRecord) {
			synchronized (lock) {
				output.collect(streamRecord);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			synchronized (lock) {
				output.emitLatencyMarker(latencyMarker);
			}
		}

		@Override
		public void emitWatermark(Watermark watermark) {
			synchronized (lock) {
				output.emitWatermark(watermark);
			}
		}
	}
}
