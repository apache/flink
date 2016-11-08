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
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Task for executing streaming sources.
 *
 * One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the StreamFunction that it must only modify its state or emit elements in
 * a synchronized block that locks on the lock Object. Also, the modification of the state
 * and the emission of elements must happen in the same block of code that is protected by the
 * synchronized block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
	extends StreamTask<OUT, OP> {

	private Output<StreamRecord<OUT>> output;

	@Override
	protected void init() {
		output = new SourceOutput<>(getHeadOutput(), getEnvironment().getMetricGroup().getIOMetricGroup());
	}

	@Override
	protected void cleanup() {
	}
	

	@Override
	protected void run() throws Exception {
		headOperator.run(getCheckpointLock(), output);
	}
	
	@Override
	protected void cancelTask() throws Exception {
		headOperator.cancel();
	}

	/**
	 * Special output for sources. for example measuring elements emit latency as a metric.
	 *
	 *  @param <OUT> Type of the output elements of this source.
	 */
	private static class SourceOutput<OUT> implements Output<StreamRecord<OUT>> {

		private final Output<StreamRecord<OUT>> output;

		private final Histogram recordProcessLatency;

		public SourceOutput(Output<StreamRecord<OUT>> output, TaskIOMetricGroup ioMetricGroup) {
			this.output = output;
			this.recordProcessLatency = ioMetricGroup.getRecordProcessLatency();
		}

		@Override
		public void emitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			output.emitLatencyMarker(latencyMarker);
		}

		@Override
		public void collect(StreamRecord<OUT> record) {
			long start=System.nanoTime();
			output.collect(record);
			long end=System.nanoTime();
			recordProcessLatency.update(end - start);
		}

		@Override
		public void close() {
			output.close();
		}
	}
}
