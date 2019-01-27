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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.source.SourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.concurrent.ScheduledFuture;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSourceV2<OUT, SRC extends SourceFunctionV2<OUT>>
		extends AbstractUdfStreamOperator<OUT, SRC> implements StreamOperator<OUT> {

	private static final long serialVersionUID = 1L;

	private transient LatencyMarksEmitter latencyEmitter;

	public StreamSourceV2(SRC sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (getExecutionConfig().isLatencyTrackingEnabled()) {
			latencyEmitter = new LatencyMarksEmitter<>(
				getProcessingTimeService(),
				getOutput(),
				getExecutionConfig().getLatencyTrackingInterval(),
				getOperatorID(),
				getRuntimeContext().getIndexOfThisSubtask());
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (latencyEmitter != null) {
			latencyEmitter.close();
		}
	}

	public SourceRecord<OUT> next() throws Exception {
		return userFunction.next();
	}

	public boolean isFinished() {
		return userFunction.isFinished();
	}

	public void cancel() {
		userFunction.cancel();
	}

	private static class LatencyMarksEmitter<OUT> {
		private final ScheduledFuture<?> latencyMarkTimer;

		public LatencyMarksEmitter(
			final ProcessingTimeService processingTimeService,
			final Output<StreamRecord<OUT>> output,
			long latencyTrackingInterval,
			final OperatorID operatorId,
			final int subtaskIndex) {

			latencyMarkTimer = processingTimeService.scheduleAtFixedRate(
				new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) throws Exception {
						try {
							// ProcessingTimeService callbacks are executed under the checkpointing lock
							output.emitLatencyMarker(new LatencyMarker(timestamp, operatorId, subtaskIndex));
						} catch (Throwable t) {
							// we catch the Throwables here so that we don't trigger the processing
							// timer services async exception handler
							LOG.warn("Error while emitting latency marker.", t);
						}
					}
				},
				0L,
				latencyTrackingInterval);
		}

		public void close() {
			latencyMarkTimer.cancel(true);
		}
	}
}
