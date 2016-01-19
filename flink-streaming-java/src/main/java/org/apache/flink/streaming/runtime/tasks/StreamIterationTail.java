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

package org.apache.flink.streaming.runtime.tasks;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class StreamIterationTail<IN> extends OneInputStreamTask<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationTail.class);
	private
	@SuppressWarnings("unchecked")
	BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel;

	@Override
	public void init() throws Exception {

		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}

		final String brokerID = StreamIterationHead.createBrokerIdString(getEnvironment().getJobID(), iterationId,
				getEnvironment().getTaskInfo().getIndexOfThisSubtask());

		final long iterationWaitTime = getConfiguration().getIterationWaitTime();

		LOG.info("Iteration tail {} trying to acquire feedback queue under {}", getName(), brokerID);

		dataChannel = (BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>>) BlockingQueueBroker.INSTANCE.get(brokerID);
		
		LOG.info("Iteration tail {} acquired feedback queue {}", getName(), brokerID);

		this.headOperator = new RecordPusher<>();
		this.headOperator.setup(this, getConfiguration(), new IterationTailOutput<>(dataChannel, iterationWaitTime));

		// call super.init() last because that needs this.headOperator to be set up
		super.init();
	}

	@Override
	protected boolean performCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
		LOG.debug("Starting checkpoint {} on task {}", checkpointMetaData.getCheckpointId(), getName());

		synchronized (getCheckpointLock()) {
			if (isRunning()) {
				dataChannel.put(new Either.Right(new CheckpointBarrier(checkpointMetaData.getCheckpointId(), checkpointMetaData.getTimestamp(), checkpointOptions)));
				getEnvironment().acknowledgeCheckpoint(checkpointMetaData.getCheckpointId(), checkpointMetrics);
				return true;
			}
			else {
				return super.performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
			}
		}
	}

	private static class RecordPusher<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(StreamRecord<IN> record) throws Exception {
			output.collect(record);
		}

		@Override
		public void processWatermark(Watermark mark) {
			// ignore
		}

		@Override
		public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			// ignore
		}
	}

	private static class IterationTailOutput<IN> implements Output<StreamRecord<IN>> {

		@SuppressWarnings("NonSerializableFieldInSerializableClass")
		private final BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel;
		
		private final long iterationWaitTime;
		
		private final boolean shouldWait;

		IterationTailOutput(BlockingQueue<Either<StreamRecord<IN>, CheckpointBarrier>> dataChannel, long iterationWaitTime) {
			this.dataChannel = dataChannel;
			this.iterationWaitTime = iterationWaitTime;
			this.shouldWait =  iterationWaitTime > 0;
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
		}

		@Override
		public void collect(StreamRecord<IN> record) {
			try {
				if (shouldWait) {
					dataChannel.offer(new Either.Left(record), iterationWaitTime, TimeUnit.MILLISECONDS);
				}
				else {
					dataChannel.put(new Either.Left(record));
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() {
		}
	}
}
