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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamIterationTail<IN> extends OneInputStreamTask<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationTail.class);

	@Override
	public void init() throws Exception {
		super.init();
		
		final String iterationId = getConfiguration().getIterationId();
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}

		final String brokerID = StreamIterationHead.createBrokerIdString(getEnvironment().getJobID(), iterationId,
				getEnvironment().getTaskInfo().getIndexOfThisSubtask());

		final long iterationWaitTime = getConfiguration().getIterationWaitTime();

		LOG.info("Iteration tail {} trying to acquire feedback queue under {}", getName(), brokerID);
		
		@SuppressWarnings("unchecked")
		BlockingQueue<StreamRecord<IN>> dataChannel =
				(BlockingQueue<StreamRecord<IN>>) BlockingQueueBroker.INSTANCE.get(brokerID);
		
		LOG.info("Iteration tail {} acquired feedback queue {}", getName(), brokerID);
		
		this.headOperator = new RecordPusher<>(dataChannel, iterationWaitTime);
	}

	private static class RecordPusher<IN> extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {
		
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("NonSerializableFieldInSerializableClass")
		private final BlockingQueue<StreamRecord<IN>> dataChannel;
		
		private final long iterationWaitTime;
		
		private final boolean shouldWait;

		RecordPusher(BlockingQueue<StreamRecord<IN>> dataChannel, long iterationWaitTime) {
			this.dataChannel = dataChannel;
			this.iterationWaitTime = iterationWaitTime;
			this.shouldWait =  iterationWaitTime > 0;
		}

		@Override
		public void processElement(StreamRecord<IN> record) throws Exception {
			if (shouldWait) {
				dataChannel.offer(record, iterationWaitTime, TimeUnit.MILLISECONDS);
			}
			else {
				dataChannel.put(record);
			}
		}

		@Override
		public void processWatermark(Watermark mark) {
			// ignore
		}
	}
}
