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
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamIterationTail<IN> extends OneInputStreamTask<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationTail.class);

	private String iterationId;

	@SuppressWarnings("rawtypes")
	private BlockingQueue<StreamRecord> dataChannel;
	private long iterationWaitTime;
	private boolean shouldWait;

	public StreamIterationTail() {
	}

	@Override
	public void registerInputOutput() {
		super.registerInputOutput();

		try {
			iterationId = configuration.getIterationId();
			iterationWaitTime = configuration.getIterationWaitTime();
			shouldWait = iterationWaitTime > 0;
			dataChannel = BlockingQueueBroker.instance().get(iterationId+"-"
					+getEnvironment().getIndexInSubtaskGroup());
		} catch (Exception e) {
			throw new StreamTaskException(String.format(
					"Cannot register inputs of StreamIterationSink %s", iterationId), e);
		}
		this.streamOperator = new RecordPusher();
	}

	class RecordPusher extends AbstractStreamOperator<IN> implements OneInputStreamOperator<IN, IN> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(StreamRecord<IN> record) throws Exception {
			try {
				if (shouldWait) {
					dataChannel.offer(record, iterationWaitTime, TimeUnit.MILLISECONDS);
				} else {
					dataChannel.put(record);
				}
			} catch (InterruptedException e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Pushing back record at iteration %s failed due to: {}", iterationId,
							StringUtils.stringifyException(e));
				}
				throw e;
			}
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			// ignore
		}
	}

}
