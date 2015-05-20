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

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.collector.StreamOutput;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamIterationHead<OUT> extends OneInputStreamTask<OUT, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);


	@SuppressWarnings("rawtypes")
	private BlockingQueue<StreamRecord> dataChannel;
	private long iterationWaitTime;
	private boolean shouldWait;

	@SuppressWarnings("rawtypes")
	public StreamIterationHead() {
		dataChannel = new ArrayBlockingQueue<StreamRecord>(1);
	}

	@Override
	public void registerInputOutput() {
		super.registerInputOutput();
		outputHandler = new OutputHandler<OUT>(this);

		Integer iterationId = configuration.getIterationId();
		iterationWaitTime = configuration.getIterationWaitTime();
		shouldWait = iterationWaitTime > 0;

		try {
			BlockingQueueBroker.instance().handIn(iterationId.toString()+"-" 
					+getEnvironment().getIndexInSubtaskGroup(), dataChannel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Iteration source {} invoked", getName());
		}

		Collection<StreamOutput<?>> outputs = outputHandler.getOutputs();

		try {
			StreamRecord<OUT> nextRecord;

			while (true) {
				if (shouldWait) {
					nextRecord = dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS);
				} else {
					nextRecord = dataChannel.take();
				}
				if (nextRecord == null) {
					break;
				}
				for (StreamOutput<?> output : outputs) {
					((StreamOutput<OUT>) output).collect(nextRecord.getObject());
				}
			}

		}
		catch (Exception e) {
			LOG.error("Iteration Head " + getEnvironment().getTaskNameWithSubtasks() + " failed", e);
			
			throw e;
		}
		finally {
			// Cleanup
			outputHandler.flushOutputs();
			clearBuffers();
		}
	}
}
