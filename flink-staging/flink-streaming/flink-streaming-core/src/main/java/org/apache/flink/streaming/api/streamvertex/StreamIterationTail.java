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

package org.apache.flink.streaming.api.streamvertex;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.io.BlockingQueueBroker;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamIterationTail<IN> extends StreamVertex<IN, IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationTail.class);

	private InputHandler<IN> inputHandler;

	private Integer iterationId;
	@SuppressWarnings("rawtypes")
	private BlockingQueue<StreamRecord> dataChannel;
	private long iterationWaitTime;
	private boolean shouldWait;

	public StreamIterationTail() {
	}

	@Override
	public void setInputsOutputs() {
		try {
			inputHandler = new InputHandler<IN>(this);

			iterationId = configuration.getIterationId();
			iterationWaitTime = configuration.getIterationWaitTime();
			shouldWait = iterationWaitTime > 0;
			BlockingQueueBroker.instance().get(iterationId.toString()+"-"
					+getEnvironment().getIndexInSubtaskGroup());
		} catch (Exception e) {
			throw new StreamVertexException(String.format(
					"Cannot register inputs of StreamIterationSink %s", iterationId), e);
		}
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Iteration sink {} invoked", getName());
		}

		try {
			forwardRecords();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Iteration sink {} invoke finished", getName());
			}
		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Iteration sink failed due to: {}", StringUtils.stringifyException(e));
			}
			throw e;
		} finally {
			// Cleanup
			clearBuffers();
		}
	}

	protected void forwardRecords() throws Exception {
		StreamRecord<IN> reuse = inputHandler.getInputSerializer().createInstance();
		while ((reuse = inputHandler.getInputIter().next(reuse)) != null) {
			if (!pushToQueue(reuse)) {
				break;
			}
			reuse = inputHandler.getInputSerializer().createInstance();
		}
	}

	private boolean pushToQueue(StreamRecord<IN> record) throws InterruptedException {
		try {
			if (shouldWait) {
				return dataChannel.offer(record, iterationWaitTime, TimeUnit.MILLISECONDS);
			} else {
				dataChannel.put(record);
				return true;
			}
		} catch (InterruptedException e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Pushing back record at iteration %s failed due to: {}", iterationId,
						StringUtils.stringifyException(e));
				throw e;
			}
			return false;
		}
	}

	@Override
	protected void setInvokable() {
	}
}
