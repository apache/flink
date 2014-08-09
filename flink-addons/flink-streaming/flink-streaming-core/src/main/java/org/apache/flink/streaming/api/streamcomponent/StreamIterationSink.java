/**
 *
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
 *
 */

package org.apache.flink.streaming.api.streamcomponent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.StringUtils;

public class StreamIterationSink<IN extends Tuple> extends
		SingleInputAbstractStreamComponent<IN, IN> {

	private static final Log LOG = LogFactory.getLog(StreamIterationSink.class);

	private String iterationId;
	@SuppressWarnings("rawtypes")
	private BlockingQueue<StreamRecord> dataChannel;
	private long iterationWaitTime;
	private boolean shouldWait;

	public StreamIterationSink() {
	}

	@Override
	public void setInputsOutputs() {
		try {
			setConfigInputs();
			setSinkSerializer();

			inputIter = createInputIterator(inputs, inputSerializer);

			iterationId = configuration.getIterationId();
			iterationWaitTime = configuration.getIterationWaitTime();
			shouldWait = iterationWaitTime > 0;
			dataChannel = BlockingQueueBroker.instance().get(iterationId);

		} catch (Exception e) {
			throw new StreamComponentException(String.format(
					"Cannot register inputs of StreamIterationSink %s", iterationId), e);
		}
	}

	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK " + name + " invoked");
		}

		forwardRecords();

		if (LOG.isDebugEnabled()) {
			LOG.debug("SINK " + name + " invoke finished");
		}
	}

	protected void forwardRecords() throws Exception {
		StreamRecord<IN> reuse = inputSerializer.createInstance();
		while ((reuse = inputIter.next(reuse)) != null) {
			if (!pushToQueue(reuse)) {
				break;
			}
			// TODO: Fix object reuse for iteration
			reuse = inputSerializer.createInstance();
		}
	}

	private boolean pushToQueue(StreamRecord<IN> record) {
		try {
			if (shouldWait) {
				return dataChannel.offer(record, iterationWaitTime, TimeUnit.MILLISECONDS);
			} else {
				dataChannel.put(record);
				return true;
			}
		} catch (InterruptedException e) {
			if (LOG.isErrorEnabled()) {
				LOG.error(String.format("Pushing back record at iteration %s failed due to: %s",
						iterationId, StringUtils.stringifyException(e)));
			}
			return false;
		}
	}

	@Override
	protected void setInvokable() {
	}
}
