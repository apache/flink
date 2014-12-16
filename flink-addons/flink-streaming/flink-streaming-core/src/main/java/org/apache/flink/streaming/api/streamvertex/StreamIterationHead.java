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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.api.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.io.BlockingQueueBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamIterationHead<OUT extends Tuple> extends StreamVertex<OUT,OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private OutputHandler<OUT> outputHandler;

	private static int numSources;
	private Integer iterationId;
	@SuppressWarnings("rawtypes")
	private BlockingQueue<StreamRecord> dataChannel;
	private long iterationWaitTime;
	private boolean shouldWait;

	@SuppressWarnings("rawtypes")
	public StreamIterationHead() {
		numSources = newVertex();
		instanceID = numSources;
		dataChannel = new ArrayBlockingQueue<StreamRecord>(1);
	}

	@Override
	public void setInputsOutputs() {
		outputHandler = new OutputHandler<OUT>(this);

		iterationId = configuration.getIterationId();
		iterationWaitTime = configuration.getIterationWaitTime();
		shouldWait = iterationWaitTime > 0;

		try {
			BlockingQueueBroker.instance().handIn(iterationId.toString(), dataChannel);
		} catch (Exception e) {

		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void invoke() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("SOURCE {} invoked with instance id {}", getName(), getInstanceID());
		}

		outputHandler.initializeOutputSerializers();

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
			for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output : outputHandler
					.getOutputs()) {
				outputHandler.outSerializationDelegate.setInstance(nextRecord);
				output.emit(outputHandler.outSerializationDelegate);
			}
		}

		outputHandler.flushOutputs();
	}

	@Override
	protected void setInvokable() {
	}
}
