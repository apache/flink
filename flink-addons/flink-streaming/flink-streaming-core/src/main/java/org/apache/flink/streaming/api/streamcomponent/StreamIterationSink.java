/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.api.streamcomponent;

import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

import org.apache.flink.runtime.io.network.api.AbstractRecordReader;

public class StreamIterationSink extends AbstractStreamComponent {

	private static final Log log = LogFactory.getLog(StreamIterationSink.class);

	private AbstractRecordReader inputs;
	private String iterationId;
	private BlockingQueue<StreamRecord> dataChannel;
	

	public StreamIterationSink() {
	}

	@Override
	public void registerInputOutput() {
		initialize();

		try {
			setSerializers();
			setSinkSerializer();
			inputs = getConfigInputs();
			iterationId = configuration.getString("iteration-id", "iteration-0");
			dataChannel = BlockingQueueBroker.instance().getAndRemove(iterationId);
		} catch (Exception e) {
			if (log.isErrorEnabled()) {
				log.error("Cannot register inputs", e);
			}
		}
	}

	@Override
	public void invoke() throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("SINK " + name + " invoked");
		}

		forwardRecords(inputs);

		if (log.isDebugEnabled()) {
			log.debug("SINK " + name + " invoke finished");
		}
	}

	protected void forwardRecords(AbstractRecordReader inputs) throws Exception {
		if (inputs instanceof UnionStreamRecordReader) {
			UnionStreamRecordReader recordReader = (UnionStreamRecordReader) inputs;
			while (recordReader.hasNext()) {
				StreamRecord record = recordReader.next();
				pushToQueue(record);
			}

		} else if (inputs instanceof StreamRecordReader) {
			StreamRecordReader recordReader = (StreamRecordReader) inputs;

			while (recordReader.hasNext()) {
				StreamRecord record = recordReader.next();
				pushToQueue(record);
			}
		}
	}

	private void pushToQueue(StreamRecord record) {
		try {
			dataChannel.put(record);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void setInvokable() {

	}
}
