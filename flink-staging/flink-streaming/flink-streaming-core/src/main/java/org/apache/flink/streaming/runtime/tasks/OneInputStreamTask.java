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

import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.IndexedMutableReader;
import org.apache.flink.streaming.runtime.io.IndexedReaderIterator;
import org.apache.flink.streaming.runtime.io.InputGateFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(OneInputStreamTask.class);

	protected StreamRecordSerializer<IN> inSerializer;
	private IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>> inputs;
	protected IndexedReaderIterator<StreamRecord<IN>> recordIterator;


	@Override
	public void registerInputOutput() {
		super.registerInputOutput();

		inSerializer = configuration.getTypeSerializerIn1(getUserCodeClassLoader());

		int numberOfInputs = configuration.getNumberOfInputs();

		if (numberOfInputs > 0) {
			InputGate inputGate = InputGateFactory.createInputGate(getEnvironment().getAllInputGates());
			inputs = new IndexedMutableReader<DeserializationDelegate<StreamRecord<IN>>>(inputGate);

			inputs.registerTaskEventListener(getSuperstepListener(), StreamingSuperstep.class);

			recordIterator = new IndexedReaderIterator<StreamRecord<IN>>(inputs, inSerializer);
		}
	}

	/*
	 * Reads the next record from the reader iterator and stores it in the
	 * nextRecord variable
	 */
	protected StreamRecord<IN> readNext() throws IOException {
		StreamRecord<IN> nextRecord = inSerializer.createInstance();
		try {
			return recordIterator.next(nextRecord);
		} catch (IOException e) {
			if (isRunning) {
				throw new RuntimeException("Could not read next record.", e);
			} else {
				// Task already cancelled do nothing
				return null;
			}
		} catch (IllegalStateException e) {
			if (isRunning) {
				throw new RuntimeException("Could not read next record.", e);
			} else {
				// Task already cancelled do nothing
				return null;
			}
		}
	}

	@Override
	public void invoke() throws Exception {
		this.isRunning = true;

		boolean operatorOpen = false;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Task {} invoked", getName());
		}

		try {
			openOperator();
			operatorOpen = true;

			StreamRecord<IN> nextRecord;
			while (isRunning && (nextRecord = readNext()) != null) {
				streamOperator.processElement(nextRecord.getObject());
			}

			closeOperator();
			operatorOpen = false;

			if (LOG.isDebugEnabled()) {
				LOG.debug("Task {} invocation finished", getName());
			}

		}
		catch (Exception e) {
			LOG.error(getEnvironment().getTaskNameWithSubtasks() + " failed", e);
			
			if (operatorOpen) {
				try {
					closeOperator();
				}
				catch (Throwable t) {
					LOG.warn("Exception while closing operator.", t);
				}
			}
			
			throw e;
		}
		finally {
			this.isRunning = false;
			// Cleanup
			inputs.clearBuffers();
			inputs.cleanup();
			outputHandler.flushOutputs();
			clearBuffers();
		}

	}
}
