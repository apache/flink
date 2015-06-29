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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.apache.flink.streaming.runtime.io.CoReaderIterator;
import org.apache.flink.streaming.runtime.io.CoStreamingRecordReader;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputStreamTask<IN1, IN2, OUT> extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(TwoInputStreamTask.class);

	protected StreamRecordSerializer<IN1> inputDeserializer1 = null;
	protected StreamRecordSerializer<IN2> inputDeserializer2 = null;

	CoStreamingRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>> coReader;
	CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>> coIter;

	private volatile boolean operatorOpen = false;


	@Override
	public void invoke() throws Exception {
		this.isRunning = true;

		operatorOpen = false;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Task {} invoked", getName());
		}

		try {

			openOperator();
			operatorOpen = true;

			int next;
			StreamRecord<IN1> reuse1 = inputDeserializer1.createInstance();
			StreamRecord<IN2> reuse2 = inputDeserializer2.createInstance();

			while (isRunning) {
				try {
					next = coIter.next(reuse1, reuse2);
				} catch (IOException e) {
					if (isRunning) {
						throw new RuntimeException("Could not read next record.", e);
					} else {
						// Task already cancelled do nothing
						next = 0;
					}
				} catch (IllegalStateException e) {
					if (isRunning) {
						throw new RuntimeException("Could not read next record.", e);
					} else {
						// Task already cancelled do nothing
						next = 0;
					}
				}

				if (next == 0) {
					break;
				} else if (next == 1) {
					streamOperator.processElement1(reuse1);
					reuse1 = inputDeserializer1.createInstance();
				} else {
					streamOperator.processElement2(reuse2);
					reuse2 = inputDeserializer2.createInstance();
				}
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
			outputHandler.flushOutputs();
			clearBuffers();
		}

	}

	@Override
	public void registerInputOutput() {
		super.registerInputOutput();

		inputDeserializer1 = configuration.getTypeSerializerIn1(userClassLoader);
		inputDeserializer2 = configuration.getTypeSerializerIn2(userClassLoader);

		int numberOfInputs = configuration.getNumberOfInputs();

		ArrayList<InputGate> inputList1 = new ArrayList<InputGate>();
		ArrayList<InputGate> inputList2 = new ArrayList<InputGate>();

		List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = inEdges.get(i).getTypeNumber();
			InputGate reader = getEnvironment().getInputGate(i);
			switch (inputType) {
				case 1:
					inputList1.add(reader);
					break;
				case 2:
					inputList2.add(reader);
					break;
				default:
					throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		coReader = new CoStreamingRecordReader<DeserializationDelegate<StreamRecord<IN1>>, DeserializationDelegate<StreamRecord<IN2>>>(
				inputList1, inputList2);

		coReader.registerTaskEventListener(getSuperstepListener(), StreamingSuperstep.class);
		coReader.registerTaskEventListener(new WatermarkListener(), Watermark.class);

		coIter = new CoReaderIterator<StreamRecord<IN1>, StreamRecord<IN2>>(coReader,
				inputDeserializer1, inputDeserializer2);
	}

	@Override
	public void clearBuffers() throws IOException {
		super.clearBuffers();
		coReader.clearBuffers();
		coReader.cleanup();
	}

	private class WatermarkListener implements EventListener<TaskEvent> {

		@Override
		public void onEvent(TaskEvent event) {
			Watermark watermark = (Watermark) event;
			try {
				if (operatorOpen) {
					if (watermark.getInputIndex() == 1) {
						streamOperator.processWatermark1(watermark);
					} else {
						streamOperator.processWatermark2(watermark);
					}
				}
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Failed to forward watermark to operator: {}", e);
				}
				throw new RuntimeException(e);
			}
		}

	}
}
