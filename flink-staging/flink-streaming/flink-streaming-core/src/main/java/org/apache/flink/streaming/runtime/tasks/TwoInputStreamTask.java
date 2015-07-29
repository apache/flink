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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputStreamTask<IN1, IN2, OUT> extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(TwoInputStreamTask.class);

	private StreamTwoInputProcessor<IN1, IN2> inputProcessor;

	@Override
	public void registerInputOutput() {
		try {
			super.registerInputOutput();
	
			TypeSerializer<IN1> inputDeserializer1 = configuration.getTypeSerializerIn1(userClassLoader);
			TypeSerializer<IN2> inputDeserializer2 = configuration.getTypeSerializerIn2(userClassLoader);
	
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
	
			this.inputProcessor = new StreamTwoInputProcessor<IN1, IN2>(inputList1, inputList2,
					inputDeserializer1, inputDeserializer2,
					getCheckpointBarrierListener(),
					configuration.getCheckpointMode(),
					getEnvironment().getIOManager(),
					getExecutionConfig().areTimestampsEnabled());

			// make sure that stream tasks report their I/O statistics
			AccumulatorRegistry registry = getEnvironment().getAccumulatorRegistry();
			AccumulatorRegistry.Reporter reporter = registry.getReadWriteReporter();
			this.inputProcessor.setReporter(reporter);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to initialize stream operator: " + e.getMessage(), e);
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

			while (inputProcessor.processInput(streamOperator)) {
				// do nothing, just keep processing
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
	public void clearBuffers() throws IOException {
		super.clearBuffers();
		inputProcessor.clearBuffers();
		inputProcessor.cleanup();
	}
}
