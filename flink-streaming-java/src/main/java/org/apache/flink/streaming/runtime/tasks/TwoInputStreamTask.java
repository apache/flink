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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;

@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends StreamTask<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

	private StreamTwoInputProcessor<IN1, IN2> inputProcessor;
	
	private volatile boolean running = true;

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();
		
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
	
		this.inputProcessor = new StreamTwoInputProcessor<IN1, IN2>(
				inputList1, inputList2,
				inputDeserializer1, inputDeserializer2,
				this,
				configuration.getCheckpointMode(),
				getEnvironment().getIOManager(),
				getEnvironment().getTaskManagerInfo().getConfiguration());

		// make sure that stream tasks report their I/O statistics
		inputProcessor.setMetricGroup(getEnvironment().getMetricGroup().getIOMetricGroup());
	}

	@Override
	protected void run() throws Exception {
		// cache some references on the stack, to make the code more JIT friendly
		final TwoInputStreamOperator<IN1, IN2, OUT> operator = this.headOperator;
		final StreamTwoInputProcessor<IN1, IN2> inputProcessor = this.inputProcessor;
		final Object lock = getCheckpointLock();
		
		while (running && inputProcessor.processInput(operator, lock)) {
			// all the work happens in the "processInput" method
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}
}
