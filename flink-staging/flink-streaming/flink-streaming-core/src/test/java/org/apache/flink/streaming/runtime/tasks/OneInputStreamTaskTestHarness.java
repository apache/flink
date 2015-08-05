/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;

import java.io.IOException;


/**
 * Test harness for testing a {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>
 * This mock Invokable provides the task with a basic runtime context and allows pushing elements
 * and watermarks into the task. {@link #getOutput()} can be used to get the emitted elements
 * and events. You are free to modify the retrieved list.
 *
 * <p>
 * After setting up everything the Task can be invoked using {@link #invoke()}. This will start
 * a new Thread to execute the Task. Use {@link #waitForTaskCompletion()} to wait for the Task
 * thread to finish. Use {@link #processElement} to send elements to the task. Use
 * {@link #processEvent(AbstractEvent)} to send events to the task.
 * Before waiting for the task to finish you must call {@link #endInput()} to signal to the task
 * that data entry is finished.
 *
 * <p>
 * When Elements or Events are offered to the Task they are put into a queue. The input gates
 * of the Task read from this queue. Use {@link #waitForInputProcessing()} to wait until all
 * queues are empty. This must be used after entering some elements before checking the
 * desired output.
 *
 * <p>
 * When using this you need to add the following line to your test class to setup Powermock:
 * {@code @PrepareForTest({ResultPartitionWriter.class})}
 */
public class OneInputStreamTaskTestHarness<IN, OUT> extends StreamTaskTestHarness<OUT> {

	private TypeInformation<IN> inputType;
	private TypeSerializer<IN> inputSerializer;

	/**
	 * Creates a test harness with the specified number of input gates and specified number
	 * of channels per input gate.
	 */
	public OneInputStreamTaskTestHarness(OneInputStreamTask<IN, OUT> task,
			int numInputGates,

			int numInputChannelsPerGate,
			TypeInformation<IN> inputType,
			TypeInformation<OUT> outputType) {
		super(task, outputType);

		this.inputType = inputType;
		inputSerializer = inputType.createSerializer(executionConfig);

		this.numInputGates = numInputGates;
		this.numInputChannelsPerGate = numInputChannelsPerGate;
	}

	/**
	 * Creates a test harness with one input gate that has one input channel.
	 */
	public OneInputStreamTaskTestHarness(OneInputStreamTask<IN, OUT> task,
			TypeInformation<IN> inputType,
			TypeInformation<OUT> outputType) {
		this(task, 1, 1, inputType, outputType);
	}

	@Override
	protected void initializeInputs() throws IOException, InterruptedException {
		inputGates = new StreamTestSingleInputGate[numInputGates];

		for (int i = 0; i < numInputGates; i++) {
			inputGates[i] = new StreamTestSingleInputGate<IN>(
					numInputChannelsPerGate,
					bufferSize,
					inputSerializer);
			this.mockEnv.addInputGate(inputGates[i].getInputGate());
		}


		streamConfig.setNumberOfInputs(1);
		streamConfig.setTypeSerializerIn1(inputSerializer);
	}

}

