/*
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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;


/**
 * Test harness for testing a {@link TwoInputStreamTask}.
 *
 * <p>This mock Invokable provides the task with a basic runtime context and allows pushing elements
 * and watermarks into the task. {@link #getOutput()} can be used to get the emitted elements
 * and events. You are free to modify the retrieved list.
 *
 * <p>After setting up everything the Task can be invoked using {@link #invoke()}. This will start
 * a new Thread to execute the Task. Use {@link #waitForTaskCompletion()} to wait for the Task
 * thread to finish. Use {@link #processElement}
 * to send elements to the task. Use
 * {@link #processEvent(org.apache.flink.runtime.event.AbstractEvent)} to send events to the task.
 * Before waiting for the task to finish you must call {@link #endInput()} to signal to the task
 * that data entry is finished.
 *
 * <p>When Elements or Events are offered to the Task they are put into a queue. The input gates
 * of the Task read from this queue. Use {@link #waitForInputProcessing()} to wait until all
 * queues are empty. This must be used after entering some elements before checking the
 * desired output.
 */
public class TwoInputStreamTaskTestHarness<IN1, IN2, OUT> extends StreamTaskTestHarness<OUT> {

	private TypeSerializer<IN1> inputSerializer1;

	private TypeSerializer<IN2> inputSerializer2;

	private int[] inputGateAssignment;

	/**
	 * Creates a test harness with the specified number of input gates and specified number
	 * of channels per input gate. Parameter inputGateAssignment specifies for each gate whether
	 * it should be assigned to the first (1), or second (2) input of the task.
	 */
	public TwoInputStreamTaskTestHarness(
			Function<Environment, ? extends AbstractTwoInputStreamTask<IN1, IN2, OUT>> taskFactory,
			int numInputGates,
			int numInputChannelsPerGate,
			int[] inputGateAssignment,
			TypeInformation<IN1> inputType1,
			TypeInformation<IN2> inputType2,
			TypeInformation<OUT> outputType) {

		super(taskFactory, outputType);

		inputSerializer1 = inputType1.createSerializer(executionConfig);

		inputSerializer2 = inputType2.createSerializer(executionConfig);

		this.numInputGates = numInputGates;
		this.numInputChannelsPerGate = numInputChannelsPerGate;
		this.inputGateAssignment = inputGateAssignment;
	}

	/**
	 * Creates a test harness with one input gate (that has one input channel) per input. The first
	 * input gate is assigned to the first task input, the second input gate is assigned to the
	 * second task input.
	 */
	public TwoInputStreamTaskTestHarness(
			Function<Environment, ? extends AbstractTwoInputStreamTask<IN1, IN2, OUT>> taskFactory,
			TypeInformation<IN1> inputType1,
			TypeInformation<IN2> inputType2,
			TypeInformation<OUT> outputType) {

		this(taskFactory, 2, 1, new int[] {1, 2}, inputType1, inputType2, outputType);
	}

	@Override
	protected void initializeInputs() throws IOException, InterruptedException {

		inputGates = new StreamTestSingleInputGate[numInputGates];
		List<StreamEdge> inPhysicalEdges = new LinkedList<>();

		StreamOperator<IN1> dummyOperator = new AbstractStreamOperator<IN1>() {
			private static final long serialVersionUID = 1L;
		};

		StreamNode sourceVertexDummy = new StreamNode(0, "default group", null, dummyOperator, "source dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(1, "default group", null, dummyOperator, "target dummy", new LinkedList<>(), SourceStreamTask.class);

		for (int i = 0; i < numInputGates; i++) {

			switch (inputGateAssignment[i]) {
				case 1: {
					inputGates[i] = new StreamTestSingleInputGate<>(
							numInputChannelsPerGate,
							bufferSize,
							inputSerializer1);

					StreamEdge streamEdge = new StreamEdge(sourceVertexDummy,
							targetVertexDummy,
							1,
							new LinkedList<>(),
							new BroadcastPartitioner<>(),
							null /* output tag */);

					inPhysicalEdges.add(streamEdge);
					break;
				}
				case 2: {
					inputGates[i] = new StreamTestSingleInputGate<>(
							numInputChannelsPerGate,
							bufferSize,
							inputSerializer2);

					StreamEdge streamEdge = new StreamEdge(sourceVertexDummy,
							targetVertexDummy,
							2,
							new LinkedList<>(),
							new BroadcastPartitioner<>(),
							null /* output tag */);

					inPhysicalEdges.add(streamEdge);
					break;
				}
				default:
					throw new IllegalStateException("Wrong input gate assignment.");
			}

			this.mockEnv.addInputGate(inputGates[i].getInputGate());
		}

		streamConfig.setInPhysicalEdges(inPhysicalEdges);
		streamConfig.setNumberOfInputs(numInputGates);
		streamConfig.setTypeSerializerIn1(inputSerializer1);
		streamConfig.setTypeSerializerIn2(inputSerializer2);
	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractTwoInputStreamTask<IN1, IN2, OUT> getTask() {
		return (AbstractTwoInputStreamTask<IN1, IN2, OUT>) super.getTask();
	}
}

