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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Builder to create a {@link StreamTaskMailboxTestHarness} to test {@link MultipleInputStreamTask}.
 */
public class MultipleInputStreamTaskTestHarnessBuilder<OUT> extends StreamTaskMailboxTestHarnessBuilder<OUT> {

	private final ArrayList<TypeSerializer<?>> inputSerializers = new ArrayList<>();
	private final ArrayList<Integer> inputChannelsPerGate = new ArrayList<>();

	public MultipleInputStreamTaskTestHarnessBuilder(
			FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory,
			TypeInformation<OUT> outputType) {
		super(taskFactory, outputType);
	}

	public MultipleInputStreamTaskTestHarnessBuilder<OUT> addInput(TypeInformation<?> inputType) {
		return addInput(inputType, 1);
	}

	public MultipleInputStreamTaskTestHarnessBuilder<OUT> addInput(TypeInformation<?> inputType, int inputChannels) {
		return addInput(inputType, inputChannels, null);
	}

	public MultipleInputStreamTaskTestHarnessBuilder<OUT> addInput(
			TypeInformation<?> inputType,
			int inputChannels,
			@Nullable KeySelector<?, ?> keySelector) {
		streamConfig.setStatePartitioner(inputSerializers.size(), keySelector);
		inputSerializers.add(inputType.createSerializer(executionConfig));
		inputChannelsPerGate.add(inputChannels);
		return this;
	}

	@Override
	protected void initializeInputs(StreamMockEnvironment streamMockEnvironment) {
		inputGates = new StreamTestSingleInputGate[inputSerializers.size()];
		List<StreamEdge> inPhysicalEdges = new LinkedList<>();

		StreamOperator<?> dummyOperator = new AbstractStreamOperator<Object>() {
			private static final long serialVersionUID = 1L;
		};

		StreamNode sourceVertexDummy = new StreamNode(0, "default group", null, dummyOperator, "source dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(1, "default group", null, dummyOperator, "target dummy", new LinkedList<>(), SourceStreamTask.class);

		for (int i = 0; i < inputSerializers.size(); i++) {
			TypeSerializer<?> inputSerializer = inputSerializers.get(i);
			inputGates[i] = new StreamTestSingleInputGate<>(
				inputChannelsPerGate.get(i),
				i,
				inputSerializer,
				bufferSize);

			StreamEdge streamEdge = new StreamEdge(
				sourceVertexDummy,
				targetVertexDummy,
				i + 1,
				new LinkedList<>(),
				new BroadcastPartitioner<>(),
				null);

			inPhysicalEdges.add(streamEdge);
			streamMockEnvironment.addInputGate(inputGates[i].getInputGate());
		}

		streamConfig.setInPhysicalEdges(inPhysicalEdges);
		streamConfig.setNumberOfInputs(inputGates.length);
		streamConfig.setTypeSerializersIn(inputSerializers.toArray(new TypeSerializer[inputSerializers.size()]));
	}
}

