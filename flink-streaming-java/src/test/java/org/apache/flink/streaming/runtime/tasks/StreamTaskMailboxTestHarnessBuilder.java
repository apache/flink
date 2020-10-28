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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.InputConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.NetworkInputConfig;
import org.apache.flink.streaming.api.graph.StreamConfig.SourceInputConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder class for {@link StreamTaskMailboxTestHarness}.
 */
public class StreamTaskMailboxTestHarnessBuilder<OUT> {
	protected final FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory;
	protected final TypeSerializer<OUT> outputSerializer;
	protected final ExecutionConfig executionConfig = new ExecutionConfig();

	protected long memorySize = 1024 * 1024;
	protected int bufferSize = 1024;
	protected Configuration jobConfig = new Configuration();
	protected StreamConfig streamConfig = new StreamConfig(new Configuration());
	protected LocalRecoveryConfig localRecoveryConfig = TestLocalRecoveryConfig.disabled();
	@Nullable
	protected StreamTestSingleInputGate[] inputGates;
	protected TaskMetricGroup taskMetricGroup = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
	protected Map<Long, TaskStateSnapshot> taskStateSnapshots;

	protected final ArrayList<InputConfig> inputs = new ArrayList<>();
	protected final ArrayList<Integer> inputChannelsPerGate = new ArrayList<>();

	private boolean setupCalled = false;

	public StreamTaskMailboxTestHarnessBuilder(
			FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory,
			TypeInformation<OUT> outputType) {
		this.taskFactory = checkNotNull(taskFactory);
		outputSerializer = outputType.createSerializer(executionConfig);
		streamConfig.setTimeCharacteristic(TimeCharacteristic.EventTime);
	}

	public <T> StreamTaskMailboxTestHarnessBuilder<OUT> modifyExecutionConfig(Consumer<ExecutionConfig> action) {
		action.accept(executionConfig);
		return this;
	}

	public <T> StreamTaskMailboxTestHarnessBuilder<OUT> modifyStreamConfig(Consumer<StreamConfig> action) {
		action.accept(streamConfig);
		return this;
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> addInput(TypeInformation<?> inputType) {
		return addInput(inputType, 1);
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> addInput(TypeInformation<?> inputType, int inputChannels) {
		return addInput(inputType, inputChannels, null);
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> addInput(
			TypeInformation<?> inputType,
			int inputChannels,
			@Nullable KeySelector<?, ?> keySelector) {
		streamConfig.setStatePartitioner(inputs.size(), keySelector);
		inputs.add(new NetworkInputConfig(inputType.createSerializer(executionConfig), inputChannelsPerGate.size()));
		inputChannelsPerGate.add(inputChannels);
		return this;
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> addSourceInput(SourceOperatorFactory<?> sourceOperatorFactory) {
		inputs.add(new SourceInputConfigPlaceHolder(sourceOperatorFactory));
		return this;
	}

	public StreamTaskMailboxTestHarness<OUT> build() throws Exception {

		TestTaskStateManager taskStateManager = new TestTaskStateManager(localRecoveryConfig);
		if (taskStateSnapshots != null) {
			taskStateManager.setReportedCheckpointId(taskStateSnapshots.keySet().iterator().next());
			taskStateManager.setJobManagerTaskStateSnapshotsByCheckpointId(taskStateSnapshots);
		}

		StreamMockEnvironment streamMockEnvironment = new StreamMockEnvironment(
			jobConfig,
			streamConfig.getConfiguration(),
			executionConfig,
			memorySize,
			new MockInputSplitProvider(),
			bufferSize,
			taskStateManager);

		streamMockEnvironment.setCheckpointResponder(taskStateManager.getCheckpointResponder());
		initializeInputs(streamMockEnvironment);

		checkState(inputGates != null, "InputGates hasn't been initialised");

		StreamElementSerializer<OUT> outputStreamRecordSerializer = new StreamElementSerializer<>(outputSerializer);

		Queue<Object> outputList = new ArrayDeque<>();
		streamMockEnvironment.addOutput(outputList, outputStreamRecordSerializer);
		streamMockEnvironment.setTaskMetricGroup(taskMetricGroup);

		StreamTask<OUT, ?> task = taskFactory.apply(streamMockEnvironment);
		task.beforeInvoke();

		return new StreamTaskMailboxTestHarness<>(
			task,
			outputList,
			inputGates,
			streamMockEnvironment);
	}

	protected void initializeInputs(StreamMockEnvironment streamMockEnvironment) {
		inputGates = new StreamTestSingleInputGate[inputChannelsPerGate.size()];
		List<StreamEdge> inPhysicalEdges = new LinkedList<>();

		StreamNode mainNode = new StreamNode(StreamConfigChainer.MAIN_NODE_ID, null, null, (StreamOperator<?>) null, null, null);
		for (int i = 0; i < inputs.size(); i++) {
			if ((inputs.get(i) instanceof NetworkInputConfig)) {
				NetworkInputConfig networkInput = (NetworkInputConfig) inputs.get(i);
				initializeNetworkInput(networkInput, mainNode, streamMockEnvironment, inPhysicalEdges);
			}
			else if ((inputs.get(i) instanceof StreamTaskMailboxTestHarnessBuilder.SourceInputConfigPlaceHolder)) {
				SourceInputConfigPlaceHolder sourceInput = (SourceInputConfigPlaceHolder) inputs.get(i);
				inputs.set(i, initializeSourceInput(i, sourceInput, mainNode));
			}
			else {
				throw new UnsupportedOperationException("Unknown input type " + inputs.get(i));
			}
		}

		streamConfig.setInPhysicalEdges(inPhysicalEdges);
		streamConfig.setNumberOfNetworkInputs(inputGates.length);
		streamConfig.setInputs(inputs.toArray(new InputConfig[inputs.size()]));
	}

	private void initializeNetworkInput(
			NetworkInputConfig networkInput,
			StreamNode targetVertexDummy,
			StreamMockEnvironment streamMockEnvironment,
			List<StreamEdge> inPhysicalEdges) {
		int gateIndex = networkInput.getInputGateIndex();

		TypeSerializer<?> inputSerializer = networkInput.getTypeSerializer();
		inputGates[gateIndex] = new StreamTestSingleInputGate<>(
			inputChannelsPerGate.get(gateIndex),
			gateIndex,
			inputSerializer,
			bufferSize);

		StreamNode sourceVertexDummy = new StreamNode(0, null, null, (StreamOperator<?>) null, null, SourceStreamTask.class);
		StreamEdge streamEdge = new StreamEdge(
			sourceVertexDummy,
			targetVertexDummy,
			gateIndex + 1,
			new BroadcastPartitioner<>(),
			null);

		inPhysicalEdges.add(streamEdge);
		streamMockEnvironment.addInputGate(inputGates[gateIndex].getInputGate());
	}

	private SourceInputConfig initializeSourceInput(
			int inputId,
			SourceInputConfigPlaceHolder sourceInput,
			StreamNode mainNode) {
		Map<Integer, StreamConfig> transitiveChainedTaskConfigs = streamConfig.getTransitiveChainedTaskConfigs(getClass().getClassLoader());
		Integer maxNodeId = transitiveChainedTaskConfigs.isEmpty() ? StreamConfigChainer.MAIN_NODE_ID : Collections.max(transitiveChainedTaskConfigs.keySet());

		List<StreamEdge> outEdgesInOrder = new LinkedList<>();

		StreamEdge sourceToMainEdge = new StreamEdge(
			new StreamNode(maxNodeId + inputId + 1337, null, null, (StreamOperator<?>) null, null, null),
			mainNode,
			0,
			new ForwardPartitioner<>(),
			null);
		outEdgesInOrder.add(sourceToMainEdge);

		StreamConfig sourceConfig = new StreamConfig(new Configuration());
		sourceConfig.setTimeCharacteristic(streamConfig.getTimeCharacteristic());
		sourceConfig.setOutEdgesInOrder(outEdgesInOrder);
		sourceConfig.setChainedOutputs(outEdgesInOrder);
		sourceConfig.setTypeSerializerOut(outputSerializer);
		sourceConfig.setOperatorID(new OperatorID());
		sourceConfig.setStreamOperatorFactory(sourceInput.getSourceOperatorFactory());

		transitiveChainedTaskConfigs.put(sourceToMainEdge.getSourceId(), sourceConfig);
		streamConfig.setTransitiveChainedTaskConfigs(transitiveChainedTaskConfigs);
		return new SourceInputConfig(sourceToMainEdge);
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> setupOutputForSingletonOperatorChain(StreamOperator<?> operator) {
		return setupOutputForSingletonOperatorChain(SimpleOperatorFactory.of(operator), new OperatorID());
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> setupOutputForSingletonOperatorChain(
			StreamOperator<?> operator,
			OperatorID operatorID) {
		return setupOutputForSingletonOperatorChain(SimpleOperatorFactory.of(operator), operatorID);
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> setupOutputForSingletonOperatorChain(StreamOperatorFactory<?> factory) {
		return setupOutputForSingletonOperatorChain(factory, new OperatorID());
	}

	/**
	 * Users of the test harness can call this utility method to setup the stream config
	 * if there will only be a single operator to be tested. The method will setup the
	 * outgoing network connection for the operator.
	 *
	 * <p>For more advanced test cases such as testing chains of multiple operators with the harness,
	 * please manually configure the stream config.
	 */
	public StreamTaskMailboxTestHarnessBuilder<OUT> setupOutputForSingletonOperatorChain(
			StreamOperatorFactory<?> factory,
			OperatorID operatorID) {
		checkState(!setupCalled, "This harness was already setup.");
		return setupOperatorChain(operatorID, factory).finishForSingletonOperatorChain(outputSerializer);
	}

	public StreamConfigChainer<StreamTaskMailboxTestHarnessBuilder<OUT>> setupOperatorChain(StreamOperator<?> headOperator) {
		return setupOperatorChain(new OperatorID(), headOperator);
	}

	public StreamConfigChainer<StreamTaskMailboxTestHarnessBuilder<OUT>> setupOperatorChain(OperatorID headOperatorId, StreamOperator<?> headOperator) {
		return setupOperatorChain(headOperatorId, SimpleOperatorFactory.of(headOperator));
	}

	public StreamConfigChainer<StreamTaskMailboxTestHarnessBuilder<OUT>> setupOperatorChain(StreamOperatorFactory<?> headOperatorFactory) {
		return setupOperatorChain(new OperatorID(), headOperatorFactory);
	}

	public StreamConfigChainer<StreamTaskMailboxTestHarnessBuilder<OUT>> setupOperatorChain(OperatorID headOperatorId, StreamOperatorFactory<?> headOperatorFactory) {
		checkState(!setupCalled, "This harness was already setup.");
		setupCalled = true;
		streamConfig.setStreamOperatorFactory(headOperatorFactory);
		return new StreamConfigChainer<>(headOperatorId, streamConfig, this);
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> setTaskMetricGroup(TaskMetricGroup taskMetricGroup) {
		this.taskMetricGroup = taskMetricGroup;
		return this;
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> setKeyType(TypeInformation<?> keyType) {
		streamConfig.setStateKeySerializer(keyType.createSerializer(executionConfig));
		return this;
	}

	public StreamTaskMailboxTestHarnessBuilder<OUT> setTaskStateSnapshot(long checkpointId, TaskStateSnapshot snapshot) {
		taskStateSnapshots = Collections.singletonMap(checkpointId, snapshot);
		return this;
	}

	/**
	 * A place holder representation of a {@link SourceInputConfig}. When building the test harness
	 * it is replaced with {@link SourceInputConfig}.
	 */
	public static class SourceInputConfigPlaceHolder implements InputConfig {
		private SourceOperatorFactory<?> sourceOperatorFactory;

		public SourceInputConfigPlaceHolder(SourceOperatorFactory<?> sourceOperatorFactory) {
			this.sourceOperatorFactory = sourceOperatorFactory;
		}

		public SourceOperatorFactory<?> getSourceOperatorFactory() {
			return sourceOperatorFactory;
		}
	}
}

