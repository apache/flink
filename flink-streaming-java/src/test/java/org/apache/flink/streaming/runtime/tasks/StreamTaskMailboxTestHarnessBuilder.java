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
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder class for {@link StreamTaskMailboxTestHarness}.
 */
public abstract class StreamTaskMailboxTestHarnessBuilder<OUT> {
	protected final FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory;
	protected final TypeSerializer<OUT> outputSerializer;
	protected final ExecutionConfig executionConfig = new ExecutionConfig();

	protected long memorySize = 1024 * 1024;
	protected int bufferSize = 1024;
	protected long bufferTimeout = 0;
	protected Configuration jobConfig = new Configuration();
	protected Configuration taskConfig = new Configuration();
	protected StreamConfig streamConfig = new StreamConfig(taskConfig);
	protected LocalRecoveryConfig localRecoveryConfig = TestLocalRecoveryConfig.disabled();
	@Nullable
	protected StreamTestSingleInputGate[] inputGates;
	protected TaskMetricGroup taskMetricGroup = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();

	private boolean setupCalled = false;

	public StreamTaskMailboxTestHarnessBuilder(
			FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory,
			TypeInformation<OUT> outputType) {
		this.taskFactory = checkNotNull(taskFactory);
		outputSerializer = outputType.createSerializer(executionConfig);
	}

	public StreamTaskMailboxTestHarness<OUT> build() throws Exception {
		streamConfig.setBufferTimeout(bufferTimeout);

		TestTaskStateManager taskStateManager = new TestTaskStateManager(localRecoveryConfig);

		StreamMockEnvironment streamMockEnvironment = new StreamMockEnvironment(
			jobConfig,
			taskConfig,
			executionConfig,
			memorySize,
			new MockInputSplitProvider(),
			bufferSize,
			taskStateManager);

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
			localRecoveryConfig,
			inputGates,
			streamMockEnvironment);
	}

	protected abstract void initializeInputs(StreamMockEnvironment streamMockEnvironment);

	public StreamTaskMailboxTestHarnessBuilder<OUT> setupOutputForSingletonOperatorChain(StreamOperator<?> operator) {
		return setupOutputForSingletonOperatorChain(SimpleOperatorFactory.of(operator));
	}

	/**
	 * Users of the test harness can call this utility method to setup the stream config
	 * if there will only be a single operator to be tested. The method will setup the
	 * outgoing network connection for the operator.
	 *
	 * <p>For more advanced test cases such as testing chains of multiple operators with the harness,
	 * please manually configure the stream config.
	 */
	public StreamTaskMailboxTestHarnessBuilder<OUT> setupOutputForSingletonOperatorChain(StreamOperatorFactory<?> factory) {
		checkState(!setupCalled, "This harness was already setup.");
		setupCalled = true;
		streamConfig.setChainStart();
		streamConfig.setTimeCharacteristic(TimeCharacteristic.EventTime);
		streamConfig.setOutputSelectors(Collections.<OutputSelector<?>>emptyList());
		streamConfig.setNumberOfOutputs(1);
		streamConfig.setTypeSerializerOut(outputSerializer);
		streamConfig.setVertexID(0);
		streamConfig.setOperatorID(new OperatorID(4711L, 123L));

		StreamOperator<OUT> dummyOperator = new AbstractStreamOperator<OUT>() {
			private static final long serialVersionUID = 1L;
		};

		List<StreamEdge> outEdgesInOrder = new LinkedList<StreamEdge>();
		StreamNode sourceVertexDummy = new StreamNode(0, "group", null, dummyOperator, "source dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(1, "group", null, dummyOperator, "target dummy", new LinkedList<OutputSelector<?>>(), SourceStreamTask.class);

		outEdgesInOrder.add(new StreamEdge(sourceVertexDummy, targetVertexDummy, 0, new LinkedList<String>(), new BroadcastPartitioner<Object>(), null /* output tag */));

		streamConfig.setOutEdgesInOrder(outEdgesInOrder);
		streamConfig.setNonChainedOutputs(outEdgesInOrder);

		streamConfig.setStreamOperatorFactory(factory);
		streamConfig.setOperatorID(new OperatorID());

		return this;
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
}

