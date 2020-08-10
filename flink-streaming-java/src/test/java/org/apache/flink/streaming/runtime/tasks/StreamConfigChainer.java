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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Helper class to build StreamConfig for chain of operators.
 */
public class StreamConfigChainer<OWNER> {
	public static final int MAIN_NODE_ID = 0;
	private final OWNER owner;
	private final StreamConfig headConfig;
	private final Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();
	private final long bufferTimeout;

	private StreamConfig tailConfig;
	private int chainIndex = MAIN_NODE_ID;

	private final List<List<StreamEdge>> outEdgesInOrder = new LinkedList<>();
	private boolean setTailNonChainedOutputs = true;

	StreamConfigChainer(OperatorID headOperatorID, StreamConfig headConfig, OWNER owner) {
		this.owner = checkNotNull(owner);
		this.headConfig = checkNotNull(headConfig);
		this.tailConfig = checkNotNull(headConfig);
		this.bufferTimeout = headConfig.getBufferTimeout();

		head(headOperatorID);
	}

	private void head(OperatorID headOperatorID) {
		headConfig.setOperatorID(headOperatorID);
		headConfig.setChainStart();
		headConfig.setChainIndex(chainIndex);
		headConfig.setBufferTimeout(bufferTimeout);
	}

	@Deprecated
	public <T> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperator<T, T> operator,
			TypeSerializer<T> typeSerializer,
			boolean createKeyedStateBackend) {
		return chain(operatorID, operator, typeSerializer, typeSerializer, createKeyedStateBackend);
	}

	@Deprecated
	public <T> StreamConfigChainer<OWNER> chain(
			OneInputStreamOperator<T, T> operator,
			TypeSerializer<T> typeSerializer) {
		return chain(new OperatorID(), operator, typeSerializer);
	}

	@Deprecated
	public <T> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperator<T, T> operator,
			TypeSerializer<T> typeSerializer) {
		return chain(operatorID, operator, typeSerializer, typeSerializer, false);
	}

	@Deprecated
	public <T> StreamConfigChainer<OWNER> chain(
			OneInputStreamOperatorFactory<T, T> operatorFactory,
			TypeSerializer<T> typeSerializer) {
		return chain(new OperatorID(), operatorFactory, typeSerializer);
	}

	@Deprecated
	public <T> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperatorFactory<T, T> operatorFactory,
			TypeSerializer<T> typeSerializer) {
		return chain(operatorID, operatorFactory, typeSerializer, typeSerializer, false);
	}

	@Deprecated
	private <IN, OUT> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperator<IN, OUT> operator,
			TypeSerializer<IN> inputSerializer,
			TypeSerializer<OUT> outputSerializer,
			boolean createKeyedStateBackend) {
		return chain(
			operatorID,
			SimpleOperatorFactory.of(operator),
			inputSerializer,
			outputSerializer,
			createKeyedStateBackend);
	}

	@Deprecated
	public <IN, OUT> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeSerializer<IN> inputSerializer,
			TypeSerializer<OUT> outputSerializer,
			boolean createKeyedStateBackend) {

		chainIndex++;

		tailConfig.setChainedOutputs(Collections.singletonList(
			new StreamEdge(
				new StreamNode(tailConfig.getChainIndex(), null, null, (StreamOperator<?>) null, null, null, null),
				new StreamNode(chainIndex, null, null, (StreamOperator<?>) null, null, null, null),
				0,
				Collections.<String>emptyList(),
				null,
				null)));
		tailConfig = new StreamConfig(new Configuration());
		tailConfig.setStreamOperatorFactory(checkNotNull(operatorFactory));
		tailConfig.setOperatorID(checkNotNull(operatorID));
		tailConfig.setTypeSerializersIn(inputSerializer);
		tailConfig.setTypeSerializerOut(outputSerializer);
		if (createKeyedStateBackend) {
			// used to test multiple stateful operators chained in a single task.
			tailConfig.setStateKeySerializer(inputSerializer);
		}
		tailConfig.setChainIndex(chainIndex);
		tailConfig.setBufferTimeout(bufferTimeout);

		chainedConfigs.put(chainIndex, tailConfig);

		return this;
	}

	public <T> StreamConfigEdgeChainer<OWNER, T, T> chain(
			TypeSerializer<T> typeSerializer) {
		return chain(typeSerializer, typeSerializer);
	}

	public <IN, OUT> StreamConfigEdgeChainer<OWNER, IN, OUT> chain(
			TypeSerializer<IN> inputSerializer,
			TypeSerializer<OUT> outputSerializer) {
		return new StreamConfigEdgeChainer<>(this, inputSerializer, outputSerializer);
	}

	public OWNER finish() {
		if (setTailNonChainedOutputs) {
			List<StreamEdge> nonChainedOutputs = Collections.singletonList(new StreamEdge(
				new StreamNode(chainIndex, null, null, (StreamOperator<?>) null, null, null, null),
				new StreamNode(chainIndex , null, null, (StreamOperator<?>) null, null, null, null),
				0,
				Collections.<String>emptyList(),
				new BroadcastPartitioner<Object>(),
				null));
			outEdgesInOrder.add(nonChainedOutputs);
			tailConfig.setNumberOfOutputs(1);
			tailConfig.setNonChainedOutputs(nonChainedOutputs);
		}

		Collections.reverse(outEdgesInOrder);
		List<StreamEdge> allOutEdgesInOrder = outEdgesInOrder.stream()
			.flatMap(List::stream)
			.collect(Collectors.toList());

		tailConfig.setChainEnd();
		headConfig.setTransitiveChainedTaskConfigs(chainedConfigs);
		headConfig.setOutEdgesInOrder(allOutEdgesInOrder);

		return owner;
	}

	public <OUT> OWNER finishForSingletonOperatorChain(TypeSerializer<OUT> outputSerializer) {

		checkState(chainIndex == 0, "Use finishForSingletonOperatorChain");
		checkState(headConfig == tailConfig);

		StreamOperator<OUT> dummyOperator = new AbstractStreamOperator<OUT>() {
			private static final long serialVersionUID = 1L;
		};
		List<StreamEdge> outEdgesInOrder = new LinkedList<>();
		StreamNode sourceVertexDummy = new StreamNode(
			MAIN_NODE_ID,
			"group",
			null,
			dummyOperator,
			"source dummy",
			new LinkedList<>(),
			SourceStreamTask.class);
		StreamNode targetVertexDummy = new StreamNode(
			MAIN_NODE_ID + 1,
			"group",
			null,
			dummyOperator,
			"target dummy",
			new LinkedList<>(),
			SourceStreamTask.class);

		outEdgesInOrder.add(new StreamEdge(
			sourceVertexDummy,
			targetVertexDummy,
			0,
			new LinkedList<>(),
			new BroadcastPartitioner<>(),
			null));

		headConfig.setVertexID(0);
		headConfig.setNumberOfOutputs(1);
		headConfig.setOutEdgesInOrder(outEdgesInOrder);
		headConfig.setNonChainedOutputs(outEdgesInOrder);
		headConfig.setTransitiveChainedTaskConfigs(chainedConfigs);
		headConfig.setOutEdgesInOrder(outEdgesInOrder);
		headConfig.setTypeSerializerOut(outputSerializer);

		return owner;
	}

	/**
	 * Helper class to build operator node.
	 */
	public static class StreamConfigEdgeChainer<OWNER, IN, OUT> {
		private final OutputTag<Void> placeHolderTag;
		private StreamConfigChainer<OWNER> parent;
		private OperatorID operatorID;

		private final TypeSerializer<IN> inputSerializer;
		private final TypeSerializer<OUT> outputSerializer;

		private StreamOperatorFactory<OUT> operatorFactory;
		private List<OutputSelector<?>> outputSelectors = new ArrayList<>();

		private Map<OutputTag<?>, Tuple2<Integer, List<String>>> nonChainedOutPuts;
		private boolean createKeyedStateBackend;

		private int nonChainIndex = -1;

		private StreamConfigEdgeChainer(
				StreamConfigChainer<OWNER> parent,
				TypeSerializer<IN> inputSerializer,
				TypeSerializer<OUT> outputSerializer) {
			this.parent = parent;
			this.parent.setTailNonChainedOutputs = true;

			this.inputSerializer = inputSerializer;
			this.outputSerializer = outputSerializer;
			this.placeHolderTag = new OutputTag<>("FLINK_PLACEHOLDER", BasicTypeInfo.VOID_TYPE_INFO);
			this.nonChainedOutPuts = new HashMap<>(4);
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> setOperatorID(OperatorID operatorID) {
			this.operatorID = operatorID;
			return this;
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> setOperatorFactory(StreamOperatorFactory operatorFactory) {
			this.operatorFactory = operatorFactory;
			return this;
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> setOutputSelectors(List<OutputSelector<?>> outputSelectors) {
			this.outputSelectors = outputSelectors;
			return this;
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> addNonChainedOutputsCount(int nonChainedOutputsCount) {
			return addNonChainedOutputsCount(nonChainedOutputsCount, Collections.emptyList());
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> addNonChainedOutputsCount(int nonChainedOutputsCount, List<String> selectedNames) {
			return addNonChainedOutputsCount(placeHolderTag, nonChainedOutputsCount, selectedNames);
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> addNonChainedOutputsCount(OutputTag<?> outputTag, int nonChainedOutputsCount) {
			return addNonChainedOutputsCount(outputTag, nonChainedOutputsCount, Collections.emptyList());
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> addNonChainedOutputsCount(OutputTag<?> outputTag, int nonChainedOutputsCount, List<String> selectedNames) {
			checkArgument(nonChainedOutputsCount >= 0 && outputTag != null);
			this.nonChainedOutPuts.put(outputTag, Tuple2.of(nonChainedOutputsCount, selectedNames));
			return this;
		}

		public StreamConfigEdgeChainer<OWNER, IN, OUT> setCreateKeyedStateBackend(boolean createKeyedStateBackend) {
			this.createKeyedStateBackend = createKeyedStateBackend;
			return this;
		}

		public StreamConfigChainer<OWNER> build() {
			parent.chainIndex++;

			parent.tailConfig.setChainedOutputs(Collections.singletonList(
				new StreamEdge(
					new StreamNode(parent.tailConfig.getChainIndex(), null, null, (StreamOperator<?>) null, null, null, null),
					new StreamNode(parent.chainIndex, null, null, (StreamOperator<?>) null, null, null, null),
					0,
					Collections.<String>emptyList(),
					null,
					null)));
			parent.tailConfig = new StreamConfig(new Configuration());
			parent.tailConfig.setStreamOperatorFactory(checkNotNull(operatorFactory));
			parent.tailConfig.setOperatorID(operatorID == null ? new OperatorID() : operatorID);
			parent.tailConfig.setTypeSerializersIn(checkNotNull(inputSerializer));
			parent.tailConfig.setTypeSerializerOut(checkNotNull(outputSerializer));
			parent.tailConfig.setOutputSelectors(outputSelectors == null ? Collections.emptyList() : outputSelectors);
			if (createKeyedStateBackend) {
				// used to test multiple stateful operators chained in a single task.
				parent.tailConfig.setStateKeySerializer(inputSerializer);
			}
			parent.tailConfig.setChainIndex(parent.chainIndex);
			parent.tailConfig.setBufferTimeout(parent.bufferTimeout);
			if (!nonChainedOutPuts.isEmpty()) {
				List<StreamEdge> nonChainedOutputs = createNonChainedOutputs(nonChainedOutPuts);
				parent.tailConfig.setNonChainedOutputs(nonChainedOutputs);
				parent.tailConfig.setNumberOfOutputs(nonChainedOutputs.size());
				parent.outEdgesInOrder.add(nonChainedOutputs);
				parent.setTailNonChainedOutputs = false;
			}
			parent.chainedConfigs.put(parent.chainIndex, parent.tailConfig);
			return parent;
		}

		private List<StreamEdge> createNonChainedOutputs(Map<OutputTag<?>, Tuple2<Integer, List<String>>> nonChainedOutputsCount) {
			List<StreamEdge> nonChainedOutputs = new ArrayList<>();
			nonChainedOutputsCount.forEach((outputTag, value) -> {
				for (int i = 0; i < value.f0; i++) {
					nonChainedOutputs.add(new StreamEdge(
						new StreamNode(parent.tailConfig.getChainIndex(), null, null, (StreamOperator<?>) null, null, null, null),
						new StreamNode(nonChainIndex--, null, null, (StreamOperator<?>) null, null, null, null),
						0,
						value.f1,
						new BroadcastPartitioner<Object>(),
						placeHolderTag.equals(outputTag) ? null : outputTag));
					if (!placeHolderTag.equals(outputTag)) {
						parent.tailConfig.setTypeSerializerSideOut(outputTag, outputSerializer);
					}
				}
			});
			return nonChainedOutputs;
		}
	}
}
