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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class to build StreamConfig for chain of operators.
 */
public class StreamConfigChainer<OWNER> {
	private final OWNER owner;
	private final StreamConfig headConfig;
	private final Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();

	private StreamConfig tailConfig;
	private int chainIndex = 0;

	StreamConfigChainer(OperatorID headOperatorID, StreamConfig headConfig, OWNER owner) {
		this.owner = checkNotNull(owner);
		this.headConfig = checkNotNull(headConfig);
		this.tailConfig = checkNotNull(headConfig);

		head(headOperatorID);
	}

	private void head(OperatorID headOperatorID) {
		headConfig.setOperatorID(headOperatorID);
		headConfig.setChainStart();
		headConfig.setChainIndex(chainIndex);
	}

	public <T> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperator<T, T> operator,
			TypeSerializer<T> typeSerializer,
			boolean createKeyedStateBackend) {
		return chain(operatorID, operator, typeSerializer, typeSerializer, createKeyedStateBackend);
	}

	public <T> StreamConfigChainer<OWNER> chain(
			OneInputStreamOperator<T, T> operator,
			TypeSerializer<T> typeSerializer) {
		return chain(new OperatorID(), operator, typeSerializer);
	}

	public <T> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperator<T, T> operator,
			TypeSerializer<T> typeSerializer) {
		return chain(operatorID, operator, typeSerializer, typeSerializer, false);
	}

	public <T> StreamConfigChainer<OWNER> chain(
			OneInputStreamOperatorFactory<T, T> operatorFactory,
			TypeSerializer<T> typeSerializer) {
		return chain(new OperatorID(), operatorFactory, typeSerializer);
	}

	public <T> StreamConfigChainer<OWNER> chain(
			OperatorID operatorID,
			OneInputStreamOperatorFactory<T, T> operatorFactory,
			TypeSerializer<T> typeSerializer) {
		return chain(operatorID, operatorFactory, typeSerializer, typeSerializer, false);
	}

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

		chainedConfigs.put(chainIndex, tailConfig);

		return this;
	}

	public OWNER finish() {
		List<StreamEdge> outEdgesInOrder = new LinkedList<StreamEdge>();
		outEdgesInOrder.add(
			new StreamEdge(
				new StreamNode(chainIndex, null, null, (StreamOperator<?>) null, null, null, null),
				new StreamNode(chainIndex , null, null, (StreamOperator<?>) null, null, null, null),
				0,
				Collections.<String>emptyList(),
				new BroadcastPartitioner<Object>(),
				null));

		tailConfig.setChainEnd();
		tailConfig.setOutputSelectors(Collections.emptyList());
		tailConfig.setNumberOfOutputs(1);
		tailConfig.setOutEdgesInOrder(outEdgesInOrder);
		tailConfig.setNonChainedOutputs(outEdgesInOrder);
		headConfig.setTransitiveChainedTaskConfigs(chainedConfigs);
		headConfig.setOutEdgesInOrder(outEdgesInOrder);

		return owner;
	}
}
