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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal configuration for a {@link StreamOperator}. This is created and populated by the
 * {@link StreamingJobGraphGenerator}.
 */
@Internal
public class OperatorConfig implements OperatorContext, java.io.Serializable {

	private StreamOperator<?> operator;
	private String name;
	private int nodeID = -1;

	//  ------------------------input---------------------------------
	private TypeSerializer<?> typeSerializer1;
	private TypeSerializer<?> typeSerializer2;

	// ----------------------------output-----------------------------
	private TypeSerializer<?> typeSerializerOut;
	private Map<String, TypeSerializer<?>> sideOutTypeSerializerMap = new HashMap<>();
	private List<StreamEdge> nonChainedOutputs;
	private List<StreamEdge> chainedOutputs;
	private List<OutputSelector<?>> outputSelectors;

	// ----------------------------chain-----------------------------
	private boolean isChainStart = false;
	private boolean isChainEnd = false;

	// ----------------------------state-----------------------------
	private TypeSerializer<?> stateKeySerializer;
	private KeySelector<?, ?> statePartitioner1;
	private KeySelector<?, ?> statePartitioner2;

	public void setStreamOperator(StreamOperator<?> operator) {
		this.operator = operator;
	}

	public <T extends StreamOperator> T getStreamOperator() {
		try {
			return (T) operator;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of operator.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerIn1() {
		try {
			return (TypeSerializer<T>) typeSerializer1;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of typeSerializer1.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerIn2() {
		try {
			return (TypeSerializer<T>) typeSerializer2;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of typeSerializer2.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerOut() {
		try {
			return (TypeSerializer<T>) typeSerializerOut;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of typeSerializerOut.", e);
		}
	}

	public void setNonChainedOutputs(List<StreamEdge> nonChainedOutputs) {
		this.nonChainedOutputs = nonChainedOutputs;
	}

	public List<StreamEdge> getNonChainedOutputs() {
		if (nonChainedOutputs != null) {
			return nonChainedOutputs;
		} else {
			return new ArrayList<>();
		}
	}

	public void setChainedOutputs(List<StreamEdge> chainedOutputs) {
		this.chainedOutputs = chainedOutputs;
	}

	public List<StreamEdge> getChainedOutputs() {
		if (chainedOutputs != null) {
			return chainedOutputs;
		} else {
			return new ArrayList<>();
		}
	}

	public void setStatePartitioner1(KeySelector<?, ?> keySelector) {
		this.statePartitioner1 = keySelector;
	}

	public KeySelector<?, Serializable> getStatePartitioner1() {
		try {
			return (KeySelector<?, Serializable>) statePartitioner1;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of statePartitioner1.", e);
		}
	}

	public void setStatePartitioner2(KeySelector<?, ?> keySelector) {
		this.statePartitioner2 = keySelector;
	}

	public KeySelector<?, Serializable> getStatePartitioner2() {
		try {
			return (KeySelector<?, Serializable>) statePartitioner2;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of statePartitioner2.", e);
		}
	}

	public void setOperatorName(String name) {
		this.name = name;
	}

	public String getOperatorName() {
		return name;
	}

	public void setChainStart() {
		this.isChainStart = true;
	}

	public boolean isChainStart() {
		return isChainStart;
	}

	public void setChainEnd() {
		isChainEnd = true;
	}

	public boolean isChainEnd() {
		return isChainEnd;
	}

	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}

	public int getNodeID() {
		return nodeID;
	}

	public void setStateKeySerializer(TypeSerializer<?> serializer) {
		this.stateKeySerializer = serializer;
	}

	public <T> TypeSerializer<T> getStateKeySerializer() {
		try {
			return (TypeSerializer<T>) stateKeySerializer;
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of stateKeySerializer.", e);
		}
	}

	public void setOutputSelectors(List<OutputSelector<?>> outputSelectors) {
		this.outputSelectors = outputSelectors;
	}

	public <T> List<OutputSelector<T>> getOutputSelectors() {
		if (outputSelectors == null) {
			return new ArrayList<>();
		}
		List<OutputSelector<T>> results = new ArrayList<>(outputSelectors.size());
		for (OutputSelector<?> outputSelector : outputSelectors) {
			try {
				results.add((OutputSelector<T>) outputSelector);
			} catch (ClassCastException e) {
				throw new StreamTaskException("Cannot cast type of outputSelector.", e);
			}
		}
		return results;
	}

	public void setTypeSerializerIn1(TypeSerializer<?> serializer) {
		this.typeSerializer1 = serializer;
	}

	public void setTypeSerializerIn2(TypeSerializer<?> serializer) {
		this.typeSerializer2 = serializer;
	}

	public void setTypeSerializerOut(TypeSerializer<?> serializer) {
		this.typeSerializerOut = serializer;
	}

	public void setTypeSerializerSideOut(OutputTag<?> outputTag, TypeSerializer<?> serializer) {
		sideOutTypeSerializerMap.put(outputTag.getId(), serializer);
	}

	public <T> TypeSerializer<T> getTypeSerializerSideOut(OutputTag<?> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output id must not be null.");
		try {
			return (TypeSerializer<T>) sideOutTypeSerializerMap.get(outputTag.getId());
		} catch (ClassCastException e) {
			throw new StreamTaskException("Cannot cast type of typeSerializerSideOut.", e);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("\n=======================");
		builder.append("Operator Settings");
		builder.append("=======================");
		builder.append("\nNode id: ").append(getNodeID());
		builder.append("\nNumber of chained outputs: ").append(getChainedOutputs().size());
		builder.append("\nNumber of non chained outputs: ").append(getNonChainedOutputs().size());
		builder.append("\nNon chained Output: ").append(getNonChainedOutputs());
		builder.append("\nPartitioning:");
		for (StreamEdge output : getNonChainedOutputs()) {
			int outputName = output.getTargetId();
			builder.append("\n\t").append(outputName).append(": ").append(output.getPartitioner());
		}

		try {
			builder.append("\nOperator: ").append(getStreamOperator().getClass().getSimpleName());
		} catch (Exception e) {
			builder.append("\nOperator: Missing");
		}
		return builder.toString();
	}
}
