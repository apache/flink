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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ClassLoaderUtil;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Internal configuration for a {@link StreamOperator}. This is created and populated by the
 * {@link StreamingJobGraphGenerator}.
 */
@Internal
public class OperatorConfig implements java.io.Serializable {

	private static final String SERIALIZED_OPERATOR = "serialized.operator";

	private static final String OPERATOR_NAME = "operator.name";

	private static final String NODE_ID = "node.id";

	//  ------------------------input---------------------------------

	private static final String TYPE_SERIALIZER_IN_1 = "type.serializer.in.1";

	private static final String TYPE_SERIALIZER_IN_2 = "type.serializer.in.2";

	// ----------------------------output-----------------------------

	private static final String TYPE_SERIALIZER_OUT_1 = "type.serializer.out.1";

	private static final String TYPE_SERIALIZER_SIDE_OUT_PREFIX = "type.serializer.side.out.";

	private static final String NON_CHAINED_OUTPUTS = "non.chained.outputs";

	private static final String CHAINED_OUTPUTS = "chained.outputs";

	private static final String OUTPUT_SELECTOR_WRAPPER = "output.selector.wrapper";

	// ----------------------------chain-----------------------------

	private static final String CHAIN_INDEX = "chain.index";

	private static final String IS_CHAIN_START = "is.chain.start";

	private static final String IS_CHAIN_END = "is.chain.end";

	// ----------------------------state-----------------------------

	private static final String STATE_KEY_SERIALIZER = "state.keyser";

	private static final String STATE_PARTITIONER_PREFIX = "state.partitioner.";

	private Configuration config;

	public OperatorConfig(Configuration configuration) {
		this.config = configuration;
	}

	public void setStreamOperator(StreamOperator<?> operator) {
		if (operator != null) {
			try {
				InstantiationUtil.writeObjectToConfig(operator, this.config, SERIALIZED_OPERATOR);
			} catch (IOException e) {
				throw new StreamTaskException("Cannot serialize operator object "
						+ operator.getClass() + ".", e);
			}
		}
	}

	public <T> T getStreamOperator(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, SERIALIZED_OPERATOR, cl);
		} catch (ClassNotFoundException e) {
			String classLoaderInfo = ClassLoaderUtil.getUserCodeClassLoaderInfo(cl);
			boolean loadableDoubleCheck = ClassLoaderUtil.validateClassLoadable(e, cl);

			String exceptionMessage = "Cannot load user class: " + e.getMessage()
					+ "\nClassLoader info: " + classLoaderInfo +
					(loadableDoubleCheck ?
							"\nClass was actually found in classloader - deserialization issue." :
							"\nClass not resolvable through given classloader.");

			throw new StreamTaskException(exceptionMessage);
		} catch (Exception e) {
			throw new StreamTaskException("Cannot instantiate user function.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerIn1(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_IN_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerIn2(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_IN_2, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	public <T> TypeSerializer<T> getTypeSerializerOut(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_OUT_1, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	public void setNonChainedOutputs(List<StreamEdge> nonChainedOutputs) {
		try {
			InstantiationUtil.writeObjectToConfig(nonChainedOutputs, this.config, NON_CHAINED_OUTPUTS);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize non chained outputs.", e);
		}
	}

	public List<StreamEdge> getNonChainedOutputs(ClassLoader cl) {
		try {
			List<StreamEdge> nonChainedOutputs = InstantiationUtil.readObjectFromConfig(this.config, NON_CHAINED_OUTPUTS, cl);
			return nonChainedOutputs == null ? new ArrayList<StreamEdge>() : nonChainedOutputs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate non chained outputs.", e);
		}
	}

	public void setChainedOutputs(List<StreamEdge> chainedOutputs) {
		try {
			InstantiationUtil.writeObjectToConfig(chainedOutputs, this.config, CHAINED_OUTPUTS);
		} catch (IOException e) {
			throw new StreamTaskException("Cannot serialize chained outputs.", e);
		}
	}

	public List<StreamEdge> getChainedOutputs(ClassLoader cl) {
		try {
			List<StreamEdge> chainedOutputs = InstantiationUtil.readObjectFromConfig(this.config, CHAINED_OUTPUTS, cl);
			return chainedOutputs == null ? new ArrayList<StreamEdge>() : chainedOutputs;
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate chained outputs.", e);
		}
	}

	public void setStatePartitioner(int input, KeySelector<?, ?> partitioner) {
		try {
			InstantiationUtil.writeObjectToConfig(partitioner, this.config, STATE_PARTITIONER_PREFIX + input);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize state partitioner.", e);
		}
	}

	public KeySelector<?, Serializable> getStatePartitioner(int input, ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, STATE_PARTITIONER_PREFIX + input, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate state partitioner.", e);
		}
	}

	public void setOperatorName(String name) {
		this.config.setString(OPERATOR_NAME, name);
	}

	public String getOperatorName() {
		return this.config.getString(OPERATOR_NAME, null);
	}

	public void setChainStart() {
		config.setBoolean(IS_CHAIN_START, true);
	}

	public boolean isChainStart() {
		return config.getBoolean(IS_CHAIN_START, false);
	}

	public void setChainEnd() {
		config.setBoolean(IS_CHAIN_END, true);
	}

	public boolean isChainEnd() {
		return config.getBoolean(IS_CHAIN_END, false);
	}

	public void setChainIndex(int index) {
		this.config.setInteger(CHAIN_INDEX, index);
	}

	public int getChainIndex() {
		return this.config.getInteger(CHAIN_INDEX, 0);
	}

	public void setNodeID(Integer nodeID) {
		config.setInteger(NODE_ID, nodeID);
	}

	public Integer getNodeID() {
		return config.getInteger(NODE_ID, -1);
	}

	public void setStateKeySerializer(TypeSerializer<?> serializer) {
		try {
			InstantiationUtil.writeObjectToConfig(serializer, this.config, STATE_KEY_SERIALIZER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize state key serializer.", e);
		}
	}

	public <K> TypeSerializer<K> getStateKeySerializer(ClassLoader cl) {
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, STATE_KEY_SERIALIZER, cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate state key serializer from task config.", e);
		}
	}

	public void setOutputSelectors(List<OutputSelector<?>> outputSelectors) {
		try {
			InstantiationUtil.writeObjectToConfig(outputSelectors, this.config, OUTPUT_SELECTOR_WRAPPER);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize output selectors", e);
		}
	}

	public <T> List<OutputSelector<T>> getOutputSelectors(ClassLoader userCodeClassloader) {
		try {
			List<OutputSelector<T>> selectors =
					InstantiationUtil.readObjectFromConfig(this.config, OUTPUT_SELECTOR_WRAPPER, userCodeClassloader);
			return selectors == null ? Collections.<OutputSelector<T>>emptyList() : selectors;

		} catch (Exception e) {
			throw new StreamTaskException("Could not read output selectors", e);
		}
	}

	public void setTypeSerializerIn1(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_1, serializer);
	}

	public void setTypeSerializerIn2(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_IN_2, serializer);
	}

	private void setTypeSerializer(String key, TypeSerializer<?> typeWrapper) {
		try {
			InstantiationUtil.writeObjectToConfig(typeWrapper, this.config, key);
		} catch (IOException e) {
			throw new StreamTaskException("Could not serialize type serializer.", e);
		}
	}

	public void setTypeSerializerOut(TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_OUT_1, serializer);
	}

	public void setTypeSerializerSideOut(OutputTag<?> outputTag, TypeSerializer<?> serializer) {
		setTypeSerializer(TYPE_SERIALIZER_SIDE_OUT_PREFIX + outputTag.getId(), serializer);
	}

	public <T> TypeSerializer<T> getTypeSerializerSideOut(OutputTag<?> outputTag, ClassLoader cl) {
		Preconditions.checkNotNull(outputTag, "Side output id must not be null.");
		try {
			return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_SIDE_OUT_PREFIX + outputTag.getId(), cl);
		} catch (Exception e) {
			throw new StreamTaskException("Could not instantiate serializer.", e);
		}
	}

	@Override
	public String toString() {
		ClassLoader cl = getClass().getClassLoader();
		StringBuilder builder = new StringBuilder();
		builder.append("\n=======================");
		builder.append("Operator Config");
		builder.append("=======================");
		builder.append("\nChain index: ").append(getChainIndex());
		builder.append("\nNode id: ").append(getNodeID());
		builder.append("\nNumber of chained outputs: ").append(getChainedOutputs(cl).size());
		builder.append("\nNumber of non chained outputs: ").append(getNonChainedOutputs(cl).size());
		builder.append("\nNon chained Output: ").append(getNonChainedOutputs(cl));
		builder.append("\nPartitioning:");
		for (StreamEdge output : getNonChainedOutputs(cl)) {
			int outputName = output.getTargetId();
			builder.append("\n\t").append(outputName).append(": ").append(output.getPartitioner());
		}

		try {
			builder.append("\nOperator: ").append(getStreamOperator(cl).getClass().getSimpleName());
		} catch (Exception e) {
			builder.append("\nOperator: Missing");
		}
		return builder.toString();
	}
}
