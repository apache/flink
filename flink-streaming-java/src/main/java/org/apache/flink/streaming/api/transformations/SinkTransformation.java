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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This Transformation represents a Sink.
 *
 * @param <T> The type of the elements in the input {@code SinkTransformation}
 */
@Internal
public class SinkTransformation<T> extends PhysicalTransformation<Object> {

	private final Transformation<T> input;

	private final StreamOperatorFactory<Object> operatorFactory;

	// We need this because sinks can also have state that is partitioned by key
	private KeySelector<T, ?> stateKeySelector;

	private TypeInformation<?> stateKeyType;

	/**
	 * Creates a new {@code SinkTransformation} from the given input {@code Transformation}.
	 *
	 * @param input The input {@code Transformation}
	 * @param name The name of the {@code Transformation}, this will be shown in Visualizations and the Log
	 * @param operator The sink operator
	 * @param parallelism The parallelism of this {@code SinkTransformation}
	 */
	public SinkTransformation(
			Transformation<T> input,
			String name,
			StreamSink<T> operator,
			int parallelism) {
		this(input, name, SimpleOperatorFactory.of(operator), parallelism);
	}

	public SinkTransformation(
			Transformation<T> input,
			String name,
			StreamOperatorFactory<Object> operatorFactory,
			int parallelism) {
		super(name, TypeExtractor.getForClass(Object.class), parallelism);
		this.input = input;
		this.operatorFactory = operatorFactory;
	}

	/**
	 * Returns the input {@code Transformation} of this {@code SinkTransformation}.
	 */
	public Transformation<T> getInput() {
		return input;
	}

	@VisibleForTesting
	public StreamSink<T> getOperator() {
		return (StreamSink<T>) ((SimpleOperatorFactory) operatorFactory).getOperator();
	}

	/**
	 * Returns the {@link StreamOperatorFactory} of this {@code SinkTransformation}.
	 */
	public StreamOperatorFactory<Object> getOperatorFactory() {
		return operatorFactory;
	}

	/**
	 * Sets the {@link KeySelector} that must be used for partitioning keyed state of this Sink.
	 *
	 * @param stateKeySelector The {@code KeySelector} to set
	 */
	public void setStateKeySelector(KeySelector<T, ?> stateKeySelector) {
		this.stateKeySelector = stateKeySelector;
	}

	/**
	 * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
	 * Sink.
	 *
	 * @see #setStateKeySelector
	 */
	public KeySelector<T, ?> getStateKeySelector() {
		return stateKeySelector;
	}

	public void setStateKeyType(TypeInformation<?> stateKeyType) {
		this.stateKeyType = stateKeyType;
	}

	public TypeInformation<?> getStateKeyType() {
		return stateKeyType;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operatorFactory.setChainingStrategy(strategy);
	}
}
