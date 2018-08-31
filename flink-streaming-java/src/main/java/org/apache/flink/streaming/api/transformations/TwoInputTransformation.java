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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This Transformation represents the application of a
 * {@link TwoInputStreamOperator} to two input {@code StreamTransformations}.
 * The result is again only one stream.
 *
 * @param <IN1> The type of the elements in the first input {@code StreamTransformation}
 * @param <IN2> The type of the elements in the second input {@code StreamTransformation}
 * @param <OUT> The type of the elements that result from this {@code TwoInputTransformation}
 */
@Internal
public class TwoInputTransformation<IN1, IN2, OUT> extends StreamTransformation<OUT> {

	private final StreamTransformation<IN1> input1;
	private final StreamTransformation<IN2> input2;

	private final TwoInputStreamOperator<IN1, IN2, OUT> operator;

	private KeySelector<IN1, ?> stateKeySelector1;

	private KeySelector<IN2, ?> stateKeySelector2;

	private TypeInformation<?> stateKeyType;

	/**
	 * Creates a new {@code TwoInputTransformation} from the given inputs and operator.
	 *
	 * @param input1 The first input {@code StreamTransformation}
	 * @param input2 The second input {@code StreamTransformation}
	 * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
	 * @param operator The {@code TwoInputStreamOperator}
	 * @param outputType The type of the elements produced by this Transformation
	 * @param parallelism The parallelism of this Transformation
	 */
	public TwoInputTransformation(
			StreamTransformation<IN1> input1,
			StreamTransformation<IN2> input2,
			String name,
			TwoInputStreamOperator<IN1, IN2, OUT> operator,
			TypeInformation<OUT> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.input1 = input1;
		this.input2 = input2;
		this.operator = operator;
	}

	/**
	 * Returns the first input {@code StreamTransformation} of this {@code TwoInputTransformation}.
	 */
	public StreamTransformation<IN1> getInput1() {
		return input1;
	}

	/**
	 * Returns the first input {@code StreamTransformation} of this {@code TwoInputTransformation}.
	 */
	public StreamTransformation<IN2> getInput2() {
		return input2;
	}

	/**
	 * Returns the {@code TypeInformation} for the elements from the first input.
	 */
	public TypeInformation<IN1> getInputType1() {
		return input1.getOutputType();
	}

	/**
	 * Returns the {@code TypeInformation} for the elements from the first input.
	 */
	public TypeInformation<IN2> getInputType2() {
		return input2.getOutputType();
	}

	/**
	 * Returns the {@code TwoInputStreamOperator} of this Transformation.
	 */
	public TwoInputStreamOperator<IN1, IN2, OUT> getOperator() {
		return operator;
	}

	/**
	 * Sets the {@link KeySelector KeySelectors} that must be used for partitioning keyed state of
	 * this transformation.
	 *
	 * @param stateKeySelector1 The {@code KeySelector} to set for the first input
	 * @param stateKeySelector2 The {@code KeySelector} to set for the first input
	 */
	public void setStateKeySelectors(KeySelector<IN1, ?> stateKeySelector1, KeySelector<IN2, ?> stateKeySelector2) {
		this.stateKeySelector1 = stateKeySelector1;
		this.stateKeySelector2 = stateKeySelector2;
	}

	/**
	 * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
	 * Operation for the first input.
	 *
	 * @see #setStateKeySelectors
	 */
	public KeySelector<IN1, ?> getStateKeySelector1() {
		return stateKeySelector1;
	}

	/**
	 * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
	 * Operation for the second input.
	 *
	 * @see #setStateKeySelectors
	 */
	public KeySelector<IN2, ?> getStateKeySelector2() {
		return stateKeySelector2;
	}

	public void setStateKeyType(TypeInformation<?> stateKeyType) {
		this.stateKeyType = stateKeyType;
	}

	public TypeInformation<?> getStateKeyType() {
		return stateKeyType;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input1.getTransitivePredecessors());
		result.addAll(input2.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}

}
