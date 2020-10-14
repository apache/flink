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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for transformations representing the application of a
 * {@link org.apache.flink.streaming.api.operators.MultipleInputStreamOperator}
 * to input {@code Transformations}. The result is again only one stream.
 *
 * @param <OUT> The type of the elements that result from this {@code MultipleInputTransformation}
 */
@Internal
public abstract class AbstractMultipleInputTransformation<OUT> extends PhysicalTransformation<OUT> {

	protected final List<Transformation<?>> inputs = new ArrayList<>();
	protected final StreamOperatorFactory<OUT> operatorFactory;

	public AbstractMultipleInputTransformation(
			String name,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<OUT> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.operatorFactory = operatorFactory;
	}

	@Override
	public List<Transformation<?>> getInputs() {
		return inputs;
	}

	/**
	 * Returns the {@code TypeInformation} for the elements from the inputs.
	 */
	public List<TypeInformation<?>> getInputTypes() {
		return inputs.stream()
			.map(Transformation::getOutputType)
			.collect(Collectors.toList());
	}

	/**
	 * Returns the {@code StreamOperatorFactory} of this Transformation.
	 */
	public StreamOperatorFactory<OUT> getOperatorFactory() {
		return operatorFactory;
	}

	@Override
	public List<Transformation<?>> getTransitivePredecessors() {
		return inputs.stream()
			.flatMap(input -> input.getTransitivePredecessors().stream())
			.collect(Collectors.toList());
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operatorFactory.setChainingStrategy(strategy);
	}
}
