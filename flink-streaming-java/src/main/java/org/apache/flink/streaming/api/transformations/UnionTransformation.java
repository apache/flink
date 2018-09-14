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
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This transformation represents a union of several input
 * {@link StreamTransformation StreamTransformations}.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code UnionTransformation}
 */
@Internal
public class UnionTransformation<T> extends StreamTransformation<T> {
	private final List<StreamTransformation<T>> inputs;

	/**
	 * Creates a new {@code UnionTransformation} from the given input {@code StreamTransformations}.
	 *
	 * <p>The input {@code StreamTransformations} must all have the same type.
	 *
	 * @param inputs The list of input {@code StreamTransformations}
	 */
	public UnionTransformation(List<StreamTransformation<T>> inputs) {
		super("Union", inputs.get(0).getOutputType(), inputs.get(0).getParallelism());

		for (StreamTransformation<T> input: inputs) {
			if (!input.getOutputType().equals(getOutputType())) {
				throw new UnsupportedOperationException("Type mismatch in input " + input);
			}
		}

		this.inputs = Lists.newArrayList(inputs);
	}

	/**
	 * Returns the list of input {@code StreamTransformations}.
	 */
	public List<StreamTransformation<T>> getInputs() {
		return inputs;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		for (StreamTransformation<T> input: inputs) {
			result.addAll(input.getTransitivePredecessors());
		}
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		throw new UnsupportedOperationException("Cannot set chaining strategy on Union Transformation.");
	}

}
