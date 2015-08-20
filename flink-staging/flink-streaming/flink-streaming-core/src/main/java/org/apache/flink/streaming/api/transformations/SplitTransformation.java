/**
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

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.util.Collection;
import java.util.List;

/**
 * This transformation represents a split of one
 * {@link org.apache.flink.streaming.api.datastream.DataStream} into several {@code DataStreams}
 * using an {@link org.apache.flink.streaming.api.collector.selector.OutputSelector}.
 *
 * <p>
 * This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code SplitTransformation}
 */
public class SplitTransformation<T> extends StreamTransformation<T> {

	private final StreamTransformation<T> input;

	private final OutputSelector<T> outputSelector;

	/**
	 * Creates a new {@code SplitTransformation} from the given input and {@code OutputSelector}.
	 *
	 * @param input The input {@code StreamTransformation}
	 * @param outputSelector The output selector
	 */
	public SplitTransformation(StreamTransformation<T> input,
			OutputSelector<T> outputSelector) {
		super("Split", input.getOutputType(), input.getParallelism());
		this.input = input;
		this.outputSelector = outputSelector;
	}

	/**
	 * Returns the input {@code StreamTransformation}.
	 */
	public StreamTransformation<T> getInput() {
		return input;
	}

	/**
	 * Returns the {@code OutputSelector}
	 */
	public OutputSelector<T> getOutputSelector() {
		return outputSelector;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(StreamOperator.ChainingStrategy strategy) {
		throw new UnsupportedOperationException("Cannot set chaining strategy on Split Transformation.");
	}
}

