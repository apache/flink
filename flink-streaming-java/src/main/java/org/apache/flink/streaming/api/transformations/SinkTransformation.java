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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;

import java.util.Collection;
import java.util.List;

/**
 * This Transformation represents a Sink.
 *
 * @param <T> The type of the elements in the input {@code SinkTransformation}
 */
@Internal
public class SinkTransformation<T> extends StreamTransformation<Object> {

	private final StreamTransformation<T> input;

	private final OneInputStreamOperator<T, Object> operator;

	/**
	 * Creates a new {@code SinkTransformation} from the given input {@code StreamTransformation}.
	 *
	 * @param input The input {@code StreamTransformation}
	 * @param name The name of the {@code StreamTransformation}, this will be shown in Visualizations and the Log
	 * @param operator The sink operator
	 * @param parallelism The parallelism of this {@code SinkTransformation}
	 */
	public SinkTransformation(
			StreamTransformation<T> input,
			String name,
			OneInputStreamOperator<T, Object> operator,
			int parallelism) {
		super(name, TypeExtractor.getForClass(Object.class), parallelism);
		this.input = input;
		this.operator = operator;
	}

	/**
	 * Returns the input {@code StreamTransformation} of this {@code SinkTransformation}.
	 */
	public StreamTransformation<T> getInput() {
		return input;
	}

	/**
	 * Returns the {@link StreamSink} that is the operator of this {@code SinkTransformation}.
	 */
	public OneInputStreamOperator<T, Object> getOperator() {
		return operator;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}
}
