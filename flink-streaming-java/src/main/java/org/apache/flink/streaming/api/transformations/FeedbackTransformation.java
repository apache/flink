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
 * This represents a feedback point in a topology.
 *
 * <p>This is different from how iterations work in batch processing. Once a feedback point is
 * defined you can connect one or several {@code StreamTransformations} as a feedback edges.
 * Operations downstream from the feedback point will receive elements from the input of this
 * feedback point and from the feedback edges.
 *
 * <p>Both the partitioning of the input and the feedback edges is preserved. They can also have
 * differing partitioning strategies. This requires, however, that the parallelism of the feedback
 * {@code StreamTransformations} must match the parallelism of the input
 * {@code StreamTransformation}.
 *
 * <p>The type of the input {@code StreamTransformation} and the feedback
 * {@code StreamTransformation} must match.
 *
 * @param <T> The type of the input elements and the feedback elements.
 */
@Internal
public class FeedbackTransformation<T> extends StreamTransformation<T> {

	private final StreamTransformation<T> input;

	private final List<StreamTransformation<T>> feedbackEdges;

	private final Long waitTime;

	/**
	 * Creates a new {@code FeedbackTransformation} from the given input.
	 *
	 * @param input The input {@code StreamTransformation}
	 * @param waitTime The wait time of the feedback operator. After the time expires
	 *                          the operation will close and not receive any more feedback elements.
	 */
	public FeedbackTransformation(StreamTransformation<T> input, Long waitTime) {
		super("Feedback", input.getOutputType(), input.getParallelism());
		this.input = input;
		this.waitTime = waitTime;
		this.feedbackEdges = Lists.newArrayList();
	}

	/**
	 * Returns the input {@code StreamTransformation} of this {@code FeedbackTransformation}.
	 */
	public StreamTransformation<T> getInput() {
		return input;
	}

	/**
	 * Adds a feedback edge. The parallelism of the {@code StreamTransformation} must match
	 * the parallelism of the input {@code StreamTransformation} of this
	 * {@code FeedbackTransformation}
	 *
	 * @param transform The new feedback {@code StreamTransformation}.
	 */
	public void addFeedbackEdge(StreamTransformation<T> transform) {

		if (transform.getParallelism() != this.getParallelism()) {
			throw new UnsupportedOperationException(
					"Parallelism of the feedback stream must match the parallelism of the original" +
							" stream. Parallelism of original stream: " + this.getParallelism() +
							"; parallelism of feedback stream: " + transform.getParallelism() +
							". Parallelism can be modified using DataStream#setParallelism() method");
		}

		feedbackEdges.add(transform);
	}

	/**
	 * Returns the list of feedback {@code StreamTransformations}.
	 */
	public List<StreamTransformation<T>> getFeedbackEdges() {
		return feedbackEdges;
	}

	/**
	 * Returns the wait time. This is the amount of time that the feedback operator keeps listening
	 * for feedback elements. Once the time expires the operation will close and will not receive
	 * further elements.
	 */
	public Long getWaitTime() {
		return waitTime;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		throw new UnsupportedOperationException("Cannot set chaining strategy on Split Transformation.");
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}

