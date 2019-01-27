/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.functions;

import org.apache.flink.util.Preconditions;

/**
 * An implementation of {@link Merger} which merges two values with an
 * aggregate function.
 *
 * @param <T> The type of the values to be merged.
 */
public class AggregateMerger<T> implements Merger<T> {

	private static final long serialVersionUID = 1L;

	/** The aggregate function used to merge values. */
	private final AggregateFunction<?, T, ?> aggregateFunction;

	/**
	 * Creates a new instance with the given aggregate function.
	 *
	 * @param aggregateFunction The aggregate function to merge values.
	 */
	public AggregateMerger(final AggregateFunction<?, T, ?> aggregateFunction) {
		Preconditions.checkNotNull(aggregateFunction);
		this.aggregateFunction = aggregateFunction;
	}

	@Override
	public T merge(T value1, T value2) {
		return aggregateFunction.merge(value1, value2);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		AggregateMerger<?> that = (AggregateMerger<?>) o;
		return aggregateFunction.equals(that.aggregateFunction);
	}

	@Override
	public int hashCode() {
		return aggregateFunction.hashCode();
	}

	@Override
	public String toString() {
		return "AggregateMerger{" +
			"aggregateFunction=" + aggregateFunction +
			"}";
	}
}
