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
 * An implementation of {@link Merger} which merges two values with a reduce
 * function.
 *
 * @param <T> The type of the values to be merged.
 */
public class ReduceMerger<T> implements Merger<T> {

	private static final long serialVersionUID = 1L;

	/** The reduce function to merge values. */
	private final ReduceFunction<T> reduceFunction;

	/**
	 * Creates a new instance with the given reduce function.
	 *
	 * @param reduceFunction The reduce function to merge values.
	 */
	public ReduceMerger(final ReduceFunction<T> reduceFunction) {
		Preconditions.checkNotNull(reduceFunction);
		this.reduceFunction = reduceFunction;
	}

	@Override
	public T merge(T value1, T value2) {
		try {
			return reduceFunction.reduce(value1, value2);
		} catch (Exception e) {
			throw new RuntimeException("Could not properly reduce the values.", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ReduceMerger<?> that = (ReduceMerger<?>) o;
		return reduceFunction.equals(that.reduceFunction);
	}

	@Override
	public int hashCode() {
		return reduceFunction.hashCode();
	}

	@Override
	public String toString() {
		return "ReduceMerger{" +
			"reduceFunction=" +
			reduceFunction +
			"}";
	}
}
