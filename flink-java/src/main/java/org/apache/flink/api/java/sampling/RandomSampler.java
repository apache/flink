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

package org.apache.flink.api.java.sampling;

import org.apache.flink.annotation.Internal;

import java.util.Iterator;

/**
 * A data sample is a set of data selected from a statistical population by a defined procedure.
 * RandomSampler helps to create data sample randomly.
 *
 * @param <T> The type of sampler data.
 */
@Internal
public abstract class RandomSampler<T> {

	protected static final double EPSILON = 1e-5;

	protected final Iterator<T> emptyIterable = new SampledIterator<T>() {
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public T next() {
			return null;
		}
	};

	/**
	 * Randomly sample the elements from input in sequence, and return the result iterator.
	 *
	 * @param input Source data
	 * @return The sample result.
	 */
	public abstract Iterator<T> sample(Iterator<T> input);

}

/**
 * A simple abstract iterator which implements the remove method as unsupported operation.
 *
 * @param <T> The type of iterator data.
 */
@Internal
abstract class SampledIterator<T> implements Iterator<T> {
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Do not support this operation.");
	}

}
