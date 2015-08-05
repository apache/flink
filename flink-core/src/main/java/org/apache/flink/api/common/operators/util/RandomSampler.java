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
package org.apache.flink.api.common.operators.util;

import java.util.Iterator;

/**
 * A data sample is a set of data collected and/or selected from a statistical population by a defined procedure.
 * RandomSampler is the util which helps to collect data sample randomly.
 *
 * @param <T> The type of sampler data.
 */
public abstract class RandomSampler<T> {
	
	protected final Iterator<T> EMPTY_ITERABLE = new SampledIterator<T>() {
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
	 * Randomly sample the elements from input, and return the result iterator.
	 *
	 * @param input source data
	 * @return the sample result.
	 */
	public abstract Iterator<T> sample(Iterator<T> input);
}

/**
 * An abstract iterator which does not support remove.
 * @param <T> The type of iterator data.
 */
abstract class SampledIterator<T> implements Iterator<T> {
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Do not support this operation.");
	}
}
