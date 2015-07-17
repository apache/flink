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

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * A sampler implementation based on a Multinomial distribution. An element is sampled only if
 * the specified event holds true.
 *
 * @param <T> The type of sample.
 */
public class MultinomialSampler<T> extends RandomSampler<T> {

	private final double lower;
	private final double upper;
	private final Random random;

	/**
	 * Create a multinomial sampler with the multinomial distribution and the required event
	 * which needs to be satisfied. The default random number generator is used.
	 *
	 * @param distribution The multinomial probability distribution
	 * @param eventId      Index of the event which must be satisfied for sampling
	 */
	public MultinomialSampler(List<Double> distribution, int eventId) {
		this(distribution, eventId, new Random());
	}

	/**
	 * Create a multinomial sampler with the multinomial distribution and the required event
	 * which needs to be satisfied.
	 *
	 * @param distribution The multinomial probability distribution
	 * @param eventId      Index of the event which must be satisfied for sampling
	 * @param seed         Seed for the random number generation
	 */
	public MultinomialSampler(List<Double> distribution, int eventId, long seed) {
		this(distribution, eventId, new Random(seed));
	}

	/**
	 * Create a multinomial sampler with the multinomial distribution and the required event
	 * which needs to be satisfied.
	 *
	 * @param distribution The multinomial probability distribution
	 * @param eventId      Index of the event which must be satisfied for sampling
	 * @param random       The random number generator to be used
	 */
	public MultinomialSampler(List<Double> distribution, int eventId, Random random) {
		// build a cumulative distribution checking that all probabilities are valid
		int length = distribution.size();
		double[] cumulative = new double[length + 1];
		cumulative[0] = 0.0d;

		for (int i = 0; i < length; i++) {
			Preconditions.checkArgument(distribution.get(i) >= 0, "All probabilities must be non-negative");
			cumulative[i + 1] = cumulative[i] + distribution.get(i);
		}

		if (Math.abs(cumulative[length] - 1) > 1e-9) {
			throw new IllegalArgumentException("Sum of all probabilities must be one");
		}

		// now set the lower and upper bounds for sampling
		this.lower = cumulative[eventId];
		// for the last event, allow a little over 1.0d since we take a strict less than.
		this.upper = (eventId == length - 1) ? (cumulative[eventId + 1] + 1e-9) : cumulative[eventId + 1];
		this.random = random;
	}

	/**
	 * Sample the input elements, for each input element, take a multinomial trial.
	 *
	 * @param input Elements to be sampled.
	 * @return The sampled result which is lazy computed upon input elements.
	 */
	@Override
	public Iterator<T> sample(final Iterator<T> input) {
		if (lower == upper) {
			return EMPTY_ITERABLE;
		}

		return new SampledIterator<T>() {
			T current = null;

			@Override
			public boolean hasNext() {
				if (current == null) {
					current = getNextSampledElement();
				}

				return current != null;
			}

			@Override
			public T next() {
				if (current == null) {
					return getNextSampledElement();
				} else {
					T result = current;
					current = null;

					return result;
				}
			}

			private T getNextSampledElement() {
				while (input.hasNext()) {
					T element = input.next();
					double rng = random.nextDouble();

					if (rng >= lower && rng < upper) {
						return element;
					}
				}

				return null;
			}
		};
	}
}
