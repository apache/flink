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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.XORShiftRandom;

import java.util.Iterator;
import java.util.Random;

/**
 * A sampler implementation built upon a Bernoulli trail. This sampler is used to sample with
 * fraction and without replacement. Whether an element is sampled or not is determined by a
 * Bernoulli experiment.
 *
 * @param <T> The type of sample.
 * @see <a href="http://erikerlandson.github.io/blog/2014/09/11/faster-random-samples-with-gap-sampling/">Gap Sampling</a>
 */
@Internal
public class BernoulliSampler<T> extends RandomSampler<T> {

	private final double fraction;
	private final Random random;

	// THRESHOLD is a tuning parameter for choosing sampling method according to the fraction.
	private static final double THRESHOLD = 0.33;

	/**
	 * Create a Bernoulli sampler with sample fraction and default random number generator.
	 *
	 * @param fraction Sample fraction, aka the Bernoulli sampler possibility.
	 */
	public BernoulliSampler(double fraction) {
		this(fraction, new XORShiftRandom());
	}

	/**
	 * Create a Bernoulli sampler with sample fraction and random number generator seed.
	 *
	 * @param fraction Sample fraction, aka the Bernoulli sampler possibility.
	 * @param seed     Random number generator seed.
	 */
	public BernoulliSampler(double fraction, long seed) {
		this(fraction, new XORShiftRandom(seed));
	}

	/**
	 * Create a Bernoulli sampler with sample fraction and random number generator.
	 *
	 * @param fraction Sample fraction, aka the Bernoulli sampler possibility.
	 * @param random   The random number generator.
	 */
	public BernoulliSampler(double fraction, Random random) {
		Preconditions.checkArgument(fraction >= 0 && fraction <= 1.0d, "fraction fraction must between [0, 1].");
		this.fraction = fraction;
		this.random = random;
	}

	/**
	 * Sample the input elements, for each input element, take a Bernoulli trail for sampling.
	 *
	 * @param input Elements to be sampled.
	 * @return The sampled result which is lazy computed upon input elements.
	 */
	@Override
	public Iterator<T> sample(final Iterator<T> input) {
		if (fraction == 0) {
			return emptyIterable;
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
				if (fraction <= THRESHOLD) {
					double rand = random.nextDouble();
					double u = Math.max(rand, EPSILON);
					int gap = (int) (Math.log(u) / Math.log(1 - fraction));
					int elementCount = 0;
					if (input.hasNext()) {
						T element = input.next();
						while (input.hasNext() && elementCount < gap) {
							element = input.next();
							elementCount++;
						}
						if (elementCount < gap) {
							return null;
						} else {
							return element;
						}
					} else {
						return null;
					}
				} else {
					while (input.hasNext()) {
						T element = input.next();

						if (random.nextDouble() <= fraction) {
							return element;
						}
					}
					return null;
				}
			}
		};
	}
}
