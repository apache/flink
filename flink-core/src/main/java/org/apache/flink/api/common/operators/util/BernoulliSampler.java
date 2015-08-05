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

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.Random;

/**
 * A sampler implementation built upon Bernoulli Trail. For sample without replacement, each element sample choice is just a bernoulli trail.
 *
 * @param <T> The type of sample.
 */
public class BernoulliSampler<T> extends RandomSampler<T> {
	
	private final double fraction;
	private final Random random;
	
	/**
	 * Create a bernoulli sampler sample fraction and default random number generator.
	 *
	 * @param fraction sample fraction, aka the bernoulli sampler possibility.
	 */
	public BernoulliSampler(double fraction) {
		this(fraction, new Random());
	}
	
	/**
	 * Create a bernoulli sampler sample fraction and random number generator seed.
	 *
	 * @param fraction sample fraction, aka the bernoulli sampler possibility.
	 * @param seed     random number generator seed.
	 */
	public BernoulliSampler(double fraction, long seed) {
		this(fraction, new Random(seed));
	}
	
	/**
	 * Create a bernoulli sampler sample fraction and random number generator.
	 *
	 * @param fraction sample fraction, aka the bernoulli sampler possibility.
	 * @param random   the random number generator.
	 */
	public BernoulliSampler(double fraction, Random random) {
		Preconditions.checkArgument(fraction >= 0 && fraction <= 1.0d, "fraction fraction must between [0, 1].");
		this.fraction = fraction;
		this.random = random;
	}
	
	/**
	 * Sample the input elements, for each input element, take a Bernoulli Trail for sample.
	 *
	 * @param input elements to be sampled.
	 * @return the sampled result which is lazy computed upon input elements.
	 */
	@Override
	public Iterator<T> sample(final Iterator<T> input) {
		if (fraction == 0) {
			return EMPTY_ITERABLE;
		}
		
		return new SampledIterator<T>() {
			T current;
			
			@Override
			public boolean hasNext() {
				if (current == null) {
					while (input.hasNext()) {
						T element = input.next();
						if (random.nextDouble() <= fraction) {
							current = element;
							return true;
						}
					}
					current = null;
					return false;
				} else {
					return true;
				}
			}
			
			@Override
			public T next() {
				T result = current;
				current = null;
				return result;
			}
		};
	}
}
