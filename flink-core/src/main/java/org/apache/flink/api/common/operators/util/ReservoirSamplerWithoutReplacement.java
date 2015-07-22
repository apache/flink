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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * A simple in memory implementation of Reservoir Sampling without replacement, and with only one pass through
 * the input iteration whose size is unpredictable.
 * This implementation refers to the Algorithm R described in "Random Sampling with a Reservoir" Vitter, 1985.
 *
 * @param <T> type of the sampler.
 */
public class ReservoirSamplerWithoutReplacement<T> extends RandomSampler<T> {
	
	private final int numSamples;
	private final Random random;
	
	/**
	 * Create a new sampler with reservoir size and a supplied random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param random     instance of random number generator for sampling.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples, Random random) {
		Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
		this.numSamples = numSamples;
		this.random = random;
	}
	
	/**
	 * Create a new sampler with reservoir size and a default random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples) {
		this(numSamples, new Random());
	}
	
	/**
	 * Create a new sampler with reservoir size and the seed for random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param seed       random number generator seed.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples, long seed) {
		
		this(numSamples, new Random(seed));
	}
	
	/**
	 * Sample the input elements, and return the sample result.
	 *
	 * @param input the elements which tend to be sampled.
	 * @return return the reservoir, the reservoir size would less than numSamples while input size is
	 * less than numSamples.
	 */
	@Override
	public Iterator<T> sample(Iterator<T> input) {
		if (numSamples == 0) {
			return EMPTY_ITERABLE;
		}
		
		List<T> reservoir = new ArrayList<T>();
		int index = 0;
		while (input.hasNext()) {
			T element = input.next();
			if (index < numSamples) {
				reservoir.add(element);
			} else {
				int rIndex = random.nextInt(index + 1);
				if (rIndex < numSamples) {
					reservoir.set(rIndex, element);
				}
			}
			index++;
		}
		return reservoir.iterator();
	}
}
