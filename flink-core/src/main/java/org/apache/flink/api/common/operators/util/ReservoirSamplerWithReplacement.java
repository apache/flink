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
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * A simple in memory implementation of Reservoir Sampling with replacement, and with only one pass through
 * the input iteration whose size is unpredictable.
 * This implementation refers to the algorithm described in "Reservoir-based Random Sampling with Replacement
 * from Data Stream".
 *
 * @param <T> the type of sample.
 */
public class ReservoirSamplerWithReplacement<T> extends RandomSampler<T> {
	private final int numSamples;
	private final Random random;
	private PoissonDistribution poissonDistribution;
	private List<Integer> positions;
	
	/**
	 * Create a reservoir sampler with fixed sample size and default random number generator.
	 *
	 * @param numSamples number of samples to retain in reservoir, must be non-negative.
	 */
	public ReservoirSamplerWithReplacement(int numSamples) {
		this(numSamples, new Random());
	}
	
	/**
	 * Create a reservoir sampler with fixed sample size and random number generator seed.
	 *
	 * @param numSamples number of samples to retain in reservoir, must be non-negative.
	 * @param seed       random number generator seed
	 */
	public ReservoirSamplerWithReplacement(int numSamples, long seed) {
		this(numSamples, new Random(seed));
	}
	
	/**
	 * Create a reservoir sampler with fixed sample size and random number generator.
	 *
	 * @param numSamples number of samples to retain in reservoir, must be non-negative.
	 */
	public ReservoirSamplerWithReplacement(int numSamples, Random random) {
		Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
		this.numSamples = numSamples;
		this.random = random;
		this.positions = new LinkedList<Integer>();
		this.initPositionList();
	}
	
	/**
	 * Sample the input elements, and return the sample result.
	 *
	 * @param input the elements which tend to be sampled.
	 * @return return the reservoir.
	 */
	@Override
	public Iterator<T> sample(Iterator<T> input) {
		if (numSamples == 0) {
			return EMPTY_ITERABLE;
		}
		
		List<T> reservoir = new ArrayList<T>(numSamples);
		int rIndex = 0;
		while (input.hasNext()) {
			T element = input.next();
			if (rIndex == 0) {
				// fill the reservoir with first element.
				for (int i = 0; i < numSamples; i++) {
					reservoir.add(element);
				}
			} else {
				double expectedCount = numSamples / ((double)rIndex + 1);
				// create poisson distribution with expect count of current element as mean value, and generate sample value.
				int seed = random.nextInt();
				poissonDistribution = new PoissonDistribution(expectedCount);
				poissonDistribution.reseedRandomGenerator(seed);
				int sampledCount = poissonDistribution.sample();
				if (sampledCount > 0) {
					// select elements in reservoir and update them with current element.
					ReservoirSamplerWithoutReplacement sampler = new ReservoirSamplerWithoutReplacement(sampledCount, seed);
					Iterator<Integer> evictedPositions = sampler.sample(positions.iterator());
					while (evictedPositions.hasNext()) {
						reservoir.set(evictedPositions.next(), element);
					}
				}
			}
			rIndex++;
		}
		return reservoir.iterator();
	}
	
	private void initPositionList() {
		for (int i = 0; i < numSamples; i++) {
			this.positions.add(i);
		}
	}
}
