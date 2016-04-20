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
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.XORShiftRandom;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A sampler implementation built upon a Bernoulli trail. This sampler is used to sample with
 * fraction and without replacement. Whether an element is sampled or not is determined by a
 * Bernoulli experiment.
 *
 * @param <T> The type of sample.
 * @see <a href="http://erikerlandson.github.io/blog/2015/11/20/very-fast-reservoir-sampling/">Very Fast Reservoir Sampling</a>
 */
@Internal
public class VeryFastReservoirSampler<T> extends DistributedRandomSampler<T> {

	private final Random random;
	// THRESHOLD is a tuning parameter for choosing sampling method according to the fraction.
	private final int THRESHOLD = 4 * super.numSamples;

	/**
	 * Create a new sampler with reservoir size and a supplied random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param random     Instance of random number generator for sampling.
	 */
	public VeryFastReservoirSampler(int numSamples, Random random) {
		super(numSamples);
		Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
		this.random = random;
	}

	/**
	 * Create a new sampler with reservoir size and a default random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 */
	public VeryFastReservoirSampler(int numSamples) {
		this(numSamples, new XORShiftRandom());
	}

	/**
	 * Create a new sampler with reservoir size and the seed for random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param seed       Random number generator seed.
	 */
	public VeryFastReservoirSampler(int numSamples, long seed) {

		this(numSamples, new XORShiftRandom(seed));
	}

	@Override
	public Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input) {
		if (numSamples == 0) {
			return EMPTY_INTERMEDIATE_ITERABLE;
		}
		PriorityQueue<IntermediateSampleData<T>> queue = new PriorityQueue<IntermediateSampleData<T>>(numSamples);
		double probability;
		IntermediateSampleData<T> smallest = null;
		int index = 0, k, gap = 0;
		while (input.hasNext()) {
			T element = input.next();
			double rand = random.nextDouble();
			if (index < THRESHOLD) {  // if index is less than THRESHOLD, then use regular reservoir
				if (index < numSamples) {
					// Fill the queue with first K elements from input.
					queue.add(new IntermediateSampleData<T>(random.nextDouble(), element));
					smallest = queue.peek();
				} else {
					// Remove the element with the smallest weight, and append current element into the queue.
					if (rand > smallest.getWeight()) {
						queue.remove();
						queue.add(new IntermediateSampleData<T>(rand, element));
						smallest = queue.peek();
					}
				}
				index++;
			} else {          // fast section
				probability = (double) numSamples / index;
				double rand1 = random.nextDouble();
				double u = Math.max(rand1, EPSILON);
				gap =  (int) (Math.log(u) / Math.log(1 - probability));
				if (gap > 0) {
					while (input.hasNext() && gap > 0) {
						gap--;
						element = input.next();
						index++;
					}
				}
				queue.remove();
				queue.add(new IntermediateSampleData<T>(rand, element));
				smallest = queue.peek();
			}
		}
		return queue.iterator();
	}
}
