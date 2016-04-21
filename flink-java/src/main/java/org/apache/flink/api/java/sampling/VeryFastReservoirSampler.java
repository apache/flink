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
 * A in memory implementation of Very Fast Reservoir Sampling. The algorithm works well then the size of streaming data is much larger than size of reservoir.
 * The algorithm runs in random sampling with P(R/j) where in R is the size of sampling and j is the current index of streaming data.
 * The algorithm consists of two part:
 * 	(1) Before the size of streaming data reaches threshold, it uses regular reservoir sampling
 * 	(2) After the size of streaming data reaches threshold, it uses geometric distribution to generate the approximation gap
 * 		to skip data, and size of gap is determined by  geometric distribution with probability p = R/j
 *
 *  Thanks to Erik Erlandson who is the author of this algorithm and help me with implementation.
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
		int index = 0, k=0, gap = 0;
		int totalgap = 0; // for test
		while (input.hasNext()) {
			T element = input.next();
			if (index < THRESHOLD) {  // if index is less than THRESHOLD, then use regular reservoir
				if (index < numSamples) {
					// Fill the queue with first K elements from input.
					queue.add(new IntermediateSampleData<T>(random.nextDouble(), element));
					smallest = queue.peek();
				} else {
					double rand = random.nextDouble();
					// Remove the element with the smallest weight, and append current element into the queue.
					if (rand > smallest.getWeight()) {
						queue.remove();
						queue.add(new IntermediateSampleData<T>(rand, element));
						smallest = queue.peek();
					}
				}
				index++;
			} else {          // fast section
				double rand = random.nextDouble();
				probability = (double) numSamples / index;
				gap = (int) (Math.log(rand) / Math.log(1 - probability));
				totalgap+=gap;  //for test
				int elementCount = 0;
				while (input.hasNext() && elementCount < gap) {
					elementCount++;
					element = input.next();
					index++;
				}
				if (elementCount <gap)
					continue;
				else {
					queue.remove();
					queue.add(new IntermediateSampleData<T>(random.nextDouble(), element));
					index++;
				}
			}
		}
		return queue.iterator();
	}
}
