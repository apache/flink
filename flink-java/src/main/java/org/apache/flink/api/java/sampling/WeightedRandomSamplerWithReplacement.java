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
 * A simple in memory implementation of Weighted Reservoir Sampling with replacement, and with only one
 * pass through the input iteration whose size is unpredictable. The items are weighted and the probability
 * of each item to be selected is determined by its relative weight. The basic idea is similar to 
 * {@link ReservoirSamplerWithoutReplacement} interface. The main difference is that we generate weights
 * for each element K times, so that each element can get selected multiple times.
 *
 * This implementation refers to the algorithm described in <a href="arxiv.org/pdf/1012.0256.pdf">
 * "Weighted Random Sampling over Data Streams"</a>.
 *
 * @param <T> The type of the sampler.
 */
@Internal
public class WeightedRandomSamplerWithReplacement<T> extends DistributedRandomSampler<T> {

	private final Random random;

	/**
	 * Create a new sampler with reservoir size and a supplied random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param random     Instance of random number generator for sampling.
	 */
	public WeightedRandomSamplerWithReplacement(int numSamples, Random random) {
		super(numSamples);
		Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
		this.random = random;
	}

	/**
	 * Create a new sampler with reservoir size and a default random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 */
	public WeightedRandomSamplerWithReplacement(int numSamples) {
		this(numSamples, new XORShiftRandom());
	}

	/**
	 * Create a new sampler with reservoir size and the seed for random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param seed       Random number generator seed.
	 */
	public WeightedRandomSamplerWithReplacement(int numSamples, long seed) {

		this(numSamples, new XORShiftRandom(seed));
	}

	@Override
	public Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input) {
		if (numSamples == 0) {
			return EMPTY_INTERMEDIATE_ITERABLE;
		}

		// This queue holds a fixed number of elements with the top K weight for current partition.
		PriorityQueue<IntermediateSampleData<T>> queue = new PriorityQueue<IntermediateSampleData<T>>(numSamples);

		IntermediateSampleData<T> smallest = null;

		if (input.hasNext()) {
			T element = input.next();
			// Initiate the queue with the first element and defined weights.
			for (int i = 0; i < numSamples; i++) {
				double weighted = ((WeightedData)element).getWeighted();
				queue.add(new IntermediateSampleData<T>(Math.pow(random.nextDouble(), (1 / weighted)), element));
				smallest = queue.peek();
			}
		}

		while (input.hasNext()) {
			T element = input.next();
			// To sample with replacement, we generate K defined weights for each element, so that it's
			// possible to be selected multi times.
			for (int i = 0; i < numSamples; i++) {
				// If current element defined weight is larger than the smallest one in queue, remove the element
				// with the smallest weight, and append current element into the queue.
				double weighted = ((WeightedData)element).getWeighted();
				double rand = Math.pow(random.nextDouble(), (1 / weighted));
				if (rand > smallest.getWeight()) {
					queue.remove();
					queue.add(new IntermediateSampleData<T>(rand, element));
					smallest = queue.peek();
				}
			}
		}
		return queue.iterator();
	}
}
