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
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A simple in memory implementation of Reservoir Sampling without replacement, and with only one pass through
 * the input iteration whose size is unpredictable.
 * The basic idea behind this sampler implementation is that: generate a random number for each input elements
 * as its weight, select top K elements with max weight, as the weights are generated randomly, so the selected
 * top K elements are selected randomly. For implementation, we implemented the two-phases interface of DistributedRandomSampler,
 * in first phase, generate random number as the weight for each elements, and select top K elements as the output
 * of each partitions. In second phase, select top K elements from all the outputs in first phase.
 * This implementation refers to the algorithm described in <a href="researcher.ibm.com/files/us-dpwoodru/tw11.pdf">
 * "Optimal Random Sampling from Distributed Streams Revisited"</a>.
 *
 * @param <T> The type of the sampler.
 */
public class ReservoirSamplerWithoutReplacement<T> extends DistributedRandomSampler<T> {
	
	private final int numSamples;
	private final Random random;

	/**
	 * Create a new sampler with reservoir size and a supplied random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param random     Instance of random number generator for sampling.
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
	 * @param seed       Random number generator seed.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples, long seed) {
		
		this(numSamples, new Random(seed));
	}
	
	@Override
	public Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input) {
		if (numSamples == 0) {
			return EMPTY_INTERMEDIATE_ITERABLE;
		}

		// This queue holds fixed number elements with the top K weight for current partition.
		PriorityQueue<IntermediateSampleData<T>> queue = new PriorityQueue<IntermediateSampleData<T>>(numSamples);
		int index = 0;
		IntermediateSampleData<T> smallest = null;
		while (input.hasNext()) {
			T element = input.next();
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
		}
		return queue.iterator();
	}

	@Override
	public Iterator<T> sampleInCoordinator(Iterator<IntermediateSampleData<T>> input) {
		if (numSamples == 0) {
			return EMPTY_ITERABLE;
		}

		// This queue holds fixed number elements with the top K weight for the coordinator.
		PriorityQueue<IntermediateSampleData<T>> queue = new PriorityQueue<IntermediateSampleData<T>>(numSamples);
		int index = 0;
		IntermediateSampleData<T> smallest = null;
		while (input.hasNext()) {
			IntermediateSampleData<T> element = input.next();
			if (index < numSamples) {
				// Fill the queue with first K elements from input.
				queue.add(element);
				smallest = queue.peek();
			} else {
				// If current element weight is larger than the smallest one in queue, remove the element
				// with the smallest weight, and append current element into the queue.
				if (element.getWeight() > smallest.getWeight()) {
					queue.remove();
					queue.add(element);
					smallest = queue.peek();
				}
			}
			index++;
		}

		final Iterator<IntermediateSampleData<T>> itr = queue.iterator();

		return new Iterator<T>() {
			@Override
			public boolean hasNext() {
				return itr.hasNext();
			}

			@Override
			public T next() {
				return itr.next().getElement();
			}

			@Override
			public void remove() {
				itr.remove();
			}
		};
	}
}
