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
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A simple in memory implementation of Reservoir Sampling without replacement, and with only one
 * pass through the input iteration whose size is unpredictable. The basic idea behind this sampler
 * implementation is to generate a random number for each input element as its weight, select the
 * top K elements with max weight. As the weights are generated randomly, so are the selected top K
 * elements. The algorithm is implemented using the {@link DistributedRandomSampler} interface. In
 * the first phase, we generate random numbers as the weights for each element and select top K
 * elements as the output of each partitions. In the second phase, we select top K elements from all
 * the outputs of the first phase.
 *
 * <p>This implementation refers to the algorithm described in <a
 * href="researcher.ibm.com/files/us-dpwoodru/tw11.pdf">"Optimal Random Sampling from Distributed
 * Streams Revisited"</a>.
 *
 * @param <T> The type of the sampler.
 */
@Internal
public class ReservoirSamplerWithoutReplacement<T> extends DistributedRandomSampler<T> {

    private final Random random;

    /**
     * Create a new sampler with reservoir size and a supplied random number generator.
     *
     * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
     * @param random Instance of random number generator for sampling.
     */
    public ReservoirSamplerWithoutReplacement(int numSamples, Random random) {
        super(numSamples);
        Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
        this.random = random;
    }

    /**
     * Create a new sampler with reservoir size and a default random number generator.
     *
     * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
     */
    public ReservoirSamplerWithoutReplacement(int numSamples) {
        this(numSamples, new XORShiftRandom());
    }

    /**
     * Create a new sampler with reservoir size and the seed for random number generator.
     *
     * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
     * @param seed Random number generator seed.
     */
    public ReservoirSamplerWithoutReplacement(int numSamples, long seed) {

        this(numSamples, new XORShiftRandom(seed));
    }

    @Override
    public Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input) {
        if (numSamples == 0) {
            return emptyIntermediateIterable;
        }

        // This queue holds fixed number elements with the top K weight for current partition.
        PriorityQueue<IntermediateSampleData<T>> queue =
                new PriorityQueue<IntermediateSampleData<T>>(numSamples);
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
                // Remove the element with the smallest weight, and append current element into the
                // queue.
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
}
