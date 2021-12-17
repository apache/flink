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
 * A simple in memory implementation of Reservoir Sampling with replacement and with only one pass
 * through the input iteration whose size is unpredictable. The basic idea behind this sampler
 * implementation is quite similar to {@link ReservoirSamplerWithoutReplacement}. The main
 * difference is that, in the first phase, we generate weights for each element K times, so that
 * each element can get selected multiple times.
 *
 * <p>This implementation refers to the algorithm described in <a
 * href="researcher.ibm.com/files/us-dpwoodru/tw11.pdf">"Optimal Random Sampling from Distributed
 * Streams Revisited"</a>.
 *
 * @param <T> The type of sample.
 */
@Internal
public class ReservoirSamplerWithReplacement<T> extends DistributedRandomSampler<T> {

    private final Random random;

    /**
     * Create a sampler with fixed sample size and default random number generator.
     *
     * @param numSamples Number of selected elements, must be non-negative.
     */
    public ReservoirSamplerWithReplacement(int numSamples) {
        this(numSamples, new XORShiftRandom());
    }

    /**
     * Create a sampler with fixed sample size and random number generator seed.
     *
     * @param numSamples Number of selected elements, must be non-negative.
     * @param seed Random number generator seed
     */
    public ReservoirSamplerWithReplacement(int numSamples, long seed) {
        this(numSamples, new XORShiftRandom(seed));
    }

    /**
     * Create a sampler with fixed sample size and random number generator.
     *
     * @param numSamples Number of selected elements, must be non-negative.
     * @param random Random number generator
     */
    public ReservoirSamplerWithReplacement(int numSamples, Random random) {
        super(numSamples);
        Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
        this.random = random;
    }

    @Override
    public Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input) {
        if (numSamples == 0) {
            return emptyIntermediateIterable;
        }

        // This queue holds a fixed number of elements with the top K weight for current partition.
        PriorityQueue<IntermediateSampleData<T>> queue =
                new PriorityQueue<IntermediateSampleData<T>>(numSamples);

        IntermediateSampleData<T> smallest = null;

        if (input.hasNext()) {
            T element = input.next();
            // Initiate the queue with the first element and random weights.
            for (int i = 0; i < numSamples; i++) {
                queue.add(new IntermediateSampleData<T>(random.nextDouble(), element));
                smallest = queue.peek();
            }
        }

        while (input.hasNext()) {
            T element = input.next();
            // To sample with replacement, we generate K random weights for each element, so that
            // it's
            // possible to be selected multi times.
            for (int i = 0; i < numSamples; i++) {
                // If current element weight is larger than the smallest one in queue, remove the
                // element
                // with the smallest weight, and append current element into the queue.
                double rand = random.nextDouble();
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
