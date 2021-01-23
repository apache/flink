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

import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * For sampling with fraction, the sample algorithms are natively distributed, while it's not true
 * for fixed size sample algorithms. The fixed size sample algorithms require two-phases sampling
 * (according to our current implementation). In the first phase, each distributed partition is
 * sampled independently. The partial sampling results are handled by a central coordinator. The
 * central coordinator combines the partial sampling results to form the final result.
 *
 * @param <T> The input data type.
 */
@Internal
public abstract class DistributedRandomSampler<T> extends RandomSampler<T> {

    protected final int numSamples;

    public DistributedRandomSampler(int numSamples) {
        this.numSamples = numSamples;
    }

    protected final Iterator<IntermediateSampleData<T>> emptyIntermediateIterable =
            new SampledIterator<IntermediateSampleData<T>>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public IntermediateSampleData<T> next() {
                    return null;
                }
            };

    /**
     * Sample algorithm for the first phase. It operates on a single partition.
     *
     * @param input The DataSet input of each partition.
     * @return Intermediate sample output which will be used as the input of the second phase.
     */
    public abstract Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input);

    /**
     * Sample algorithm for the second phase. This operation should be executed as the UDF of an all
     * reduce operation.
     *
     * @param input The intermediate sample output generated in the first phase.
     * @return The sampled output.
     */
    public Iterator<T> sampleInCoordinator(Iterator<IntermediateSampleData<T>> input) {
        if (numSamples == 0) {
            return emptyIterable;
        }

        // This queue holds fixed number elements with the top K weight for the coordinator.
        PriorityQueue<IntermediateSampleData<T>> reservoir =
                new PriorityQueue<IntermediateSampleData<T>>(numSamples);
        int index = 0;
        IntermediateSampleData<T> smallest = null;
        while (input.hasNext()) {
            IntermediateSampleData<T> element = input.next();
            if (index < numSamples) {
                // Fill the queue with first K elements from input.
                reservoir.add(element);
                smallest = reservoir.peek();
            } else {
                // If current element weight is larger than the smallest one in queue, remove the
                // element
                // with the smallest weight, and append current element into the queue.
                if (element.getWeight() > smallest.getWeight()) {
                    reservoir.remove();
                    reservoir.add(element);
                    smallest = reservoir.peek();
                }
            }
            index++;
        }
        final Iterator<IntermediateSampleData<T>> itr = reservoir.iterator();

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

    /**
     * Combine the first phase and second phase in sequence, implemented for test purpose only.
     *
     * @param input Source data.
     * @return Sample result in sequence.
     */
    @Override
    public Iterator<T> sample(Iterator<T> input) {
        return sampleInCoordinator(sampleInPartition(input));
    }
}
