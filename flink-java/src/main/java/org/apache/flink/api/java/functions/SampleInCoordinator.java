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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.sampling.DistributedRandomSampler;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithReplacement;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithoutReplacement;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * SampleInCoordinator wraps the sample logic of the coordinator side (the second phase of
 * distributed sample algorithm). It executes the coordinator side sample logic in an all reduce
 * function. The user needs to make sure that the operator parallelism of this function is 1 to make
 * sure this is a central coordinator. Besides, we do not need the task index information for random
 * generator seed as the parallelism must be 1.
 *
 * @param <T> the data type wrapped in ElementWithRandom as input.
 */
@Internal
public class SampleInCoordinator<T> implements GroupReduceFunction<IntermediateSampleData<T>, T> {

    private boolean withReplacement;
    private int numSample;
    private long seed;

    /**
     * Create a function instance of SampleInCoordinator.
     *
     * @param withReplacement Whether element can be selected more than once.
     * @param numSample Fixed sample size.
     * @param seed Random generator seed.
     */
    public SampleInCoordinator(boolean withReplacement, int numSample, long seed) {
        this.withReplacement = withReplacement;
        this.numSample = numSample;
        this.seed = seed;
    }

    @Override
    public void reduce(Iterable<IntermediateSampleData<T>> values, Collector<T> out)
            throws Exception {
        DistributedRandomSampler<T> sampler;
        if (withReplacement) {
            sampler = new ReservoirSamplerWithReplacement<>(numSample, seed);
        } else {
            sampler = new ReservoirSamplerWithoutReplacement<>(numSample, seed);
        }

        Iterator<T> sampled = sampler.sampleInCoordinator(values.iterator());
        while (sampled.hasNext()) {
            out.collect(sampled.next());
        }
    }
}
