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
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.sampling.DistributedRandomSampler;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithReplacement;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithoutReplacement;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * SampleInPartition wraps the sample logic on the partition side (the first phase of distributed
 * sample algorithm). It executes the partition side sample logic in a mapPartition function.
 *
 * @param <T> The type of input data
 */
@Internal
public class SampleInPartition<T> extends RichMapPartitionFunction<T, IntermediateSampleData<T>> {

	private boolean withReplacement;
	private int numSample;
	private long seed;

	/**
	 * Create a function instance of SampleInPartition.
	 *
	 * @param withReplacement Whether element can be selected more than once.
	 * @param numSample       Fixed sample size.
	 * @param seed            Random generator seed.
	 */
	public SampleInPartition(boolean withReplacement, int numSample, long seed) {
		this.withReplacement = withReplacement;
		this.numSample = numSample;
		this.seed = seed;
	}

	@Override
	public void mapPartition(Iterable<T> values, Collector<IntermediateSampleData<T>> out) throws Exception {
		DistributedRandomSampler<T> sampler;
		long seedAndIndex = seed + getRuntimeContext().getIndexOfThisSubtask();
		if (withReplacement) {
			sampler = new ReservoirSamplerWithReplacement<T>(numSample, seedAndIndex);
		} else {
			sampler = new ReservoirSamplerWithoutReplacement<T>(numSample, seedAndIndex);
		}

		Iterator<IntermediateSampleData<T>> sampled = sampler.sampleInPartition(values.iterator());
		while (sampled.hasNext()) {
			out.collect(sampled.next());
		}
	}
}

