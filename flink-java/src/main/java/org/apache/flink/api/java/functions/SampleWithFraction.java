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
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.PoissonSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * A map partition function wrapper for sampling algorithms with fraction, the sample algorithm
 * takes the partition iterator as input.
 *
 * @param <T>
 */
@Internal
public class SampleWithFraction<T> extends RichMapPartitionFunction<T, T> {

	private boolean withReplacement;
	private double fraction;
	private long seed;

	/**
	 * Create a function instance of SampleWithFraction.
	 *
	 * @param withReplacement Whether element can be selected more than once.
	 * @param fraction        Probability that each element is selected.
	 * @param seed            random number generator seed.
	 */
	public SampleWithFraction(boolean withReplacement, double fraction, long seed) {
		this.withReplacement = withReplacement;
		this.fraction = fraction;
		this.seed = seed;
	}

	@Override
	public void mapPartition(Iterable<T> values, Collector<T> out) throws Exception {
		RandomSampler<T> sampler;
		long seedAndIndex = seed + getRuntimeContext().getIndexOfThisSubtask();
		if (withReplacement) {
			sampler = new PoissonSampler<>(fraction, seedAndIndex);
		} else {
			sampler = new BernoulliSampler<>(fraction, seedAndIndex);
		}

		Iterator<T> sampled = sampler.sample(values.iterator());
		while (sampled.hasNext()) {
			out.collect(sampled.next());
		}
	}
}
