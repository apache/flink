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
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;


/**
 * A simple stratified sampler implementation. Each stratum is sampled by Bernoulli Sampling without replacement
 * and Poisson sampling with replacement.
 *
 * @param <T> The type of sample.
 * @see <a href="http://ebrain.io/stratified-sampling/">Stratified Sampling with Map Reduce</a>
 */
@Internal
public class SimpleStratifiedSampler<T> implements GroupReduceFunction<T, T>, GroupCombineFunction<T, T> {
	private static final long serialVersionUID = 1L;
	private final boolean withReplacement;
	private final double fraction;
	private final long seed;

	public SimpleStratifiedSampler(boolean withReplacement, double fraction, final long seed) {
		Preconditions.checkArgument(fraction >= 0 && fraction <= 1.0d, "fraction fraction must between [0, 1].");
		this.withReplacement = withReplacement;
		this.fraction = fraction;
		this.seed = seed;
	}

	@Override
	public void reduce(Iterable<T> values, Collector<T> out) throws Exception {
		RandomSampler<T> sampler;

		if (withReplacement) {
			sampler = new PoissonSampler<T>(fraction, seed);
		} else {
			sampler = new BernoulliSampler<T>(fraction, seed);
		}

		//Sample each stratum
		Iterator<T> sampled = sampler.sample(values.iterator());
		while (sampled.hasNext()) {
			out.collect(sampled.next());
		}
	}

	@Override
	public void combine(Iterable<T> values, Collector<T> out) throws Exception {
		reduce(values, out);
	}

}
