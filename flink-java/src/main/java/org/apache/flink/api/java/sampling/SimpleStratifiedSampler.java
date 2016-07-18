package org.apache.flink.api.java.sampling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;


/**
 * A simple stratified sampler implementation. Each stratum is sampled with Reservoir Sampling.
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
		int numSample;
		Iterator<T> stratum = values.iterator();

		//Read each item of each stratum to a local cache
		Collection<T> cacheStratum = new ArrayList<>();
		while (stratum.hasNext()) {
			cacheStratum.add(stratum.next());
		}

		numSample = (int) (fraction * cacheStratum.size());
		if (withReplacement) {
			sampler = new ReservoirSamplerWithReplacement<T>(numSample, seed);
		} else {
			sampler = new ReservoirSamplerWithoutReplacement<T>(numSample, seed);
		}

		//Sample each stratum
		Iterator<T> sampled = sampler.sample(cacheStratum.iterator());
		while (sampled.hasNext()) {
			out.collect(sampled.next());
		}
	}

	@Override
	public void combine(Iterable<T> values, Collector<T> out) throws Exception {
		reduce(values, out);
	}

}
