package eu.stratosphere.nephele.jobmanager.accumulators;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.accumulators.Accumulator;
import eu.stratosphere.api.accumulators.AccumulatorHelper;

/**
 * Simple class wrapping a map of accumulators for a single job. Just for better
 * handling.
 */
public class JobAccumulators {

	private final Map<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();

	public Map<String, Accumulator<?, ?>> getAccumulators() {
		return this.accumulators;
	}

	public void processNew(Map<String, Accumulator<?, ?>> newAccumulators) {
		AccumulatorHelper.mergeInto(this.accumulators, newAccumulators);
	}
}