package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * Computes the difference between double values and returns {@link FuzzyTestValueSimilarity#NO_MATCH} if the difference
 * is
 * above a given threshold.
 * 
 * @author Arvid Heise
 */
public class DoubleValueSimilarity extends AbstractValueSimilarity<PactDouble> {
	private double delta;

	/**
	 * Initializes DoubleValueSimilarity with the given threshold.
	 * 
	 * @param delta
	 *        the threshold defining the maximum allowed difference.
	 */
	public DoubleValueSimilarity(double delta) {
		this.delta = delta;
	}

	/**
	 * Returns the threshold.
	 * 
	 * @return the threshold
	 */
	public double getDelta() {
		return this.delta;
	}

	@Override
	public double getDistance(PactDouble value1, PactDouble value2) {
		double diff = Math.abs(value1.getValue() - value2.getValue());
		if (diff <= this.delta)
			return diff;
		return NO_MATCH;
	}
}