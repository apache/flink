package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * Computes the difference between double values and returns {@link FuzzyTestValueSimilarity#NO_MATCH} if the difference
 * is
 * above a given threshold.
 * 
 * @author Arvid Heise
 */
public class DoubleValueSimilarity implements FuzzyTestValueSimilarity {
	private double delta;

	private int valueIndex;

	/**
	 * Initializes DoubleValueSimilarity with the given threshold.
	 * 
	 * @param delta
	 *        the threshold defining the maximum allowed difference.
	 */
	public DoubleValueSimilarity(double delta, int valueIndex) {
		this.delta = delta;
		this.valueIndex = valueIndex;
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
	public double getDistance(PactRecord record1, PactRecord record2) {
		double value1 = record1.getField(this.valueIndex, PactDouble.class).getValue();
		double value2 = record2.getField(this.valueIndex, PactDouble.class).getValue();
		if (Double.compare(value1, value2) == 0)
			return 0;
		double diff = Math.abs(value1 - value2);
		if (diff <= this.delta)
			return diff;
		return NO_MATCH;
	}
}