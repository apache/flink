package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Distance function between two values.
 * 
 * @author Arvid Heise
 * @param <V>
 *        the value type
 */
public interface FuzzyTestValueSimilarity {
	/**
	 * Constant used to indicate that two values do not match.
	 */
	public static double NO_MATCH = -1;

	/**
	 * Returns the distance between the first and the second value or {@link #NO_MATCH}.
	 * 
	 * @param value1
	 *        the first value
	 * @param value2
	 *        the second value
	 * @return a positive value corresponding to the distance or {@link #NO_MATCH}
	 */
	public double getDistance(PactRecord value1, PactRecord value2);
}