package eu.stratosphere.pact.testing;

/**
 * Distance function between two values. 
 * 
 * @author Arvid Heise
 *
 * @param <V> the value type
 */
public interface FuzzyTestValueSimilarity<V> {
	/**
	 * Constant used to indicate that two values do not match.
	 */
	public static double NO_MATCH = -1;

	/**
	 * Returns the distance between the first and the second value or {@link #NO_MATCH}.
	 * 
	 * @param value1 the first value
	 * @param value2 the second value
	 * @return a positive value corresponding to the distance or {@link #NO_MATCH}
	 */
	public double getDistance(V value1, V value2);
}