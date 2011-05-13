package eu.stratosphere.pact.testing;

import java.util.Collection;

/**
 * Global match algorithm that uses a {@link FuzzyTestValueSimilarity} to match a bag of expected values with a bag of
 * actual values.
 * 
 * @author Arvid Heise
 * @param <V>
 *        the value type
 */
public interface FuzzyTestValueMatcher<V> {
	/**
	 * Removes all pairs of matching items from the two collections. The cardinality of both collections is guaranteed
	 * to be the same on input and does not have to be equal on output. The given {@link FuzzyTestValueSimilarity}
	 * defines the similarity measure between two values.<br>
	 * Since both collections have the bag semantic, some values may appear multiple times. The algorithm should remove
	 * only one value per matching pair unless otherwise documented.
	 * 
	 * @param similarity
	 *        the similarity measure
	 * @param expectedValues
	 *        a bag of expected values
	 * @param actualValues
	 *        a bag of actual values
	 */
	public void removeMatchingValues(FuzzyTestValueSimilarity<V> similarity, Collection<V> expectedValues,
			Collection<V> actualValues);
}