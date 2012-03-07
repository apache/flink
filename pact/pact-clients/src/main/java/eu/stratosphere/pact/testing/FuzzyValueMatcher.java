package eu.stratosphere.pact.testing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Collection;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Global match algorithm that uses a {@link FuzzyTestValueSimilarity} to match a bag of expected values with a bag of
 * actual values.
 * 
 * @author Arvid Heise
 * @param <V>
 *        the value type
 */
public interface FuzzyValueMatcher {

	/**
	 * Removes all pairs of matching items from the two collections. The cardinality of both collections is guaranteed
	 * to be the same on input and does not have to be equal on output. The given {@link FuzzyTestValueSimilarity}
	 * defines the similarity measure between two values.<br>
	 * Since both collections have the bag semantic, some values may appear multiple times. The algorithm should remove
	 * only one value per matching pair unless otherwise documented.
	 * 
	 * @param similarities
	 *        the similarity measures
	 * @param expectedValues
	 *        a bag of expected values
	 * @param actualValues
	 *        a bag of actual values
	 */
	public void removeMatchingValues(Int2ObjectMap<List<ValueSimilarity<?>>> similarities,
			Class<? extends Value>[] schema, Collection<PactRecord> expectedValues, Collection<PactRecord> actualValues);
}