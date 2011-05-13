package eu.stratosphere.pact.testing;

import java.util.Collection;
import java.util.Iterator;

/**
 * Matches all exact matching pairs using equals.
 * 
 * @author Arvid Heise
 * @param <V>
 */
public class EqualityValueMatcher<V> implements FuzzyTestValueMatcher<V> {

	@Override
	public void removeMatchingValues(FuzzyTestValueSimilarity<V> similarity, Collection<V> expectedValues,
			Collection<V> actualValues) {
		Iterator<V> actualIterator = actualValues.iterator();
		while (!expectedValues.isEmpty() && actualIterator.hasNext()) {
			// match
			final V actual = actualIterator.next();
			if (expectedValues.remove(actual))
				actualIterator.remove();
		}
	}

}
