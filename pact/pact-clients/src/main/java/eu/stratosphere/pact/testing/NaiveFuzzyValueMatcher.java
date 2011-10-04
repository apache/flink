package eu.stratosphere.pact.testing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.type.Value;

/**
 * Simple matching algorithm that returns unmatched values but allows a value from one bag to be matched several times
 * against items from another bag.
 * 
 * @author Arvid.Heise
 * @param <V>
 */
public class NaiveFuzzyValueMatcher<V extends Value> implements FuzzyTestValueMatcher<V> {
	@Override
	public void removeMatchingValues(FuzzyTestValueSimilarity<V> matcher, Collection<V> expectedValues,
			Collection<V> actualValues) {
		Iterator<V> expectedIterator = expectedValues.iterator();
		List<V> matchedActualValues = new ArrayList<V>();
		while (expectedIterator.hasNext()) {
			V expected = expectedIterator.next();
			boolean matched = false;
			for (V actual : actualValues)
				if (matcher.getDistance(expected, actual) >= 0) {
					matched = true;
					matchedActualValues.add(actual);
				}
			if (matched)
				expectedIterator.remove();
		}

		actualValues.removeAll(matchedActualValues);
	}
}
