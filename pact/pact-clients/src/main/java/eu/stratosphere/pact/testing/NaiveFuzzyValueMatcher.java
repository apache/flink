package eu.stratosphere.pact.testing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Simple matching algorithm that returns unmatched values but allows a value from one bag to be matched several times
 * against items from another bag.
 * 
 * @author Arvid.Heise
 * @param <PactRecord>
 */
public class NaiveFuzzyValueMatcher extends AbstractValueMatcher {
	@Override
	public void removeMatchingValues(Int2ObjectMap<List<ValueSimilarity<?>>> similarities,
			Class<? extends Value>[] schema, Collection<PactRecord> expectedValues, Collection<PactRecord> actualValues) {
		Iterator<PactRecord> expectedIterator = expectedValues.iterator();
		List<PactRecord> matchedActualValues = new ArrayList<PactRecord>();
		while (expectedIterator.hasNext()) {
			PactRecord expected = expectedIterator.next();
			boolean matched = false;
			for (PactRecord actual : actualValues)
				if (getDistance(schema, similarities, expected, actual) >= 0) {
					matched = true;
					matchedActualValues.add(actual);
				}
			if (matched)
				expectedIterator.remove();
		}

		actualValues.removeAll(matchedActualValues);
	}

}
