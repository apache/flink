package eu.stratosphere.pact.testing;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

/**
 * Matches all exact matching pairs using equals.
 * 
 * @author Arvid Heise
 * @param <V>
 */
public class EqualityValueMatcher implements FuzzyValueMatcher {
	private Equaler<PactRecord> recordEqualer;

	/**
	 * Initializes EqualityValueMatcher.
	 */
	public EqualityValueMatcher() {
		this(null);
	}

	public EqualityValueMatcher(Equaler<PactRecord> recordEqualer) {
		this.recordEqualer = recordEqualer;
	}

	@Override
	public void removeMatchingValues(Int2ObjectMap<List<ValueSimilarity<?>>> similarities,
			Class<? extends Value>[] schema, Collection<PactRecord> expectedValues, Collection<PactRecord> actualValues) {
		Equaler<PactRecord> recordEqualer = this.recordEqualer == null ? new PactRecordEqualer(schema)
			: this.recordEqualer;

		Iterator<PactRecord> actualIterator = actualValues.iterator();
		while (!expectedValues.isEmpty() && actualIterator.hasNext()) {
			// match
			final PactRecord actual = actualIterator.next();

			Iterator<PactRecord> expectedIterator = expectedValues.iterator();
			while (expectedIterator.hasNext())
				if (recordEqualer.equal(actual, expectedIterator.next())) {
					actualIterator.remove();
					expectedIterator.remove();
					break;
				}
		}
	}

}
