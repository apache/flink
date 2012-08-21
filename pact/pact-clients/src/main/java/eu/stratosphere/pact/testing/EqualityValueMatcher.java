/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

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
