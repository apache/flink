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

import static eu.stratosphere.pact.testing.PactRecordUtil.stringify;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.TestRecords.SortInfo;

/**
 * Utility class that is used from {@link TestRecords#assertEquals(TestRecords)}.<br>
 * It tries to align all matching records by successively eliminating matching records from the multiset of expected and
 * actual values.<br>
 * To reduce memory consumption, only tuples with the same key are held within memory. The key is simply determined by
 * using all parts of the schema that implement {@link Key} and that are not matched fuzzily.
 * 
 * @author Arvid Heise
 */
class TestRecordsAssertor {
	private Class<? extends Value>[] schema;

	private FuzzyValueMatcher fuzzyMatcher;

	private Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap;

	private SortInfo sortInfo;

	private TestRecords expectedValues, actualRecords;

	private Iterator<PactRecord> actualIterator;

	private Iterator<PactRecord> expectedIterator;

	public TestRecordsAssertor(Class<? extends Value>[] schema, FuzzyValueMatcher fuzzyMatcher,
			Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap, SortInfo sortInfo, TestRecords expectedValues,
			TestRecords actualRecords) {
		this.schema = schema;
		this.fuzzyMatcher = fuzzyMatcher;
		this.similarityMap = similarityMap;
		this.sortInfo = this.getSortInfoForAssertion(similarityMap,
			TestRecords.firstNonNull(expectedValues.sortInfo, this.sortInfo));
		if (sortInfo == null)
			throw new IllegalStateException("Expected value does not have schema specified");
		this.expectedValues = expectedValues;
		this.actualRecords = actualRecords;
		this.actualIterator = actualRecords.iterator(sortInfo);
		this.expectedIterator = expectedValues.iterator(sortInfo);
	}

	public void assertEquals() {
		try {
			// initialize with null
			List<Key> currentKeys = new ArrayList<Key>(Arrays.asList(new Key[this.sortInfo.sortKeys.size()])), nextKeys =
				new ArrayList<Key>(currentKeys);
			int itemIndex = 0;
			List<PactRecord> expectedValuesWithCurrentKey = new ArrayList<PactRecord>();
			List<PactRecord> actualValuesWithCurrentKey = new ArrayList<PactRecord>();
			if (this.expectedIterator.hasNext()) {
				PactRecord expected = this.expectedIterator.next();
				this.setKeys(currentKeys, expected);
				expectedValuesWithCurrentKey.add(expected);

				// take chunks of expected values with the same keys and match them
				while (this.actualIterator.hasNext() && this.expectedIterator.hasNext()) {
					expected = this.expectedIterator.next().createCopy();
					this.setKeys(nextKeys, expected);
					if (!currentKeys.equals(nextKeys)) {
						this.matchValues(currentKeys, itemIndex, expectedValuesWithCurrentKey,
							actualValuesWithCurrentKey);
						this.setKeys(currentKeys, expected);
					}
					expectedValuesWithCurrentKey.add(expected);

					itemIndex++;
				}

				// remaining values
				if (!expectedValuesWithCurrentKey.isEmpty())
					this.matchValues(currentKeys, itemIndex, expectedValuesWithCurrentKey, actualValuesWithCurrentKey);
			}

			if (!expectedValuesWithCurrentKey.isEmpty() || this.expectedIterator.hasNext())
				Assert.fail("More elements expected: " + expectedValuesWithCurrentKey
					+ stringify(this.expectedIterator, this.schema));
			if (!actualValuesWithCurrentKey.isEmpty() || this.actualIterator.hasNext())
				Assert.fail("Less elements expected: " + actualValuesWithCurrentKey
					+ stringify(this.actualIterator, this.schema));
		} finally {
			this.actualRecords.close();
			this.expectedValues.close();
		}
	}

	private void matchValues(List<Key> currentKeys, int itemIndex, List<PactRecord> expectedValuesWithCurrentKey,
			List<PactRecord> actualValuesWithCurrentKey) throws ArrayComparisonFailure {

		List<Key> actualKeys = new ArrayList<Key>(currentKeys);

		// collect all actual values with the same key
		PactRecord actualRecord = null;
		while (this.actualIterator.hasNext()) {
			actualRecord = this.actualIterator.next();
			this.setKeys(actualKeys, actualRecord);

			if (!currentKeys.equals(actualKeys))
				break;
			actualValuesWithCurrentKey.add(actualRecord);
			actualRecord = null;
		}

		if (actualValuesWithCurrentKey.isEmpty())
			throw new ArrayComparisonFailure("Unexpected value for key " + currentKeys, new AssertionFailedError(
				Assert.format(" ", stringify(expectedValuesWithCurrentKey.iterator(), this.schema), stringify(
					actualRecord, this.schema))), itemIndex + expectedValuesWithCurrentKey.size() - 1);

		// and invoke the fuzzy matcher
		this.fuzzyMatcher.removeMatchingValues(this.similarityMap, this.schema, expectedValuesWithCurrentKey,
			actualValuesWithCurrentKey);

		if (!expectedValuesWithCurrentKey.isEmpty() || !actualValuesWithCurrentKey.isEmpty())
			throw new ArrayComparisonFailure("Unexpected values for key " + currentKeys + ": ",
				new AssertionFailedError(Assert.format(" ", stringify(expectedValuesWithCurrentKey.iterator(),
					this.schema), stringify(actualValuesWithCurrentKey.iterator(), this.schema))),
				itemIndex - expectedValuesWithCurrentKey.size());

		// don't forget the first record that has a different key
		if (actualRecord != null)
			actualValuesWithCurrentKey.add(actualRecord);
	}

	protected SortInfo getSortInfoForAssertion(Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap, SortInfo sortInfo) {
		if (similarityMap.isEmpty())
			return sortInfo;
		sortInfo = sortInfo.copy();
		// remove all keys that have a fuzzy similarity measure
		for (Entry<List<ValueSimilarity<?>>> similarityEntry : similarityMap.int2ObjectEntrySet())
			if (similarityEntry.getIntKey() != TestRecords.ALL_VALUES && !similarityEntry.getValue().isEmpty())
				sortInfo.remove(similarityEntry.getIntKey());
		return sortInfo;
	}

	private void setKeys(List<Key> keyList, PactRecord record) {
		for (int index = 0; index < this.sortInfo.sortKeys.size(); index++) {
			final int fieldIndex = this.sortInfo.sortKeys.getInt(index);
			if (record.getNumFields() <= fieldIndex)
				Assert.fail("Record has less fields then the expected values suggested: "
					+ stringify(record, this.schema));
			keyList.set(index, record.getField(fieldIndex, this.sortInfo.keyClasses.get(index)));
		}
	}
}