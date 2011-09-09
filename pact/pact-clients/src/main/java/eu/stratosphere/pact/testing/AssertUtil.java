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

import java.util.Iterator;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.junit.internal.ArrayComparisonFailure;

/**
 * Additional assertions for unit tests.
 * 
 * @author Arvid Heise
 */
public class AssertUtil {
	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param message
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static void assertIteratorEquals(String message, Iterator<?> expectedIterator, Iterator<?> actualIterator) {

		int index = 0;
		for (; actualIterator.hasNext() && expectedIterator.hasNext(); index++) {
			final Object expected = expectedIterator.next(), actual = actualIterator.next();
			try {
				Assert.assertEquals(expected, actual);
			} catch (final AssertionFailedError e) {
				throw new ArrayComparisonFailure(message, e, index);
			}
		}

		if (expectedIterator.hasNext())
			throw new ArrayComparisonFailure(message, new AssertionError("More elements expected"), index);
		if (actualIterator.hasNext())
			throw new ArrayComparisonFailure(message, new AssertionError("Less elements expected"), index);
	}

	/**
	 * Asserts that two iterators generate equal series of objects.
	 * 
	 * @param expectedIterator
	 * @param actualIterator
	 */
	public static void assertIteratorEquals(Iterator<?> expectedIterator, Iterator<?> actualIterator) {
		assertIteratorEquals(null, expectedIterator, actualIterator);
	}

}
