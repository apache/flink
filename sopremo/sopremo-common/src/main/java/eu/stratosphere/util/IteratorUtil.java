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
package eu.stratosphere.util;

import java.util.Iterator;

import eu.stratosphere.pact.testing.Equaler;

/**
 * @author Arvid Heise
 */
public class IteratorUtil {
	public static <T> boolean equal(Iterator<? extends T> expectedIterator, Iterator<? extends T> actualIterator,
			Equaler<T> equaler) {

		for (; actualIterator.hasNext() && expectedIterator.hasNext();) {
			final T expected = expectedIterator.next(), actual = actualIterator.next();
			if (!equaler.equal(expected, actual))
				return false;
		}

		return !actualIterator.hasNext() && !expectedIterator.hasNext();
	}

	public static <T> boolean equal(Iterator<? extends T> expectedIterator, Iterator<? extends T> actualIterator) {
		return IteratorUtil.equal(expectedIterator, actualIterator, Equaler.JavaEquals);
	}

	public static <T> int hashCode(Iterator<? extends T> iterator, HashCoder<T> hashCoder) {
		int prime = 101;
		int result = 1;
		for (; iterator.hasNext();)
			result = prime * result + hashCoder.hashCodeFor(iterator.next());

		return result;
	}

	public static <T> int hashCode(Iterator<? extends T> iterator) {
		return IteratorUtil.hashCode(iterator, HashCoder.JavaHashCode);
	}

	public static <T> String toString(Iterator<? extends T> iterator, int maxEntries, Stringifier<T> stringifier) {
		final StringBuilder stringBuilder = new StringBuilder();
		for (int index = 0; index < maxEntries && iterator.hasNext(); index++) {
			if (index > 0)
				stringBuilder.append("; ");
			stringBuilder.append(stringifier.stringify(iterator.next()));
		}
		if (iterator.hasNext())
			stringBuilder.append("...");
		return stringBuilder.toString();
	}

	public static <T> String toString(Iterator<? extends T> iterator, int maxEntries) {
		return toString(iterator, maxEntries, Stringifier.JavaString);
	}
}
