/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
 * This utility class provides functionality to work easier with iterators.
 * 
 * @author Arvid Heise
 */
public class IteratorUtil {

	/**
	 * Determines if two given iterators are equal concerning the given {@link Equaler}.
	 * 
	 * @param expectedIterator
	 *        the first iterator
	 * @param actualIterator
	 *        the second iterator
	 * @param equaler
	 *        the Equaler that should be used
	 * @return
	 *         either the two iterators are equal or not
	 */
	public static <T> boolean equal(final Iterator<? extends T> expectedIterator,
			final Iterator<? extends T> actualIterator,
			final Equaler<T> equaler) {

		for (; actualIterator.hasNext() && expectedIterator.hasNext();) {
			final T expected = expectedIterator.next(), actual = actualIterator.next();
			if (!equaler.equal(expected, actual))
				return false;
		}

		return !actualIterator.hasNext() && !expectedIterator.hasNext();
	}

	/**
	 * Determines if two given iterators are equal concerning {@link Object#equals(Object)}.
	 * 
	 * @param expectedIterator
	 *        the first iterator
	 * @param actualIterator
	 *        the second iterator
	 * @return
	 *         either the two iterators are equal or not
	 */
	public static <T> boolean equal(final Iterator<? extends T> expectedIterator,
			final Iterator<? extends T> actualIterator) {
		return IteratorUtil.equal(expectedIterator, actualIterator, Equaler.JavaEquals);
	}

	/**
	 * Generates the hash code of the given iterator. The given {@link HashCoder} is used for calculation.
	 * 
	 * @param iterator
	 *        the iterator that should be used
	 * @param hashCoder
	 *        the HashCoder that should be used
	 * @return
	 *         the hash code
	 */
	public static <T> int hashCode(final Iterator<? extends T> iterator, final HashCoder<T> hashCoder) {
		final int prime = 101;
		int result = 1;
		for (; iterator.hasNext();)
			result = prime * result + hashCoder.hashCodeFor(iterator.next());

		return result;
	}

	/**
	 * Generates the hash code of the given iterator. {@link Object#hashCode()} is used for calculation.
	 * 
	 * @param iterator
	 *        the iterator that should be used
	 * @return
	 *         the hash code
	 */
	public static <T> int hashCode(final Iterator<? extends T> iterator) {
		return IteratorUtil.hashCode(iterator, HashCoder.JavaHashCode);
	}

	/**
	 * Generates a string representation of the given iterator. Each element will be transformed to a string by using
	 * the given {@link Stringifier}.
	 * 
	 * @param iterator
	 *        the iterator that should be used
	 * @param maxEntries
	 *        The maximum of elements that should be transformed into strings. Should the iterator contain more, than
	 *        his string representation will have a trailing <code>...</code>
	 * @param stringifier
	 *        the Stringifier that should be used
	 * @return
	 *         the string representation
	 */
	public static <T> String toString(final Iterator<? extends T> iterator, final int maxEntries,
			final Stringifier<T> stringifier) {
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

	/**
	 * Generates a string representation of the given iterator. Each element will be transformed to a string by using
	 * {@link Object#toString()}.
	 * 
	 * @param iterator
	 *        the iterator that should be used
	 * @param maxEntries
	 *        The maximum of elements that should be transformed into strings. Should the iterator contain more, than
	 *        his string representation will have a trailing <code>...</code>
	 * @return
	 *         the string representation
	 */
	public static <T> String toString(final Iterator<? extends T> iterator, final int maxEntries) {
		return toString(iterator, maxEntries, Stringifier.JavaString);
	}
}
