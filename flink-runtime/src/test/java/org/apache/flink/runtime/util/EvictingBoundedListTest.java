/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class EvictingBoundedListTest {

	@Test
	public void testAddGet() {
		int insertSize = 17;
		int boundSize = 5;
		Integer defaultElement = 4711;

		EvictingBoundedList<Integer> list = new EvictingBoundedList<>(boundSize, defaultElement);
		assertTrue(list.isEmpty());

		for (int i = 0; i < insertSize; ++i) {
			list.add(i);
		}

		assertEquals(17, list.size());

		for (int i = 0; i < insertSize; ++i) {
			int exp = i < (insertSize - boundSize) ? defaultElement : i;
			int act = list.get(i);
			assertEquals(exp, act);
		}
	}

	@Test
	public void testSet() {
		int insertSize = 17;
		int boundSize = 5;
		Integer defaultElement = 4711;
		List<Integer> reference = new ArrayList<>(insertSize);
		EvictingBoundedList<Integer> list = new EvictingBoundedList<>(boundSize, defaultElement);
		for (int i = 0; i < insertSize; ++i) {
			reference.add(i);
			list.add(i);
		}

		assertEquals(reference.size(), list.size());

		list.set(0, 123);
		list.set(insertSize - boundSize - 1, 123);

		list.set(insertSize - boundSize, 42);
		reference.set(insertSize - boundSize, 42);
		list.set(13, 43);
		reference.set(13, 43);
		list.set(16, 44);
		reference.set(16, 44);

		try {
			list.set(insertSize, 23);
			fail("Illegal index in set not detected.");
		} catch (IllegalArgumentException ignored) {

		}

		for (int i = 0; i < insertSize; ++i) {
			int exp = i < (insertSize - boundSize) ? defaultElement : reference.get(i);
			int act = list.get(i);
			assertEquals(exp, act);
		}

		assertEquals(reference.size(), list.size());
	}

	@Test
	public void testClear() {
		int insertSize = 17;
		int boundSize = 5;
		Integer defaultElement = 4711;

		EvictingBoundedList<Integer> list = new EvictingBoundedList<>(boundSize, defaultElement);
		for (int i = 0; i < insertSize; ++i) {
			list.add(i);
		}

		list.clear();

		assertEquals(0, list.size());
		assertTrue(list.isEmpty());

		try {
			list.get(0);
			fail();
		} catch (IndexOutOfBoundsException ignore) {
		}
	}

	@Test
	public void testIterator() {
		int insertSize = 17;
		int boundSize = 5;
		Integer defaultElement = 4711;

		EvictingBoundedList<Integer> list = new EvictingBoundedList<>(boundSize, defaultElement);
		assertTrue(list.isEmpty());

		for (int i = 0; i < insertSize; ++i) {
			list.add(i);
		}

		Iterator<Integer> iterator = list.iterator();

		for (int i = 0; i < insertSize; ++i) {
			assertTrue(iterator.hasNext());
			int exp = i < (insertSize - boundSize) ? defaultElement : i;
			int act = iterator.next();
			assertEquals(exp, act);
		}

		assertFalse(iterator.hasNext());

		try {
			iterator.next();
			fail("Next on exhausted iterator did not trigger exception.");
		} catch (NoSuchElementException ignored) {

		}

		iterator = list.iterator();
		assertTrue(iterator.hasNext());
		iterator.next();
		list.add(123);
		assertTrue(iterator.hasNext());
		try {
			iterator.next();
			fail("Concurrent modification not detected.");
		} catch (ConcurrentModificationException ignored) {

		}
	}

	@Test
	public void testMapWithHalfFullList() {
		final Object[] originals = { new Object(), new Object(), new Object() };
		final Object defaultValue = new Object();

		final EvictingBoundedList<Object> original = new EvictingBoundedList<>(5, defaultValue);
		for (Object o : originals) {
			original.add(o);
		}

		final EvictingBoundedList<TransformedObject> transformed = original.map(new Mapper());

		assertEquals(original.size(), transformed.size());
		assertEquals(original.getSizeLimit(), transformed.getSizeLimit());
		assertEquals(defaultValue, transformed.getDefaultElement().original);

		int i = 0;
		for (TransformedObject to : transformed) {
			assertEquals(originals[i++], to.original); 
		}

		try {
			transformed.get(originals.length);
			fail("should have failed with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}
	}

	@Test
	public void testMapWithEvictedElements() {
		final Object[] originals = { new Object(), new Object(), new Object(), new Object(), new Object() };
		final Object defaultValue = new Object();

		final EvictingBoundedList<Object> original = new EvictingBoundedList<>(2, defaultValue);
		for (Object o : originals) {
			original.add(o);
		}

		final EvictingBoundedList<TransformedObject> transformed = original.map(new Mapper());

		assertEquals(originals.length, transformed.size());
		assertEquals(original.size(), transformed.size());
		assertEquals(original.getSizeLimit(), transformed.getSizeLimit());
		assertEquals(defaultValue, transformed.getDefaultElement().original);

		for (int i = 0; i < originals.length; i++) {
			if (i < originals.length - transformed.getSizeLimit()) {
				assertEquals(transformed.getDefaultElement(), transformed.get(i));
			} else {
				assertEquals(originals[i], transformed.get(i).original);
			}
		}

		try {
			transformed.get(originals.length);
			fail("should have failed with an exception");
		} catch (IndexOutOfBoundsException e) {
			// expected
		}
	}

	@Test
	public void testMapWithNullDefault() {
		final EvictingBoundedList<Object> original = new EvictingBoundedList<>(5, null);
		final EvictingBoundedList<TransformedObject> transformed = original.map(new Mapper());

		assertEquals(original.size(), transformed.size());
		assertNull(transformed.getDefaultElement());
	}

	// ------------------------------------------------------------------------

	private static final class TransformedObject {

		final Object original;

		TransformedObject(Object original) {
			this.original = checkNotNull(original);
		}
	}

	// ------------------------------------------------------------------------

	private static final class Mapper implements EvictingBoundedList.Function<Object, TransformedObject> {

		@Override
		public TransformedObject apply(Object value) {
			return new TransformedObject(value);
		}
	}
}
