/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.src.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the {@link ArrayResultIterator}.
 */
public class ArrayResultIteratorTest {

	@Test
	public void testEmptyConstruction() {
		final ArrayResultIterator<Object> iter = new ArrayResultIterator<>();
		assertNull(iter.next());
	}

	@Test
	public void testGetElements() {
		final String[] elements = new String[] { "1", "2", "3", "4"};
		final long initialPos = 1422;
		final long initialSkipCount = 17;

		final ArrayResultIterator<String> iter = new ArrayResultIterator<>();
		iter.set(elements, elements.length, initialPos, initialSkipCount);

		for (int i = 0; i < elements.length; i++) {
			final RecordAndPosition<String> recAndPos = iter.next();
			assertEquals(elements[i], recAndPos.getRecord());
			assertEquals(initialPos, recAndPos.getOffset());
			assertEquals(initialSkipCount + i + 1, recAndPos.getRecordSkipCount());
		}
	}

	@Test
	public void testExhausted() {
		final ArrayResultIterator<String> iter = new ArrayResultIterator<>();
		iter.set(new String[] { "1", "2"}, 2, 0L, 0L);

		iter.next();
		iter.next();

		assertNull(iter.next());
	}

	@Test
	public void testArraySubRange() {
		final ArrayResultIterator<String> iter = new ArrayResultIterator<>();
		iter.set(new String[] { "1", "2", "3"}, 2, 0L, 0L);

		assertNotNull(iter.next());
		assertNotNull(iter.next());
		assertNull(iter.next());
	}

	@Test
	public void testNoRecycler() {
		final ArrayResultIterator<Object> iter = new ArrayResultIterator<>();
		iter.releaseBatch();
	}

	@Test
	public void testRecycler() {
		final AtomicBoolean recycled = new AtomicBoolean();
		final ArrayResultIterator<Object> iter = new ArrayResultIterator<>(() -> recycled.set(true));

		iter.releaseBatch();

		assertTrue(recycled.get());
	}
}
