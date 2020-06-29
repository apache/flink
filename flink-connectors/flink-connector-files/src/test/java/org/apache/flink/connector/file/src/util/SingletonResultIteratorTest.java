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
 * Unit tests for the {@link SingletonResultIterator}.
 */
public class SingletonResultIteratorTest {

	@Test
	public void testEmptyConstruction() {
		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
		assertNull(iter.next());
	}

	@Test
	public void testGetElement() {
		final Object element = new Object();
		final long pos = 1422;
		final long skipCount = 17;

		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
		iter.set(element, pos, skipCount);

		final RecordAndPosition<Object> record = iter.next();
		assertNotNull(record);
		assertEquals(element, record.getRecord());
		assertEquals(pos, record.getOffset());
		assertEquals(skipCount, record.getRecordSkipCount());
	}

	@Test
	public void testExhausted() {
		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
		iter.set(new Object(), 1, 2);
		iter.next();

		assertNull(iter.next());
	}

	@Test
	public void testNoRecycler() {
		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>();
		iter.releaseBatch();
	}

	@Test
	public void testRecycler() {
		final AtomicBoolean recycled = new AtomicBoolean();
		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>(() -> recycled.set(true));

		iter.releaseBatch();

		assertTrue(recycled.get());
	}
}
