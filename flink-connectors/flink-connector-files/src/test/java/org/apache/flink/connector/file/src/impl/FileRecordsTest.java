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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.connector.file.src.util.SingletonResultIterator;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the {@link FileRecords} class.
 */
public class FileRecordsTest {

	@Test
	public void testEmptySplits() {
		final String split = "empty";
		final FileRecords<Object> records = FileRecords.finishedSplit(split);

		assertEquals(Collections.singleton(split), records.finishedSplits());
	}

	@Test
	public void testMoveToFirstSplit() {
		final String splitId = "splitId";
		final FileRecords<Object> records = FileRecords.forRecords(splitId, new SingletonResultIterator<>());

		final String firstSplitId = records.nextSplit();

		assertEquals(splitId, firstSplitId);
	}

	@Test
	public void testMoveToSecondSplit() {
		final FileRecords<Object> records = FileRecords.forRecords("splitId", new SingletonResultIterator<>());
		records.nextSplit();

		final String secondSplitId = records.nextSplit();

		assertNull(secondSplitId);
	}

	@Test
	public void testRecordsFromFirstSplit() {
		final SingletonResultIterator<String> iter = new SingletonResultIterator<>();
		iter.set("test", 18, 99);
		final FileRecords<String> records = FileRecords.forRecords("splitId", iter);
		records.nextSplit();

		final RecordAndPosition<String> recAndPos = records.nextRecordFromSplit();

		assertEquals("test", recAndPos.getRecord());
		assertEquals(18, recAndPos.getOffset());
		assertEquals(99, recAndPos.getRecordSkipCount());
	}

	@Test(expected = IllegalStateException.class)
	public void testRecordsInitiallyIllegal() {
		final FileRecords<Object> records = FileRecords.forRecords("splitId", new SingletonResultIterator<>());

		records.nextRecordFromSplit();
	}

	@Test(expected = IllegalStateException.class)
	public void testRecordsOnSecondSplitIllegal() {
		final FileRecords<Object> records = FileRecords.forRecords("splitId", new SingletonResultIterator<>());
		records.nextSplit();
		records.nextSplit();

		records.nextRecordFromSplit();
	}

	@Test
	public void testRecycleExhaustedBatch() {
		final AtomicBoolean recycled = new AtomicBoolean(false);
		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>(() -> recycled.set(true));
		iter.set(new Object(), 1L, 2L);

		final FileRecords<Object> records = FileRecords.forRecords("test split", iter);
		records.nextSplit();
		records.nextRecordFromSplit();

		// make sure we exhausted the iterator
		assertNull(records.nextRecordFromSplit());
		assertNull(records.nextSplit());

		records.recycle();
		assertTrue(recycled.get());
	}

	@Test
	public void testRecycleNonExhaustedBatch() {
		final AtomicBoolean recycled = new AtomicBoolean(false);
		final SingletonResultIterator<Object> iter = new SingletonResultIterator<>(() -> recycled.set(true));
		iter.set(new Object(), 1L, 2L);

		final FileRecords<Object> records = FileRecords.forRecords("test split", iter);
		records.nextSplit();

		records.recycle();
		assertTrue(recycled.get());
	}
}
