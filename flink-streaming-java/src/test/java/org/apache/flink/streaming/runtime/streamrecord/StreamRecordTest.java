/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StreamRecord}.
 */
public class StreamRecordTest {

	@Test
	public void testWithNoTimestamp() {
		StreamRecord<String> record = new StreamRecord<>("test");

		assertTrue(record.isRecord());
		assertFalse(record.isWatermark());

		assertFalse(record.hasTimestamp());
		assertEquals("test", record.getValue());

//		try {
//			record.getTimestamp();
//			fail("should throw an exception");
//		} catch (IllegalStateException e) {
//			assertTrue(e.getMessage().contains("timestamp"));
//		}
		// for now, the "no timestamp case" returns Long.MIN_VALUE
		assertEquals(Long.MIN_VALUE, record.getTimestamp());

		assertNotNull(record.toString());
		assertTrue(record.hashCode() == new StreamRecord<>("test").hashCode());
		assertTrue(record.equals(new StreamRecord<>("test")));

		assertEquals(record, record.asRecord());

		try {
			record.asWatermark();
			fail("should throw an exception");
		} catch (Exception e) {
			// expected
		}
	}

	@Test
	public void testWithTimestamp() {
		StreamRecord<String> record = new StreamRecord<>("foo", 42);

		assertTrue(record.isRecord());
		assertFalse(record.isWatermark());

		assertTrue(record.hasTimestamp());
		assertEquals(42L, record.getTimestamp());

		assertEquals("foo", record.getValue());

		assertNotNull(record.toString());

		assertTrue(record.hashCode() == new StreamRecord<>("foo", 42).hashCode());
		assertTrue(record.hashCode() != new StreamRecord<>("foo").hashCode());

		assertTrue(record.equals(new StreamRecord<>("foo", 42)));
		assertFalse(record.equals(new StreamRecord<>("foo")));

		assertEquals(record, record.asRecord());

		try {
			record.asWatermark();
			fail("should throw an exception");
		} catch (Exception e) {
			// expected
		}
	}

	@Test
	public void testAllowedTimestampRange() {
		assertEquals(0L, new StreamRecord<>("test", 0).getTimestamp());
		assertEquals(-1L, new StreamRecord<>("test", -1).getTimestamp());
		assertEquals(1L, new StreamRecord<>("test", 1).getTimestamp());
		assertEquals(Long.MIN_VALUE, new StreamRecord<>("test", Long.MIN_VALUE).getTimestamp());
		assertEquals(Long.MAX_VALUE, new StreamRecord<>("test", Long.MAX_VALUE).getTimestamp());
	}

	@Test
	public void testReplacePreservesTimestamp() {
		StreamRecord<String> recNoTimestamp = new StreamRecord<>("o sole mio");
		StreamRecord<Integer> newRecNoTimestamp = recNoTimestamp.replace(17);
		assertFalse(newRecNoTimestamp.hasTimestamp());

		StreamRecord<String> recWithTimestamp = new StreamRecord<>("la dolce vita", 99);
		StreamRecord<Integer> newRecWithTimestamp = recWithTimestamp.replace(17);

		assertTrue(newRecWithTimestamp.hasTimestamp());
		assertEquals(99L, newRecWithTimestamp.getTimestamp());
	}

	@Test
	public void testReplaceWithTimestampOverridesTimestamp() {
		StreamRecord<String> record = new StreamRecord<>("la divina comedia");
		assertFalse(record.hasTimestamp());

		StreamRecord<Double> newRecord = record.replace(3.14, 123);
		assertTrue(newRecord.hasTimestamp());
		assertEquals(123L, newRecord.getTimestamp());
	}

	@Test
	public void testCopy() {
		StreamRecord<String> recNoTimestamp = new StreamRecord<String>("test");
		StreamRecord<String> recNoTimestampCopy = recNoTimestamp.copy("test");
		assertEquals(recNoTimestamp, recNoTimestampCopy);

		StreamRecord<String> recWithTimestamp = new StreamRecord<String>("test", 99);
		StreamRecord<String> recWithTimestampCopy = recWithTimestamp.copy("test");
		assertEquals(recWithTimestamp, recWithTimestampCopy);
	}

	@Test
	public void testCopyTo() {
		StreamRecord<String> recNoTimestamp = new StreamRecord<String>("test");
		StreamRecord<String> recNoTimestampCopy = new StreamRecord<>(null);
		recNoTimestamp.copyTo("test", recNoTimestampCopy);
		assertEquals(recNoTimestamp, recNoTimestampCopy);

		StreamRecord<String> recWithTimestamp = new StreamRecord<String>("test", 99);
		StreamRecord<String> recWithTimestampCopy = new StreamRecord<>(null);
		recWithTimestamp.copyTo("test", recWithTimestampCopy);
		assertEquals(recWithTimestamp, recWithTimestampCopy);
	}

	@Test
	public void testSetAndEraseTimestamps() {
		StreamRecord<String> rec = new StreamRecord<String>("hello");
		assertFalse(rec.hasTimestamp());

		rec.setTimestamp(13456L);
		assertTrue(rec.hasTimestamp());
		assertEquals(13456L, rec.getTimestamp());

		rec.eraseTimestamp();
		assertFalse(rec.hasTimestamp());
	}
}
