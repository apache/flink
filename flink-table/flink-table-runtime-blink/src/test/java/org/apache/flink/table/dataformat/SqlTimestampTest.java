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

package org.apache.flink.table.dataformat;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

/**
 * Test for {@link SqlTimestamp}.
 */
public class SqlTimestampTest {

	@Test
	public void testNormal() {
		// From long to SqlTimestamp and vice versa
		Assert.assertEquals(1123L, SqlTimestamp.fromEpochMillis(1123L).getMillisecond());
		Assert.assertEquals(-1123L, SqlTimestamp.fromEpochMillis(-1123L).getMillisecond());

		Assert.assertEquals(1123L, SqlTimestamp.fromEpochMillis(1123L, 45678).getMillisecond());
		Assert.assertEquals(45678, SqlTimestamp.fromEpochMillis(1123L, 45678).getNanoOfMillisecond());

		Assert.assertEquals(-1123L, SqlTimestamp.fromEpochMillis(-1123L, 45678).getMillisecond());
		Assert.assertEquals(45678, SqlTimestamp.fromEpochMillis(-1123L, 45678).getNanoOfMillisecond());

		// From SqlTimestamp to SqlTimestamp and vice versa
		java.sql.Timestamp t19 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		java.sql.Timestamp t16 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456");
		java.sql.Timestamp t13 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123");
		java.sql.Timestamp t10 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00");

		Assert.assertEquals(t19, SqlTimestamp.fromTimestamp(t19).toTimestamp());
		Assert.assertEquals(t16, SqlTimestamp.fromTimestamp(t16).toTimestamp());
		Assert.assertEquals(t13, SqlTimestamp.fromTimestamp(t13).toTimestamp());
		Assert.assertEquals(t10, SqlTimestamp.fromTimestamp(t10).toTimestamp());

		java.sql.Timestamp t2 = java.sql.Timestamp.valueOf("1979-01-02 00:00:00.123456");
		Assert.assertEquals(t2, SqlTimestamp.fromTimestamp(t2).toTimestamp());

		java.sql.Timestamp t3 = new java.sql.Timestamp(1572333940000L);
		Assert.assertEquals(t3, SqlTimestamp.fromTimestamp(t3).toTimestamp());

		// From LocalDateTime to SqlTimestamp and vice versa
		LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
		LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
		LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

		Assert.assertEquals(ldt19, SqlTimestamp.fromLocalDateTime(ldt19).toLocalDateTime());
		Assert.assertEquals(ldt16, SqlTimestamp.fromLocalDateTime(ldt16).toLocalDateTime());
		Assert.assertEquals(ldt13, SqlTimestamp.fromLocalDateTime(ldt13).toLocalDateTime());
		Assert.assertEquals(ldt10, SqlTimestamp.fromLocalDateTime(ldt10).toLocalDateTime());

		LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
		Assert.assertEquals(ldt2, SqlTimestamp.fromLocalDateTime(ldt2).toLocalDateTime());

		LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals(ldt3, SqlTimestamp.fromLocalDateTime(ldt3).toLocalDateTime());

		LocalDateTime ldt4 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		java.sql.Timestamp t4 = java.sql.Timestamp.valueOf(ldt4);
		Assert.assertEquals(SqlTimestamp.fromLocalDateTime(ldt4), SqlTimestamp.fromTimestamp(t4));

		// From Instant to SqlTimestamp and vice versa
		Instant instant1 = Instant.ofEpochMilli(123L);
		Instant instant2 = Instant.ofEpochSecond(0L, 123456789L);
		Instant instant3 = Instant.ofEpochSecond(-2L, 123456789L);

		Assert.assertEquals(instant1, SqlTimestamp.fromInstant(instant1).toInstant());
		Assert.assertEquals(instant2, SqlTimestamp.fromInstant(instant2).toInstant());
		Assert.assertEquals(instant3, SqlTimestamp.fromInstant(instant3).toInstant());
	}

	@Test
	public void testDaylightSavingTime() {
		TimeZone tz = TimeZone.getDefault();
		TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

		java.sql.Timestamp dstBegin2018 = java.sql.Timestamp.valueOf("2018-03-11 03:00:00");
		Assert.assertEquals(dstBegin2018, SqlTimestamp.fromTimestamp(dstBegin2018).toTimestamp());

		java.sql.Timestamp dstBegin2019 = java.sql.Timestamp.valueOf("2019-03-10 02:00:00");
		Assert.assertEquals(dstBegin2019, SqlTimestamp.fromTimestamp(dstBegin2019).toTimestamp());

		TimeZone.setDefault(tz);
	}

	@Test
	public void testToString() {

		java.sql.Timestamp t = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		Assert.assertEquals("1969-01-02T00:00:00.123456789", SqlTimestamp.fromTimestamp(t).toString());

		Assert.assertEquals("1970-01-01T00:00:00.123", SqlTimestamp.fromEpochMillis(123L).toString());
		Assert.assertEquals("1970-01-01T00:00:00.123456789", SqlTimestamp.fromEpochMillis(123L, 456789).toString());

		Assert.assertEquals("1969-12-31T23:59:59.877", SqlTimestamp.fromEpochMillis(-123L).toString());
		Assert.assertEquals("1969-12-31T23:59:59.877456789", SqlTimestamp.fromEpochMillis(-123L, 456789).toString());

		LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals("1969-01-02T00:00:00.123456789", SqlTimestamp.fromLocalDateTime(ldt).toString());

		Instant instant = Instant.ofEpochSecond(0L, 123456789L);
		Assert.assertEquals("1970-01-01T00:00:00.123456789", SqlTimestamp.fromInstant(instant).toString());
	}
}
