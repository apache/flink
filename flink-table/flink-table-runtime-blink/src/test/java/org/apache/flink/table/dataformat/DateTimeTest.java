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

import java.time.LocalDateTime;

/**
 * Test for {@link DateTime}.
 */
public class DateTimeTest {

	@Test
	public void testNormal() {
		// From long to DateTime and vice versa
		Assert.assertEquals(1123L, DateTime.fromEpochMillis(1123L).getMillisecond());
		Assert.assertEquals(-1123L, DateTime.fromEpochMillis(-1123L).getMillisecond());

		Assert.assertEquals(1123L, DateTime.fromEpochMillis(1123L, 45678).getMillisecond());
		Assert.assertEquals(45678, DateTime.fromEpochMillis(1123L, 45678).getNanoOfMillisecond());

		Assert.assertEquals(-1123L, DateTime.fromEpochMillis(-1123L, 45678).getMillisecond());
		Assert.assertEquals(45678, DateTime.fromEpochMillis(-1123L, 45678).getNanoOfMillisecond());

		// From DateTime to DateTime and vice versa
		java.sql.Timestamp t19 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		java.sql.Timestamp t16 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456");
		java.sql.Timestamp t13 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123");
		java.sql.Timestamp t10 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00");

		Assert.assertEquals(t19, DateTime.fromTimestamp(t19).toTimestamp());
		Assert.assertEquals(t16, DateTime.fromTimestamp(t16).toTimestamp());
		Assert.assertEquals(t13, DateTime.fromTimestamp(t13).toTimestamp());
		Assert.assertEquals(t10, DateTime.fromTimestamp(t10).toTimestamp());

		java.sql.Timestamp t2 = java.sql.Timestamp.valueOf("1979-01-02 00:00:00.123456");
		Assert.assertEquals(t2, DateTime.fromTimestamp(t2).toTimestamp());

		java.sql.Timestamp t3 = new java.sql.Timestamp(1572333940000L);
		Assert.assertEquals(t3, DateTime.fromTimestamp(t3).toTimestamp());

		// From LocalDateTime to DateTime and vice versa
		LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
		LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
		LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

		Assert.assertEquals(ldt19, DateTime.fromLocalDateTime(ldt19).toLocalDateTime());
		Assert.assertEquals(ldt16, DateTime.fromLocalDateTime(ldt16).toLocalDateTime());
		Assert.assertEquals(ldt13, DateTime.fromLocalDateTime(ldt13).toLocalDateTime());
		Assert.assertEquals(ldt10, DateTime.fromLocalDateTime(ldt10).toLocalDateTime());

		LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
		Assert.assertEquals(ldt2, DateTime.fromLocalDateTime(ldt2).toLocalDateTime());

		LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals(ldt3, DateTime.fromLocalDateTime(ldt3).toLocalDateTime());

		LocalDateTime ldt4 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		java.sql.Timestamp t4 = java.sql.Timestamp.valueOf(ldt4);
		Assert.assertEquals(DateTime.fromLocalDateTime(ldt4), DateTime.fromTimestamp(t4));
	}

	@Test
	public void testToString() {

		java.sql.Timestamp t = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		Assert.assertEquals("1969-01-02T00:00:00.123456789", DateTime.fromTimestamp(t).toString());

		Assert.assertEquals("1970-01-01T00:00:00.123", DateTime.fromEpochMillis(123L).toString());
		Assert.assertEquals("1970-01-01T00:00:00.123456789", DateTime.fromEpochMillis(123L, 456789).toString());

		Assert.assertEquals("1969-12-31T23:59:59.877", DateTime.fromEpochMillis(-123L).toString());
		Assert.assertEquals("1969-12-31T23:59:59.877456789", DateTime.fromEpochMillis(-123L, 456789).toString());

		LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals("1969-01-02T00:00:00.123456789", DateTime.fromLocalDateTime(ldt).toString());
	}
}
