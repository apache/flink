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
 * Test for {@link Timestamp}.
 */
public class TimestampTest {

	@Test
	public void testNormal() {
		// From long to Timestamp and vice versa
		Assert.assertEquals(1123L, Timestamp.fromLong(1123L, 3).getMillisecond());
		Assert.assertEquals(1120L, Timestamp.fromLong(1123L, 2).getMillisecond());
		Assert.assertEquals(1100L, Timestamp.fromLong(1123L, 1).getMillisecond());
		Assert.assertEquals(1000L, Timestamp.fromLong(1123L, 0).getMillisecond());

		Assert.assertEquals(-1123L, Timestamp.fromLong(-1123L, 3).getMillisecond());
		Assert.assertEquals(-1120L, Timestamp.fromLong(-1123L, 2).getMillisecond());
		Assert.assertEquals(-1100L, Timestamp.fromLong(-1123L, 1).getMillisecond());
		Assert.assertEquals(-1000L, Timestamp.fromLong(-1123L, 0).getMillisecond());

		// From Timestamp to Timestamp and vice versa
		java.sql.Timestamp t19 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		java.sql.Timestamp t16 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456");
		java.sql.Timestamp t13 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123");
		java.sql.Timestamp t10 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00");

		Assert.assertEquals(t19, Timestamp.fromTimestamp(t19, 9).toTimestamp());
		Assert.assertEquals(t16, Timestamp.fromTimestamp(t19, 6).toTimestamp());
		Assert.assertEquals(t13, Timestamp.fromTimestamp(t19, 3).toTimestamp());
		Assert.assertEquals(t10, Timestamp.fromTimestamp(t19, 0).toTimestamp());

		java.sql.Timestamp t2 = java.sql.Timestamp.valueOf("1979-01-02 00:00:00.123456");
		Assert.assertEquals(t2, Timestamp.fromTimestamp(t2, 6).toTimestamp());

		java.sql.Timestamp t3 = new java.sql.Timestamp(1572333940000L);
		Assert.assertEquals(t3, Timestamp.fromTimestamp(t3, 0).toTimestamp());

		// From LocalDateTime to Timestamp and vice versa
		LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
		LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
		LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

		Assert.assertEquals(ldt19, Timestamp.fromLocalDateTime(ldt19, 9).toLocalDateTime());
		Assert.assertEquals(ldt16, Timestamp.fromLocalDateTime(ldt19, 6).toLocalDateTime());
		Assert.assertEquals(ldt13, Timestamp.fromLocalDateTime(ldt19, 3).toLocalDateTime());
		Assert.assertEquals(ldt10, Timestamp.fromLocalDateTime(ldt19, 0).toLocalDateTime());

		LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
		Assert.assertEquals(ldt2, Timestamp.fromLocalDateTime(ldt2, 9).toLocalDateTime());

		LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals(ldt3, Timestamp.fromLocalDateTime(ldt3, 9).toLocalDateTime());

		LocalDateTime ldt4 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		java.sql.Timestamp t4 = java.sql.Timestamp.valueOf(ldt4);
		Assert.assertEquals(Timestamp.fromLocalDateTime(ldt4, 9), Timestamp.fromTimestamp(t4, 9));
	}

	@Test
	public void testToString() {

		java.sql.Timestamp t = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		Assert.assertEquals("1969-01-02T00:00:00.123456789", Timestamp.fromTimestamp(t, 9).toString());
		Assert.assertEquals("1969-01-02T00:00:00.123456", Timestamp.fromTimestamp(t, 6).toString());
		Assert.assertEquals("1969-01-02T00:00:00.123", Timestamp.fromTimestamp(t, 3).toString());
		Assert.assertEquals("1969-01-02T00:00", Timestamp.fromTimestamp(t, 0).toString());

		Assert.assertEquals("1970-01-01T00:00:00.123", Timestamp.fromLong(123L, 9).toString());
		Assert.assertEquals("1970-01-01T00:00:00.123", Timestamp.fromLong(123L, 3).toString());
		Assert.assertEquals("1970-01-01T00:00", Timestamp.fromLong(123L, 0).toString());

		Assert.assertEquals("1969-12-31T23:59:59.877", Timestamp.fromLong(-123L, 3).toString());

		// 123 milliseconds before 1970-01-01 00:00:00, ignore milliseconds and retain 0 second
		Assert.assertEquals("1970-01-01T00:00", Timestamp.fromLong(-123L, 0).toString());

		// 1123 milliseconds before 1970-01-01 00:00:00, ignore milliseconds and retain 1 second
		Assert.assertEquals("1969-12-31T23:59:59", Timestamp.fromLong(-1123L, 0).toString());

		LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals("1969-01-02T00:00:00.123456789", Timestamp.fromLocalDateTime(ldt, 9).toString());
		Assert.assertEquals("1969-01-02T00:00:00.123456", Timestamp.fromLocalDateTime(ldt, 6).toString());
		Assert.assertEquals("1969-01-02T00:00:00.123", Timestamp.fromLocalDateTime(ldt, 3).toString());
		Assert.assertEquals("1969-01-02T00:00", Timestamp.fromLocalDateTime(ldt, 0).toString());
	}
}
