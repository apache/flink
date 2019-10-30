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
		Assert.assertEquals(1123L, Timestamp.fromLong(1123L).getMillisecond());
		Assert.assertEquals(-1123L, Timestamp.fromLong(-1123L).getMillisecond());

		// From Timestamp to Timestamp and vice versa
		java.sql.Timestamp t19 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		java.sql.Timestamp t16 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456");
		java.sql.Timestamp t13 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123");
		java.sql.Timestamp t10 = java.sql.Timestamp.valueOf("1969-01-02 00:00:00");

		Assert.assertEquals(t19, Timestamp.fromTimestamp(t19).toTimestamp());
		Assert.assertEquals(t16, Timestamp.fromTimestamp(t16).toTimestamp());
		Assert.assertEquals(t13, Timestamp.fromTimestamp(t13).toTimestamp());
		Assert.assertEquals(t10, Timestamp.fromTimestamp(t10).toTimestamp());

		java.sql.Timestamp t2 = java.sql.Timestamp.valueOf("1979-01-02 00:00:00.123456");
		Assert.assertEquals(t2, Timestamp.fromTimestamp(t2).toTimestamp());

		java.sql.Timestamp t3 = new java.sql.Timestamp(1572333940000L);
		Assert.assertEquals(t3, Timestamp.fromTimestamp(t3).toTimestamp());

		// From LocalDateTime to Timestamp and vice versa
		LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
		LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
		LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

		Assert.assertEquals(ldt19, Timestamp.fromLocalDateTime(ldt19).toLocalDateTime());
		Assert.assertEquals(ldt16, Timestamp.fromLocalDateTime(ldt16).toLocalDateTime());
		Assert.assertEquals(ldt13, Timestamp.fromLocalDateTime(ldt13).toLocalDateTime());
		Assert.assertEquals(ldt10, Timestamp.fromLocalDateTime(ldt10).toLocalDateTime());

		LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
		Assert.assertEquals(ldt2, Timestamp.fromLocalDateTime(ldt2).toLocalDateTime());

		LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals(ldt3, Timestamp.fromLocalDateTime(ldt3).toLocalDateTime());

		LocalDateTime ldt4 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		java.sql.Timestamp t4 = java.sql.Timestamp.valueOf(ldt4);
		Assert.assertEquals(Timestamp.fromLocalDateTime(ldt4), Timestamp.fromTimestamp(t4));
	}

	@Test
	public void testToString() {

		java.sql.Timestamp t = java.sql.Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		Assert.assertEquals("1969-01-02T00:00:00.123456789", Timestamp.fromTimestamp(t).toString());

		Assert.assertEquals("1970-01-01T00:00:00.123", Timestamp.fromLong(123L).toString());

		Assert.assertEquals("1969-12-31T23:59:59.877", Timestamp.fromLong(-123L).toString());

		LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals("1969-01-02T00:00:00.123456789", Timestamp.fromLocalDateTime(ldt).toString());
	}
}
