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

import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Test for {@link PreciseTimestamp}.
 */
public class PreciseTimestampTest {

	@Test
	public void testNormal() {
		// From long to PreciseTimestamp and vice versa
		Assert.assertEquals(1123L, PreciseTimestamp.fromLong(1123L, 3).toLong());
		Assert.assertEquals(1120L, PreciseTimestamp.fromLong(1123L, 2).toLong());
		Assert.assertEquals(1100L, PreciseTimestamp.fromLong(1123L, 1).toLong());
		Assert.assertEquals(1000L, PreciseTimestamp.fromLong(1123L, 0).toLong());

		Assert.assertEquals(-1123L, PreciseTimestamp.fromLong(-1123L, 3).toLong());
		Assert.assertEquals(-1120L, PreciseTimestamp.fromLong(-1123L, 2).toLong());
		Assert.assertEquals(-1100L, PreciseTimestamp.fromLong(-1123L, 1).toLong());
		Assert.assertEquals(-1000L, PreciseTimestamp.fromLong(-1123L, 0).toLong());

		// From Timestamp to PreciseTimestamp and vice versa
		Timestamp t19 = Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		Timestamp t16 = Timestamp.valueOf("1969-01-02 00:00:00.123456");
		Timestamp t13 = Timestamp.valueOf("1969-01-02 00:00:00.123");
		Timestamp t10 = Timestamp.valueOf("1969-01-02 00:00:00");

		Assert.assertEquals(t19, PreciseTimestamp.fromTimestamp(t19, 9).toTimestamp());
		Assert.assertEquals(t16, PreciseTimestamp.fromTimestamp(t19, 6).toTimestamp());
		Assert.assertEquals(t13, PreciseTimestamp.fromTimestamp(t19, 3).toTimestamp());
		Assert.assertEquals(t10, PreciseTimestamp.fromTimestamp(t19, 0).toTimestamp());

		Timestamp t2 = Timestamp.valueOf("1979-01-02 00:00:00.123456");
		Assert.assertEquals(t2, PreciseTimestamp.fromTimestamp(t2, 6).toTimestamp());

		Timestamp t3 = new Timestamp(1572333940000L);
		Assert.assertEquals(t3, PreciseTimestamp.fromTimestamp(t3, 0).toTimestamp());

		// From LocalDateTime to PreciseTimestamp and vice versa
		LocalDateTime ldt19 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		LocalDateTime ldt16 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456000);
		LocalDateTime ldt13 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123000000);
		LocalDateTime ldt10 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);

		Assert.assertEquals(ldt19, PreciseTimestamp.fromLocalDateTime(ldt19, 9).toLocalDateTime());
		Assert.assertEquals(ldt16, PreciseTimestamp.fromLocalDateTime(ldt19, 6).toLocalDateTime());
		Assert.assertEquals(ldt13, PreciseTimestamp.fromLocalDateTime(ldt19, 3).toLocalDateTime());
		Assert.assertEquals(ldt10, PreciseTimestamp.fromLocalDateTime(ldt19, 0).toLocalDateTime());

		LocalDateTime ldt2 = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 0);
		Assert.assertEquals(ldt2, PreciseTimestamp.fromLocalDateTime(ldt2, 9).toLocalDateTime());

		LocalDateTime ldt3 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals(ldt3, PreciseTimestamp.fromLocalDateTime(ldt3, 9).toLocalDateTime());

		LocalDateTime ldt4 = LocalDateTime.of(1989, 1, 2, 0, 0, 0, 123456789);
		Timestamp t4 = Timestamp.valueOf(ldt4);
		Assert.assertEquals(PreciseTimestamp.fromLocalDateTime(ldt4, 9), PreciseTimestamp.fromTimestamp(t4, 9));
	}

	@Test
	public void testToString() {
		Timestamp t = Timestamp.valueOf("1969-01-02 00:00:00.123456789");
		Assert.assertEquals("1969-01-02 00:00:00.123456789", PreciseTimestamp.fromTimestamp(t, 9).toString());
		Assert.assertEquals("1969-01-02 00:00:00.123456", PreciseTimestamp.fromTimestamp(t, 6).toString());
		Assert.assertEquals("1969-01-02 00:00:00.123", PreciseTimestamp.fromTimestamp(t, 3).toString());
		Assert.assertEquals("1969-01-02 00:00:00", PreciseTimestamp.fromTimestamp(t, 0).toString());

		Assert.assertEquals("1970-01-01 00:00:00.123000000", PreciseTimestamp.fromLong(123L, 9).toString());
		Assert.assertEquals("1970-01-01 00:00:00.123", PreciseTimestamp.fromLong(123L, 3).toString());
		Assert.assertEquals("1970-01-01 00:00:00", PreciseTimestamp.fromLong(123L, 0).toString());
		Assert.assertEquals("1969-12-31 23:59:59.877", PreciseTimestamp.fromLong(-123L, 3).toString());

		// 123 milliseconds before 1970-01-01 00:00:00, ignore milliseconds and retain 0 second
		Assert.assertEquals("1970-01-01 00:00:00", PreciseTimestamp.fromLong(-123L, 0).toString());

		// 1123 milliseconds before 1970-01-01 00:00:00, ignore milliseconds and retain 1 second
		Assert.assertEquals("1969-12-31 23:59:59", PreciseTimestamp.fromLong(-1123L, 0).toString());

		LocalDateTime ldt = LocalDateTime.of(1969, 1, 2, 0, 0, 0, 123456789);
		Assert.assertEquals("1969-01-02 00:00:00.123456789", PreciseTimestamp.fromLocalDateTime(ldt, 9).toString());
		Assert.assertEquals("1969-01-02 00:00:00.123456", PreciseTimestamp.fromLocalDateTime(ldt, 6).toString());
		Assert.assertEquals("1969-01-02 00:00:00.123", PreciseTimestamp.fromLocalDateTime(ldt, 3).toString());
		Assert.assertEquals("1969-01-02 00:00:00", PreciseTimestamp.fromLocalDateTime(ldt, 0).toString());
	}
}
