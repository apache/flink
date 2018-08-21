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

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.streaming.connectors.fs.Clock;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DateTimeBucketer}.
 */
public class DateTimeBucketerTest {
	private static final long TEST_TIME_IN_MILLIS = 1533363082011L;
	private static final Path TEST_PATH = new Path("test");

	private static final Clock mockedClock = new MockedClock();

	@Test
	public void testGetBucketPathWithSpecifiedTimezone() {
		DateTimeBucketer bucketer = new DateTimeBucketer(ZoneId.of("America/Los_Angeles"));

		assertEquals(new Path("test/2018-08-03--23"), bucketer.getBucketPath(mockedClock, TEST_PATH, null));
	}

	@Test
	public void testGetBucketPathWithSpecifiedFormatString() {
		DateTimeBucketer bucketer = new DateTimeBucketer("yyyy-MM-dd-HH", ZoneId.of("America/Los_Angeles"));

		assertEquals(new Path("test/2018-08-03-23"), bucketer.getBucketPath(mockedClock, TEST_PATH, null));
	}

	private static class MockedClock implements Clock {

		@Override
		public long currentTimeMillis() {
			return TEST_TIME_IN_MILLIS;
		}
	}
}
