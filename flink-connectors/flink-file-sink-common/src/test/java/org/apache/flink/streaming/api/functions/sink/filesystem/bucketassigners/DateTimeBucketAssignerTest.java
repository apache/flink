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

package org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners;

import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DateTimeBucketAssigner}.
 */
public class DateTimeBucketAssignerTest {
	private static final long TEST_TIME_IN_MILLIS = 1533363082011L;

	private static final MockedContext mockedContext = new MockedContext();

	@Test
	public void testGetBucketPathWithSpecifiedTimezone() {
		DateTimeBucketAssigner bucketAssigner = new DateTimeBucketAssigner(ZoneId.of("America/Los_Angeles"));

		assertEquals("2018-08-03--23", bucketAssigner.getBucketId(null, mockedContext));
	}

	@Test
	public void testGetBucketPathWithSpecifiedFormatString() {
		DateTimeBucketAssigner bucketAssigner = new DateTimeBucketAssigner("yyyy-MM-dd-HH", ZoneId.of("America/Los_Angeles"));

		assertEquals("2018-08-03-23", bucketAssigner.getBucketId(null, mockedContext));
	}

	private static class MockedContext implements BucketAssigner.Context {
		@Override
		public long currentProcessingTime() {
			return TEST_TIME_IN_MILLIS;
		}

		@Override
		public long currentWatermark() {
			throw new UnsupportedOperationException();
		}

		@Nullable
		@Override
		public Long timestamp() {
			return null;
		}
	}
}
