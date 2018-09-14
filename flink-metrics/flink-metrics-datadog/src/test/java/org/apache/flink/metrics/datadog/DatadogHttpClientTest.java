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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the DatadogHttpClient.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DMetric.class)
public class DatadogHttpClientTest {

	private static List<String> tags = Arrays.asList("tag1", "tag2");

	private static final long MOCKED_SYSTEM_MILLIS = 123L;

	@Before
	public void mockSystemMillis() {
		PowerMockito.mockStatic(DMetric.class);
		PowerMockito.when(DMetric.getUnixEpochTimestamp()).thenReturn(MOCKED_SYSTEM_MILLIS);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testClientWithEmptyKey() {
		new DatadogHttpClient("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testClientWithNullKey() {
		new DatadogHttpClient(null);
	}

	@Test
	public void serializeGauge() throws JsonProcessingException {

		DGauge g = new DGauge(new Gauge<Number>() {
			@Override
			public Number getValue() {
				return 1;
			}
		}, "testCounter", "localhost", tags);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"gauge\",\"host\":\"localhost\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(g));
	}

	@Test
	public void serializeGaugeWithoutHost() throws JsonProcessingException {

		DGauge g = new DGauge(new Gauge<Number>() {
			@Override
			public Number getValue() {
				return 1;
			}
		}, "testCounter", null, tags);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"gauge\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(g));
	}

	@Test
	public void serializeCounter() throws JsonProcessingException {
		DCounter c = new DCounter(new Counter() {
			@Override
			public void inc() {}

			@Override
			public void inc(long n) {}

			@Override
			public void dec() {}

			@Override
			public void dec(long n) {}

			@Override
			public long getCount() {
				return 1;
			}
		}, "testCounter", "localhost", tags);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"counter\",\"host\":\"localhost\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(c));
	}

	@Test
	public void serializeCounterWithoutHost() throws JsonProcessingException {
		DCounter c = new DCounter(new Counter() {
			@Override
			public void inc() {}

			@Override
			public void inc(long n) {}

			@Override
			public void dec() {}

			@Override
			public void dec(long n) {}

			@Override
			public long getCount() {
				return 1;
			}
		}, "testCounter", null, tags);

		assertEquals(
			"{\"metric\":\"testCounter\",\"type\":\"counter\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1]]}",
			DatadogHttpClient.serialize(c));
	}

	@Test
	public void serializeMeter() throws JsonProcessingException {

		DMeter m = new DMeter(new Meter() {
			@Override
			public void markEvent() {}

			@Override
			public void markEvent(long n) {}

			@Override
			public double getRate() {
				return 1;
			}

			@Override
			public long getCount() {
				return 0;
			}
		}, "testMeter", "localhost", tags);

		assertEquals(
			"{\"metric\":\"testMeter\",\"type\":\"gauge\",\"host\":\"localhost\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1.0]]}",
			DatadogHttpClient.serialize(m));
	}

	@Test
	public void serializeMeterWithoutHost() throws JsonProcessingException {

		DMeter m = new DMeter(new Meter() {
			@Override
			public void markEvent() {}

			@Override
			public void markEvent(long n) {}

			@Override
			public double getRate() {
				return 1;
			}

			@Override
			public long getCount() {
				return 0;
			}
		}, "testMeter", null, tags);

		assertEquals(
			"{\"metric\":\"testMeter\",\"type\":\"gauge\",\"tags\":[\"tag1\",\"tag2\"],\"points\":[[123,1.0]]}",
			DatadogHttpClient.serialize(m));
	}
}
