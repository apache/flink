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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.api.common.typeutils.base.LongValueSerializer;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.types.LongValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Tests for various network benchmarks based on {@link StreamNetworkThroughputBenchmark}.
 */
public class StreamNetworkThroughputBenchmarkTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void pointToPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			1,
			1,
			1,
			100,
			1_000,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		try {
			benchmark.executeBenchmark();
		}
		finally {
			benchmark.tearDown();
		}
	}

	@Test
	public void largeLocalMode() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			4,
			1,
			10,
			100,
			10_000_000,
			true,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		benchmark.executeBenchmark();
		benchmark.tearDown();
	}

	@Test
	public void largeRemoteMode() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			4,
			1,
			10,
			100,
			10_000_000,
			false,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		benchmark.executeBenchmark();
		benchmark.tearDown();
	}

	@Test
	public void largeRemoteAlwaysFlush() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			1,
			1,
			1,
			0,
			1_000,
			false,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		benchmark.executeBenchmark();
		benchmark.tearDown();
	}

	@Test
	public void remoteModeInsufficientBuffersSender() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		int writers = 2;
		int channels = 2;

		expectedException.expect(IOException.class);
		expectedException.expectMessage("Insufficient number of network buffers");

		benchmark.setUp(
			writers,
			1,
			channels,
			100,
			1_000,
			false,
			writers * channels - 1,
			writers * channels * TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue(),
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
	}

	@Test
	public void remoteModeInsufficientBuffersReceiver() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		int writers = 2;
		int channels = 2;

		expectedException.expect(IOException.class);
		expectedException.expectMessage("Insufficient number of network buffers");

		benchmark.setUp(
			writers,
			1,
			channels,
			100,
			1_000,
			false,
			writers * channels,
			writers * channels * TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue() - 1,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
	}

	@Test
	public void pointToMultiPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			1,
			1,
			100,
			100,
			1_000,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		try {
			benchmark.executeBenchmark();
		}
		finally {
			benchmark.tearDown();
		}
	}

	@Test
	public void multiPointToPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			4,
			1,
			1,
			100,
			1_000,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		try {
			benchmark.executeBenchmark();
		}
		finally {
			benchmark.tearDown();
		}
	}

	@Test
	public void multiPointToMultiPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark<LongValue> benchmark = new StreamNetworkThroughputBenchmark<>();
		benchmark.setUp(
			4,
			1,
			100,
			100,
			1_000,
			new LongValue[] {new LongValue(0)},
			new LongValue(),
			new LongValueSerializer());
		try {
			benchmark.executeBenchmark();
		}
		finally {
			benchmark.tearDown();
		}
	}
}
