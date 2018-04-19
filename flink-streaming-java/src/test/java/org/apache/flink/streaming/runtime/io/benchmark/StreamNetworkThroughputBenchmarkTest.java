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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.junit.Test;

/**
 * Tests for various network benchmarks based on {@link StreamNetworkThroughputBenchmark}.
 */
public class StreamNetworkThroughputBenchmarkTest {
	@Test
	public void pointToPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark benchmark = new StreamNetworkThroughputBenchmark();
		benchmark.setUp(1, 1, 100);
		try {
			benchmark.executeBenchmark(1_000);
		}
		finally {
			benchmark.tearDown();
		}
	}

	@Test
	public void largeLocalMode() throws Exception {
		StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
		env.setUp(4, 10, 100, true);
		env.executeBenchmark(10_000_000);
		env.tearDown();
	}

	@Test
	public void largeRemoteMode() throws Exception {
		StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
		env.setUp(4, 10, 100, false);
		env.executeBenchmark(10_000_000);
		env.tearDown();
	}

	@Test
	public void largeRemoteAlwaysFlush() throws Exception {
		StreamNetworkThroughputBenchmark env = new StreamNetworkThroughputBenchmark();
		env.setUp(1, 1, 0, false);
		env.executeBenchmark(1_000_000);
		env.tearDown();
	}

	@Test
	public void pointToMultiPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark benchmark = new StreamNetworkThroughputBenchmark();
		benchmark.setUp(1, 100, 100);
		try {
			benchmark.executeBenchmark(1_000);
		}
		finally {
			benchmark.tearDown();
		}
	}

	@Test
	public void multiPointToPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark benchmark = new StreamNetworkThroughputBenchmark();
		benchmark.setUp(4, 1, 100);
		try {
			benchmark.executeBenchmark(1_000);
		}
		finally {
			benchmark.tearDown();
		}
	}

	@Test
	public void multiPointToMultiPointBenchmark() throws Exception {
		StreamNetworkThroughputBenchmark benchmark = new StreamNetworkThroughputBenchmark();
		benchmark.setUp(4, 100, 100);
		try {
			benchmark.executeBenchmark(1_000);
		}
		finally {
			benchmark.tearDown();
		}
	}
}
