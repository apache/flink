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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.streaming.runtime.io.benchmark.StreamTaskSelectableInputThroughputBenchmarkTest.verifyResult;

/**
 * Tests for various task-input (non-selectable) benchmarks based on {@link StreamTaskNonSelectableInputThroughputBenchmark}.
 */
public class StreamTaskNonSelectableInputThroughputBenchmarkTest {

	private static final long DEFAULT_TIMEOUT = 10_000L;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	protected static StreamTaskNonSelectableInputThroughputBenchmark createBenchmark() {
		return new StreamTaskNonSelectableInputThroughputBenchmark();
	}

	@Test
	public void singleGateAndSingleChannelBenchmark() throws Exception {
		testBase(1, 1, 1, 1, 1, 10_000, 20_000);
	}

	@Test
	public void multiGatesAndMultiChannelsBenchmark() throws Exception {
		testBase(1, 2, 3, 4, 5, 1_000, 500);
	}

	@Test
	public void multiIterationBenchmark() throws Exception {
		testBase(3, 2, 2, 1, 1, 10_000, 20_000);
	}

	private static void testBase(
		int numIterations,
		int numInputGates1,
		int numInputGates2,
		int numChannels1PerGate,
		int numChannels2PerGate,
		long numRecords1PerChannel,
		long numRecords2PerChannel) throws Exception {

		SummingLongStreamOperator streamOperator = new SummingLongStreamOperator();

		StreamTaskNonSelectableInputThroughputBenchmark benchmark = createBenchmark();
		benchmark.setUp(
			numInputGates1,
			numInputGates2,
			numChannels1PerGate,
			numChannels2PerGate,
			numRecords1PerChannel,
			numRecords2PerChannel,
			streamOperator);

		try {
			for (int i = 1; i <= numIterations; i++) {
				benchmark.executeBenchmark(DEFAULT_TIMEOUT);

				// check the result
				long expectedNumberOfInputRecords1 = numInputGates1 * numChannels1PerGate * numRecords1PerChannel * i;
				long expectedNumberOfInputRecords2 = numInputGates2 * numChannels2PerGate * numRecords2PerChannel * i;
				verifyResult(
					streamOperator,
					expectedNumberOfInputRecords1, expectedNumberOfInputRecords2,
					expectedNumberOfInputRecords1 * 1, expectedNumberOfInputRecords2 * 2);
			}
		} finally {
			benchmark.tearDown();
		}
	}
}
