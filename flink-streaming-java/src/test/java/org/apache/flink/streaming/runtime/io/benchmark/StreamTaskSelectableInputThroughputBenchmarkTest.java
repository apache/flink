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

import org.apache.flink.streaming.runtime.io.benchmark.StreamTaskInputThroughputBenchmarkBase.SequentialReadingStreamOperator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Tests for various task-input (selectable) benchmarks based on {@link StreamTaskSelectableInputThroughputBenchmark}.
 */
public class StreamTaskSelectableInputThroughputBenchmarkTest {

	private static final long DEFAULT_TIMEOUT = 10_000L;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	protected static StreamTaskSelectableInputThroughputBenchmark createBenchmark() {
		return new StreamTaskSelectableInputThroughputBenchmark();
	}

	@Test
	public void singleGateAndSingleChannelBenchmark() throws Exception {
		testBase(
			1,
			1,
			1,
			1,
			1,
			1_0000,
			200,
			new SummingLongStreamOperator());
	}

	@Test
	public void anyReadingBenchmark() throws Exception {
		testBase(
			2,
			2,
			2,
			1,
			1,
			1_0000,
			2_0000,
			new SummingLongStreamOperator());
	}

	@Test
	public void sequentialReadingBenchmark() throws Exception {
		testBase(
			2,
			2,
			3,
			4,
			5,
			1_000,
			500,
			new SequentialReadingStreamOperator());
	}

	private static void testBase(
		int numIterations,
		int numInputGates1,
		int numInputGates2,
		int numChannels1PerGate,
		int numChannels2PerGate,
		long numRecords1PerChannel,
		long numRecords2PerChannel,
		SummingLongStreamOperator streamOperator) throws Exception {

		StreamTaskSelectableInputThroughputBenchmark benchmark = createBenchmark();
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

	static void verifyResult(
		SummingLongStreamOperator streamOperator,
		long expectedRecordNumber1,
		long expectedRecordNumber2,
		long expectedSum1,
		long expectedSum2) {

		// check the number of input records
		long actualRecordNumber1 = streamOperator.getRecordNumber1();
		long actualRecordNumber2 = streamOperator.getRecordNumber2();

		checkState(expectedRecordNumber1 == actualRecordNumber1,
			"[input1] expected records: " + expectedRecordNumber1 + ", actual records: " + actualRecordNumber1);
		checkState(expectedRecordNumber2 == actualRecordNumber2,
			"[input2] expected records: " + expectedRecordNumber2 + ", actual records: " + actualRecordNumber2);

		// check the sum of input records
		long actualSum1 = streamOperator.getSum1();
		long actualSum2 = streamOperator.getSum2();

		checkState(expectedSum1 == actualSum1,
			"[input1] expected sum: " + expectedSum1 + ", actual sum: " + actualSum1);
		checkState(expectedSum2 == actualSum2,
			"[input2] expected sum: " + expectedSum2 + ", actual sum: " + actualSum2);
	}
}
