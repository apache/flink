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

import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Base class for task-input throughput benchmarks.
 */
public abstract class StreamTaskInputThroughputBenchmarkBase {

	protected StreamTaskInputBenchmarkEnvironment environment;

	private AbstractTaskInputProcessorThread processorThread;

	private long totalRecordsPerIteration = -1;

	public long executeBenchmark() throws Exception {
		return executeBenchmark(Long.MAX_VALUE);
	}

	/**
	 * Executes the throughput benchmark.
	 */
	public long executeBenchmark(long timeout) throws Exception {
		processorThread.setRecordsToProcess(totalRecordsPerIteration);

		return processorThread.getProcessedRecords().get(timeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Initializes the throughput benchmark with the given parameters.
	 */
	public void setUp(
		int numInputGates1,
		int numInputGates2,
		int numChannels1PerGate,
		int numChannels2PerGate,
		long numRecords1PerChannel,
		long numRecords2PerChannel,
		SummingLongStreamOperator streamOperator)	throws Exception {

		long numRecords1PerIteration = numInputGates1 * numChannels1PerGate * numRecords1PerChannel;
		long numRecords2PerIteration = numInputGates2 * numChannels2PerGate * numRecords2PerChannel;

		totalRecordsPerIteration = numRecords1PerIteration + numRecords2PerIteration;

		environment = new StreamTaskInputBenchmarkEnvironment();
		environment.setUp();

		processorThread = createProcessorThread(
			numInputGates1,
			numInputGates2,
			numChannels1PerGate,
			numChannels2PerGate,
			numRecords1PerChannel,
			numRecords2PerChannel,
			1,
			2,
			streamOperator);

		streamOperator.open(processorThread, numRecords1PerIteration, numRecords2PerIteration);
		processorThread.start();
	}

	/**
	 * Shuts down a benchmark previously set up via {@link #setUp}.
	 */
	public void tearDown() {
		processorThread.shutdown();
		environment.tearDown();
	}

	protected abstract AbstractTaskInputProcessorThread createProcessorThread(
		int numInputGates1,
		int numInputGates2,
		int numChannels1PerGate,
		int numChannels2PerGate,
		long numRecords1PerChannel,
		long numRecords2PerChannel,
		long inputValue1,
		long inputValue2,
		SummingLongStreamOperator streamOperator) throws IOException;

	// ------------------------------------------------------------------------
	//  Mocks
	// ------------------------------------------------------------------------

	static final class SequentialReadingStreamOperator extends SummingLongStreamOperator implements InputSelectable {

		private long currentIterationRecords1 = 0;
		private long currentIterationRecords2 = 0;

		@Override
		public void processElement1(StreamRecord<Long> element) throws Exception {
			super.processElement1(element);

			currentIterationRecords1++;
			if (currentIterationRecords1 == numRecords1PerIteration) {
				// switch to read another input
				inputSelection = InputSelection.SECOND;
				currentIterationRecords1 = 0;
			}
		}

		@Override
		public void processElement2(StreamRecord<Long> element) throws Exception {
			super.processElement2(element);

			currentIterationRecords2++;
			if (currentIterationRecords2 == numRecords2PerIteration) {
				// switch to read another input
				inputSelection = InputSelection.FIRST;
				currentIterationRecords2 = 0;
			}
		}

		@Override
		public InputSelection nextSelection() {
			return inputSelection;
		}
	}
}
