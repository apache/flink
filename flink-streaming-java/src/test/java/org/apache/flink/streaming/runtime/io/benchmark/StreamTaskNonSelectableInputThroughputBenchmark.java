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

import org.apache.flink.runtime.io.network.partition.consumer.IterableInputChannel;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.apache.flink.streaming.runtime.io.benchmark.StreamTaskInputBenchmarkEnvironment.ProcessorAndChannels;

import java.io.IOException;
import java.util.List;

/**
 * Task-input (non-selectable) throughput benchmarks executed by the external
 * <a href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamTaskNonSelectableInputThroughputBenchmark extends StreamTaskInputThroughputBenchmarkBase {

	public void setUp(int numInputGates, int numChannelsPerGate, long numRecordsPerChannel) throws Exception {
		setUp(
			numInputGates, numInputGates,
			numChannelsPerGate, numChannelsPerGate,
			numRecordsPerChannel, numRecordsPerChannel,
			new SummingLongStreamOperator());
	}

	@Override
	protected AbstractTaskInputProcessorThread createProcessorThread(
		int numInputGates1,
		int numInputGates2,
		int numChannels1PerGate,
		int numChannels2PerGate,
		long numRecords1PerChannel,
		long numRecords2PerChannel,
		long inputValue1,
		long inputValue2,
		SummingLongStreamOperator streamOperator) throws IOException {

		ProcessorAndChannels<StreamTwoInputProcessor<?, ?>, IterableInputChannel> processorAndChannels =
			environment.createTwoInputProcessor(
				numInputGates1,
				numInputGates2,
				numChannels1PerGate,
				numChannels2PerGate,
				numRecords1PerChannel,
				numRecords2PerChannel,
				inputValue1,
				inputValue2,
				streamOperator);

		return new StreamTwoInputProcessorThread(
			processorAndChannels.processor(),
			processorAndChannels.channels(),
			streamOperator);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class StreamTwoInputProcessorThread extends AbstractTaskInputProcessorThread {

		private volatile boolean running = true;

		private final StreamTwoInputProcessor<?, ?> inputProcessor;

		private final SummingLongStreamOperator streamOperator;

		public StreamTwoInputProcessorThread(
			StreamTwoInputProcessor<?, ?> inputProcessor,
			List<IterableInputChannel> inputChannels,
			SummingLongStreamOperator streamOperator) {

			super(inputChannels);

			this.inputProcessor = inputProcessor;
			this.streamOperator = streamOperator;
		}

		@Override
		protected long processTaskInput(long records) throws Exception {
			// see {@link TwoInputStreamTask#run()}
			final StreamTwoInputProcessor<?, ?> inputProcessor = this.inputProcessor;

			while (running && inputProcessor.processInput()) {
				// all the work happens in the "processInput" method
			}

			return streamOperator.getRecordNumber1() + streamOperator.getRecordNumber2();
		}

		@Override
		protected void cleanup() throws IOException {
			inputProcessor.cleanup();
		}

		@Override
		protected void startIteration() {
			running = true;
		}

		@Override
		protected void endIteration() {
			running = false;
		}
	}
}
