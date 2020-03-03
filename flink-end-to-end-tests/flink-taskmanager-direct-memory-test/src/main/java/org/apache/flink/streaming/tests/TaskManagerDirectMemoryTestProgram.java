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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Test program for taskmanager direct memory consumption.
 */
public class TaskManagerDirectMemoryTestProgram {
	private static ConfigOption<Integer> RUNNING_TIME_IN_SECONDS = ConfigOptions
		.key("test.running_time_in_seconds")
		.defaultValue(120)
		.withDescription("The time to run.");

	private static ConfigOption<Integer> RECORD_LENGTH = ConfigOptions
		.key("test.record_length")
		.defaultValue(2048)
		.withDescription("The length of record.");

	private static ConfigOption<Integer> MAP_PARALLELISM = ConfigOptions
		.key("test.map_parallelism")
		.defaultValue(1)
		.withDescription("The number of map tasks.");

	private static ConfigOption<Integer> REDUCE_PARALLELISM = ConfigOptions
		.key("test.reduce_parallelism")
		.defaultValue(1)
		.withDescription("The number of reduce tasks.");

	public static void main(String[] args) throws Exception {
		// parse the parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		final int runningTimeInSeconds = params.getInt(RUNNING_TIME_IN_SECONDS.key(), RUNNING_TIME_IN_SECONDS.defaultValue());
		final int recordLength = params.getInt(RECORD_LENGTH.key(), RECORD_LENGTH.defaultValue());
		final int mapParallelism = params.getInt(MAP_PARALLELISM.key(), MAP_PARALLELISM.defaultValue());
		final int reduceParallelism = params.getInt(REDUCE_PARALLELISM.key(), REDUCE_PARALLELISM.defaultValue());

		checkArgument(runningTimeInSeconds > 0,
			"The running time in seconds should be positive, but it is {}",
			recordLength);
		checkArgument(recordLength > 0,
			"The record length should be positive, but it is {}",
			recordLength);
		checkArgument(mapParallelism > 0,
			"The number of map tasks should be positive, but it is {}",
			mapParallelism);
		checkArgument(reduceParallelism > 0,
			"The number of reduce tasks should be positve, but it is {}",
			reduceParallelism);

		byte[] bytes = new byte[recordLength];
		for (int i = 0; i < recordLength; ++i) {
			bytes[i] = 'a';
		}
		String str = new String(bytes);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new StringSourceFunction(str, runningTimeInSeconds))
			.setParallelism(mapParallelism)
			.slotSharingGroup("a")
			.rebalance()
			.addSink(new DummySink())
			.setParallelism(reduceParallelism)
			.slotSharingGroup("b");

		// execute program
		env.execute("TaskManager Direct Memory Test");
	}

	public static class StringSourceFunction extends RichParallelSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning;

		private final String str;

		private final long runningTimeInSeconds;

		private transient long stopTime;

		public StringSourceFunction(String str, long runningTimeInSeconds) {
			this.str = str;
			this.runningTimeInSeconds = runningTimeInSeconds;
		}

		@Override
		public void open(Configuration parameters) {
			isRunning = true;
			stopTime = System.nanoTime() + runningTimeInSeconds * 1_000_000_000L;
		}

		@Override
		public void run(SourceContext<String> ctx) {
			while (isRunning && (System.nanoTime() < stopTime)) {
				ctx.collect(str);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class DummySink extends RichSinkFunction<String> {

		@Override
		public void invoke(String value, Context context) throws Exception {
			// Do nothing.
		}
	}
}
