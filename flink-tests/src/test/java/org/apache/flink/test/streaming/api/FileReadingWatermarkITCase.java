/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/**
 * Tests that watermarks are emitted while file is being read, particularly the last split.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-19109">FLINK-19109</a>
 */
public class FileReadingWatermarkITCase {
	private static final String NUM_WATERMARKS_ACC_NAME = "numWatermarks";
	private static final int FILE_SIZE_LINES = 100_000;
	private static final int WATERMARK_INTERVAL_MILLIS = 10;
	private static final int MIN_EXPECTED_WATERMARKS = 5;

	@Test
	public void testWatermarkEmissionWithChaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(WATERMARK_INTERVAL_MILLIS);

		checkState(env.isChainingEnabled());

		env
			.readTextFile(getSourceFile().getAbsolutePath())
			.assignTimestampsAndWatermarks(getExtractorAssigner())
			.addSink(getWatermarkCounter());

		JobExecutionResult result = env.execute();

		int actual = result.getAccumulatorResult(NUM_WATERMARKS_ACC_NAME);

		assertTrue("too few watermarks emitted: " + actual, actual >= MIN_EXPECTED_WATERMARKS);
	}

	private File getSourceFile() throws IOException {
		File file = File.createTempFile(UUID.randomUUID().toString(), null);
		try (PrintWriter printWriter = new PrintWriter(file)) {
			for (int i = 0; i < FILE_SIZE_LINES; i++) {
				printWriter.println(i);
			}
		}
		file.deleteOnExit();
		return file;
	}

	private static BoundedOutOfOrdernessTimestampExtractor<String> getExtractorAssigner() {
		return new BoundedOutOfOrdernessTimestampExtractor<String>(Time.hours(1)) {
			private final long started = System.currentTimeMillis();

			@Override
			public long extractTimestamp(String line) {
				return started + Long.parseLong(line);
			}
		};
	}

	private static SinkFunction<String> getWatermarkCounter() {
		return new RichSinkFunction<String>() {
			private final IntCounter numWatermarks = new IntCounter();
			private long lastWatermark = -1;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				getRuntimeContext().addAccumulator(NUM_WATERMARKS_ACC_NAME, numWatermarks);
			}

			@Override
			public void close() throws Exception {
				super.close();
			}

			@Override
			public void invoke(String value, SinkFunction.Context context) {
				if (context.currentWatermark() != lastWatermark) {
					lastWatermark = context.currentWatermark();
					numWatermarks.add(1);
				}
			}
		};
	}
}
