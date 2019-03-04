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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Collections;
import java.util.List;

/**
 * This is a periodic streaming job that runs for CLI testing purposes.
 *
 * <p>The stream is bounded and will complete after the specified duration.
 *
 * <p>Parameters:
 * -outputPath Sets the path to where the result data is written.
 * -recordsPerSecond Sets the output record frequency.
 * -durationInSecond Sets the running duration of the job.
 * -offsetInSecond Sets the startup delay before processing the first message.
 */
public class PeriodicStreamingJob {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String outputPath = params.getRequired("outputPath");
		int recordsPerSecond = params.getInt("recordsPerSecond", 10);
		int duration = params.getInt("durationInSecond", 60);
		int offset = params.getInt("offsetInSecond", 0);

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		sEnv.enableCheckpointing(4000);
		sEnv.getConfig().setAutoWatermarkInterval(1000);

		// execute a simple pass through program.
		PeriodicSourceGenerator generator = new PeriodicSourceGenerator(
			recordsPerSecond, duration, offset);
		DataStream<Tuple> rows = sEnv.addSource(generator);

		DataStream<Tuple> result = rows
			.keyBy(1)
			.timeWindow(Time.seconds(5))
			.sum(0);

		result.writeAsText(outputPath + "/result.txt", FileSystem.WriteMode.OVERWRITE)
			.setParallelism(1);

		sEnv.execute();
	}

	/**
	 * Data-generating source function.
	 */
	public static class PeriodicSourceGenerator implements SourceFunction<Tuple>, ResultTypeQueryable<Tuple>, ListCheckpointed<Long> {
		private final int sleepMs;
		private final int durationMs;
		private final int offsetSeconds;
		private long ms = 0;

		public PeriodicSourceGenerator(float rowsPerSecond, int durationSeconds, int offsetSeconds) {
			this.durationMs = durationSeconds * 1000;
			this.sleepMs = (int) (1000 / rowsPerSecond);
			this.offsetSeconds = offsetSeconds;
		}

		@Override
		public void run(SourceContext<Tuple> ctx) throws Exception {
			long offsetMs = offsetSeconds * 1000L;

			while (ms < durationMs) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(new Tuple2<>(ms + offsetMs, "key"));
				}
				ms += sleepMs;
				Thread.sleep(sleepMs);
			}
		}

		@Override
		public void cancel() {
			// nothing to do
		}

		@Override
		public TypeInformation<Tuple> getProducedType() {
			return Types.TUPLE(Types.LONG, Types.STRING);
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) {
			return Collections.singletonList(ms);
		}

		@Override
		public void restoreState(List<Long> state) {
			for (Long l : state) {
				ms += l;
			}
		}
	}
}
