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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end test for the bucketing sink.
 *
 * <p>Contains a simple stateful job that emits into buckets per key.
 *
 * <p>The stream is bounded and will complete after about a minute.
 * The result is always constant.
 *
 * <p>Parameters:
 * -outputPath Sets the path to where the result data is written.
 */
public class BucketingSinkTestProgram {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String outputPath = params.getRequired("outputPath");

		StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(
				Integer.MAX_VALUE,
				Time.of(10, TimeUnit.SECONDS)
			));
		sEnv.enableCheckpointing(4000);

		final int idlenessMs = 10;

		// define bucketing sink to emit the result
		BucketingSink<Tuple4<Integer, Long, Integer, String>> sink = new BucketingSink<Tuple4<Integer, Long, Integer, String>>(outputPath)
				.setBucketer(new KeyBucketer())
				.setBatchSize(Long.MAX_VALUE)
				.setBatchRolloverInterval(Long.MAX_VALUE)
				.setInactiveBucketCheckInterval(50)
				.setInactiveBucketThreshold(1000);

		// generate data, shuffle, perform stateful operation, sink
		sEnv.addSource(new Generator(10, idlenessMs, 60))
			.keyBy(0)
			.map(new SubtractingMapper(-1L * idlenessMs))
			.addSink(sink);

		sEnv.execute();
	}

	/**
	 * Use first field for buckets.
	 */
	public static class KeyBucketer implements Bucketer<Tuple4<Integer, Long, Integer, String>> {

		@Override
		public Path getBucketPath(Clock clock, Path basePath, Tuple4<Integer, Long, Integer, String> element) {
			return basePath.suffix(String.valueOf(element.f0));
		}
	}

	/**
	 * Subtracts the timestamp of the previous element from the current element.
	 */
	public static class SubtractingMapper extends RichMapFunction<Tuple3<Integer, Long, String>, Tuple4<Integer, Long, Integer, String>> {

		private final long initialValue;

		private ValueState<Integer> counter;
		private ValueState<Long> last;

		public SubtractingMapper(long initialValue) {
			this.initialValue = initialValue;
		}

		@Override
		public void open(Configuration parameters) {
			counter = getRuntimeContext().getState(new ValueStateDescriptor<>("counter", Types.INT));
			last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Types.LONG));
		}

		@Override
		public Tuple4<Integer, Long, Integer, String> map(Tuple3<Integer, Long, String> value) throws IOException {
			// update counter
			Integer counterValue = counter.value();
			if (counterValue == null) {
				counterValue = 0;
			}
			counter.update(counterValue + 1);

			// save last value
			Long lastValue = last.value();
			if (lastValue == null) {
				lastValue = initialValue;
			}
			last.update(value.f1);

			return Tuple4.of(value.f0, value.f1 - lastValue, counterValue, value.f2);
		}
	}

	/**
	 * Data-generating source function.
	 */
	public static class Generator implements SourceFunction<Tuple3<Integer, Long, String>>, CheckpointedFunction {

		private final int numKeys;
		private final int idlenessMs;
		private final int durationMs;

		private long ms = 0;
		private volatile boolean canceled = false;

		private ListState<Long> state = null;

		public Generator(int numKeys, int idlenessMs, int durationSeconds) {
			this.numKeys = numKeys;
			this.idlenessMs = idlenessMs;
			this.durationMs = durationSeconds * 1000;
		}

		@Override
		public void run(SourceContext<Tuple3<Integer, Long, String>> ctx) throws Exception {
			while (ms < durationMs && !canceled) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Tuple3.of(i, ms, "Some payload..."));
					}
					ms += idlenessMs;
				}
				Thread.sleep(idlenessMs);
			}

			while (!canceled) {
				Thread.sleep(50);
			}
		}

		@Override
		public void cancel() {
			canceled = true;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(
					new ListStateDescriptor<Long>("state", LongSerializer.INSTANCE));

			for (Long l : state.get()) {
				ms += l;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(ms);
		}
	}
}
