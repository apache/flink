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

package org.apache.flink.benchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * Benchmark for the simplest Flink application. It executes simple chain of:
 * LongSource -> simple map function -> windowAll -> simple reduce function -> print
 */
public class EventCountBenchmark extends BenchmarkBase {

	@Benchmark
	public void benchmarkCount(Context context) throws Exception {

		StreamExecutionEnvironment env = context.getEnv();
		DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_INVOCATION));

		source
				.map(new MultiplyByTwo())
				.windowAll(GlobalWindows.create())
				.reduce(new SumReduce())
				.print();

		env.execute();
	}

	public static class MultiplyByTwo implements MapFunction<Long, Long> {
		@Override
		public Long map(Long value) throws Exception {
			return value * 2;
		}
	}

	public static class SumReduce implements ReduceFunction<Long> {
		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}
	}

	public static class LongSource extends RichParallelSourceFunction<Long> {

		private volatile boolean running = true;
		private long maxValue;

		public LongSource(long maxValue) {
			this.maxValue = maxValue;
		}

		@Override
		public void run(SourceFunction.SourceContext<Long> ctx) throws Exception {
			long counter = 0;

			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(counter);
					counter++;
					if (counter >= maxValue) {
						cancel();
					}
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static void main(String[] args)
		throws RunnerException {
		Options options = new OptionsBuilder()
			.verbosity(VerboseMode.NORMAL)
			.include(".*" + EventCountBenchmark.class.getSimpleName() + ".*")
			.build();

		new Runner(options).run();
	}
}
