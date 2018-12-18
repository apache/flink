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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A streaming program with a custom reducing KvState.
 *
 * <p>This is used to test proper usage of the user code class loader when
 * disposing savepoints.
 */
public class CustomKvStateProgram {

	public static void main(String[] args) throws Exception {
		final int parallelism = Integer.parseInt(args[0]);
		final String checkpointPath = args[1];
		final int checkpointingInterval = Integer.parseInt(args[2]);
		final String outputPath = args[3];

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(checkpointingInterval);
		env.setStateBackend(new FsStateBackend(checkpointPath));

		DataStream<Integer> source = env.addSource(new InfiniteIntegerSource());
		source
				.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> map(Integer value) throws Exception {
						return new Tuple2<>(ThreadLocalRandom.current().nextInt(parallelism), value);
					}
				})
				.keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
						return value.f0;
					}
				}).flatMap(new ReducingStateFlatMap()).writeAsText(outputPath);

		env.execute();
	}

	private static class InfiniteIntegerSource implements ParallelSourceFunction<Integer> {
		private static final long serialVersionUID = -7517574288730066280L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			int counter = 0;
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(counter++);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class ReducingStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Integer> {

		private static final long serialVersionUID = -5939722892793950253L;
		private transient ReducingState<Integer> kvState;

		@Override
		public void open(Configuration parameters) throws Exception {
			ReducingStateDescriptor<Integer> stateDescriptor =
					new ReducingStateDescriptor<>(
							"reducing-state",
							new ReduceSum(),
							Integer.class);

			this.kvState = getRuntimeContext().getReducingState(stateDescriptor);
		}

		@Override
		public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> out) throws Exception {
			kvState.add(value.f1);
		}

		private static class ReduceSum implements ReduceFunction<Integer> {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		}
	}
}
