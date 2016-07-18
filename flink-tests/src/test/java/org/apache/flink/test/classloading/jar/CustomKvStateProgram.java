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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
 * <p>This is used to test proper usage of the user code class laoder when
 * disposing savepoints.
 */
public class CustomKvStateProgram {

	public static void main(String[] args) throws Exception {
		final String jarFile = args[0];
		final String host = args[1];
		final int port = Integer.parseInt(args[2]);
		final int parallelism = Integer.parseInt(args[3]);
		final String checkpointPath = args[4];
		final int checkpointingInterval = Integer.parseInt(args[5]);
		final String outputPath = args[6];

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarFile);
		env.setParallelism(parallelism);
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(checkpointingInterval);
		env.setStateBackend(new FsStateBackend(checkpointPath));

		DataStream<Integer> source = env.addSource(new InfiniteIntegerSource());
		source.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = -9044152404048903826L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return ThreadLocalRandom.current().nextInt(parallelism);
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

	private static class ReducingStateFlatMap extends RichFlatMapFunction<Integer, Integer> {

		private static final long serialVersionUID = -5939722892793950253L;
		private ReducingState<Integer> kvState;

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
		public void flatMap(Integer value, Collector<Integer> out) throws Exception {
			kvState.add(value);
		}

		private static class ReduceSum implements ReduceFunction<Integer> {
			@Override
			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		}
	}
}
