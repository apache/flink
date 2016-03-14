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

package org.apache.flink.test.scaling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.Random;

public class KeyGroupsITCase extends TestLogger {

	@Test
	public void testKeyGroups() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(1000);
		env.getConfig().setMaxParallelism(5);
		env.setParallelism(3);

		DataStream<Tuple2<Integer, Integer>> input = env.addSource(new SimpleSource()).map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
			private static final long serialVersionUID = 494178923987052158L;

			@Override
			public Tuple2<Integer, Integer> map(Integer value) throws Exception {
				return Tuple2.of(value, value);
			}
		});

		DataStream<Tuple2<Integer, Integer>> result = input.keyBy(0).map(new RichMapFunction<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {

			private static final long serialVersionUID = 5713085119372248196L;

			transient ValueState<Integer> counter = null;

			@Override
			public void open(Configuration configuration) {
				counter = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("counter", Integer.class, 0));
			}

			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				int cnt = counter.value();
				counter.update(value.f1 + cnt);

				return Tuple2.of(value.f0, value.f1 + cnt);
			}
		});

		result.print();

		env.execute();
	}

	public static class SimpleSource extends RichParallelSourceFunction<Integer> {

		private static final long serialVersionUID = 517364335085858153L;

		private boolean running = true;

		transient private Random random;

		@Override
		public void open(Configuration configuration) {
			random = new Random();
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				ctx.collect(random.nextInt(20));
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
