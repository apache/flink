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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Test program with very fast failure rate.
 */
@SuppressWarnings("serial")
public class FastFailuresITCase extends AbstractTestBase {

	static final AtomicInteger FAILURES_SO_FAR = new AtomicInteger();
	static final int NUM_FAILURES = 200;

	@Test
	public void testThis() {
		final int parallelism = 4;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().disableSysoutLogging();
		env.setParallelism(parallelism);
		env.enableCheckpointing(1000);
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(210, 0));

		DataStream<Tuple2<Integer, Integer>> input = env.addSource(new RichSourceFunction<Tuple2<Integer, Integer>>() {

			@Override
			public void open(Configuration parameters) {
				if (FAILURES_SO_FAR.incrementAndGet() <= NUM_FAILURES) {
					throw new RuntimeException("fail");
				}
			}

			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx)  {}

			@Override
			public void cancel() {}
		});

		input
				.keyBy(0)
				.map(new MapFunction<Tuple2<Integer, Integer>, Integer>() {

					@Override
					public Integer map(Tuple2<Integer, Integer> value) {
						return value.f0;
					}
				})
				.addSink(new SinkFunction<Integer>() {
					@Override
					public void invoke(Integer value) {}
				});

		try {
			env.execute();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
