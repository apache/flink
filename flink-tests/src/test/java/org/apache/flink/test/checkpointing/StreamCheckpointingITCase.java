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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 */
@SuppressWarnings("serial")
public class StreamCheckpointingITCase {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_TASK_SLOTS = 3;
	private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static final long NUM_STRINGS = 10000000L;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration conf = new Configuration();
			conf.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);

			cluster = new ForkableFlinkMiniCluster(conf, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("custer startup failed");
		}
	}

	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Cluster shutdown failed.");
		}
	}

	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *
	 *     (source)  ->  (filter)  ->  (map)  ->  (groupBy / reduce)  -> (sink)
	 *
	 * </pre>
	 */
	@Test
	public void runCheckpointedProgram() {

		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
																	"localhost", cluster.getJobManagerRPCPort());
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(1000);
			env.getConfig().disableSysoutLogging();

			DataStream<String> stream = env.addSource(new RichParallelSourceFunction<String>() {

				private Random rnd;
				private StringBuilder stringBuilder;

				private int step;

				private boolean running = true;

				@Override
				public void open(Configuration parameters) {
					rnd = new Random();
					stringBuilder = new StringBuilder();
					step = getRuntimeContext().getNumberOfParallelSubtasks();
				}

				@Override
				public void run(Collector<String> collector) throws Exception {
					for (long i = getRuntimeContext().getIndexOfThisSubtask(); running && i < NUM_STRINGS; i += step) {
						char first = (char) ((i % 40) + 40);

						stringBuilder.setLength(0);
						stringBuilder.append(first);

						collector.collect(randomString(stringBuilder, rnd));
					}
				}

				@Override
				public void cancel() {
					running = false;
				}
			});

			stream
					.filter(new FilterFunction<String>() {
						@Override
						public boolean filter(String value) {
							return value.length() < 100;
						}
					})
					.map(new MapFunction<String, PrefixCount>() {

						@Override
						public PrefixCount map(String value) {
							return new PrefixCount(value.substring(0, 1), value, 1L);
						}
					})
					.groupBy("prefix")
					.reduce(new ReduceFunction<PrefixCount>() {
						@Override
						public PrefixCount reduce(PrefixCount value1, PrefixCount value2) {
							value1.count += value2.count;
							return value1;
						}
					})
					.addSink(new RichSinkFunction<PrefixCount>() {

						private Map<Character, Long> counts = new HashMap<Character, Long>();

						@Override
						public void invoke(PrefixCount value) {
							Character first = value.prefix.charAt(0);
							Long previous = counts.get(first);
							if (previous == null) {
								counts.put(first, value.count);
							} else {
								counts.put(first, Math.max(previous, value.count));
							}
						}

						@Override
						public void close() {
							for (Long count : counts.values()) {
								assertEquals(NUM_STRINGS / 40, count.longValue());
							}
						}

			});

			env.execute();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static String randomString(StringBuilder bld, Random rnd) {
		final int len = rnd.nextInt(10) + 5;

		for (int i = 0; i < len; i++) {
			char next = (char) (rnd.nextInt(20000) + 33);
			bld.append(next);
		}

		return bld.toString();
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Type Classes
	// --------------------------------------------------------------------------------------------

	public static class PrefixCount {

		public String prefix;
		public String value;
		public long count;

		public PrefixCount() {}

		public PrefixCount(String prefix, String value, long count) {
			this.prefix = prefix;
			this.value = value;
			this.count = count;
		}

		@Override
		public String toString() {
			return prefix + " / " + value;
		}
	}
}
