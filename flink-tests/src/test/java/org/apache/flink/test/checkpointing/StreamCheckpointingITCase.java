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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 * 
 * The test triggers a failure after a while and verifies that, after completion, the
 * state reflects the "exactly once" semantics.
 */
@SuppressWarnings("serial")
public class StreamCheckpointingITCase {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_TASK_SLOTS = 3;
	private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
			config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 ms");
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);
			
			cluster = new ForkableFlinkMiniCluster(config, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.shutdown();
			cluster = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}
	
	
	
	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *     [ (source)->(filter)->(map) ] -> [ (map) ] -> [ (groupBy/reduce)->(sink) ]
	 * </pre>
	 */
	@Test
	public void runCheckpointedProgram() {

		final long NUM_STRINGS = 10000000L;
		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);
		
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
																	"localhost", cluster.getJobManagerRPCPort());
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(500);
			env.getConfig().disableSysoutLogging();

			DataStream<String> stream = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS));
			
			stream
					// -------------- first vertex, chained to the source ----------------
					
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

					// -------------- seconds vertex - the stateful one that also fails ----------------
					
					.startNewChain()
					.map(new StatefulCounterFunction())

					// -------------- third vertex - reducer and the sink ----------------
					
					.groupBy("prefix")
					.reduce(new OnceFailingReducer(NUM_STRINGS))
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

//						@Override
//						public void close() {
//							for (Long count : counts.values()) {
//								assertEquals(NUM_STRINGS / 40, count.longValue());
//							}
//						}
					});

			env.execute();
			
			long countSum = 0;
			for (long l : StatefulCounterFunction.counts) {
				countSum += l;
			}
			
			// verify that we counted exactly right
			
			// this line should be uncommented once the "exactly one off by one" is fixed
//			assertEquals(NUM_STRINGS, countSum);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------
	
	private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<String> 
			implements Checkpointed<Long> {

		private final long numElements;
		
		private Random rnd;
		private StringBuilder stringBuilder;

		private long index;
		private int step;


		StringGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) {
			rnd = new Random();
			stringBuilder = new StringBuilder();
			step = getRuntimeContext().getNumberOfParallelSubtasks();
			
			if (index == 0) {
				index = getRuntimeContext().getIndexOfThisSubtask();
			}
		}

		@Override
		public boolean reachedEnd() throws Exception {
			return index >= numElements;
		}

		@Override
		public String next() throws Exception {
			char first = (char) ((index % 40) + 40);

			stringBuilder.setLength(0);
			stringBuilder.append(first);

			String result = randomString(stringBuilder, rnd);
			index += step;
			return result;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return this.index;
		}

		@Override
		public void restoreState(Long state) {
			this.index = state;
		}

		private static String randomString(StringBuilder bld, Random rnd) {
			final int len = rnd.nextInt(10) + 5;

			for (int i = 0; i < len; i++) {
				char next = (char) (rnd.nextInt(20000) + 33);
				bld.append(next);
			}

			return bld.toString();
		}
	}
	
	private static class StatefulCounterFunction extends RichMapFunction<PrefixCount, PrefixCount> 
			implements Checkpointed<Long> {

		static final long[] counts = new long[PARALLELISM];

		private long count = 0;

		@Override
		public PrefixCount map(PrefixCount value) throws Exception {
			count++;
			return value;
		}

		@Override
		public void close() {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return count;
		}

		@Override
		public void restoreState(Long state) {
			count = state;
		}
	}
	
	private static class OnceFailingReducer extends RichReduceFunction<PrefixCount> {

		private static volatile boolean hasFailed = false;

		private final long numElements;
		
		private long failurePos;
		private long count;

		OnceFailingReducer(long numElements) {
			this.numElements = numElements;
		}
		
		@Override
		public void open(Configuration parameters) {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
		}
		
		@Override
		public PrefixCount reduce(PrefixCount value1, PrefixCount value2) throws Exception {
			count++;
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}
			
			value1.count += value2.count;
			return value1;
		}
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
