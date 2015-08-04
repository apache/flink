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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.Collector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 * 
 * The test triggers a failure after a while and verifies that, after completion, the
 * state reflects the "exactly once" semantics.
 */
@SuppressWarnings("serial")
public class StateCheckpoinedITCase {

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
					.filter(new StringRichFilterFunction())

					// -------------- seconds vertex - one-to-one connected ----------------
					.map(new StringPrefixCountRichMapFunction())
					.startNewChain()
					.map(new StatefulCounterFunction())

					// -------------- third vertex - reducer and the sink ----------------
					.partitionByHash("prefix")
					.flatMap(new OnceFailingAggregator(NUM_STRINGS))
					.addSink(new ValidatingSink());

			env.execute();
			
			long filterSum = 0;
			for (long l : StringRichFilterFunction.counts) {
				filterSum += l;
			}

			long mapSum = 0;
			for (long l : StringPrefixCountRichMapFunction.counts) {
				mapSum += l;
			}

			long countSum = 0;
			for (long l : StatefulCounterFunction.counts) {
				countSum += l;
			}

			// verify that we counted exactly right
			assertEquals(NUM_STRINGS, filterSum);
			assertEquals(NUM_STRINGS, mapSum);
			assertEquals(NUM_STRINGS, countSum);

			for (Map<Character, Long> map : ValidatingSink.maps) {
				for (Long count : map.values()) {
					assertEquals(NUM_STRINGS / 40, count.longValue());
				}
			}
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
			implements CheckpointedAsynchronously<Integer>
	{
		private final long numElements;

		private int index;

		private volatile boolean isRunning = true;
		
		
		StringGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			final Random rnd = new Random();
			final StringBuilder stringBuilder = new StringBuilder();
			
			final int step = getRuntimeContext().getNumberOfParallelSubtasks();
			
			if (index == 0) {
				index = getRuntimeContext().getIndexOfThisSubtask();
			}

			while (isRunning && index < numElements) {
				char first = (char) ((index % 40) + 40);

				stringBuilder.setLength(0);
				stringBuilder.append(first);

				String result = randomString(stringBuilder, rnd);

				synchronized (lockingObject) {
					index += step;
					ctx.collect(result);
				}
			}
		}
		
		@Override
		public void cancel() {
			isRunning = false;
		}

		private static String randomString(StringBuilder bld, Random rnd) {
			final int len = rnd.nextInt(10) + 5;

			for (int i = 0; i < len; i++) {
				char next = (char) (rnd.nextInt(20000) + 33);
				bld.append(next);
			}

			return bld.toString();
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
			return index;
		}

		@Override
		public void restoreState(Integer state) {
			index = state;
		}
	}

	private static class StringRichFilterFunction extends RichFilterFunction<String> implements Checkpointed<Long> {

		static final long[] counts = new long[PARALLELISM];

		private long count;

		@Override
		public boolean filter(String value) {
			count++;
			return value.length() < 100; // should be always true
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

	private static class StringPrefixCountRichMapFunction extends RichMapFunction<String, PrefixCount> 
			implements CheckpointedAsynchronously<Long> {
		
		static final long[] counts = new long[PARALLELISM];

		private long count;
		
		@Override
		public PrefixCount map(String value) {
			count++;
			return new PrefixCount(value.substring(0, 1), value, 1L);
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
	
	private static class StatefulCounterFunction extends RichMapFunction<PrefixCount, PrefixCount> {

		static final long[] counts = new long[PARALLELISM];
		
		private OperatorState<Long> count;

		@Override
		public PrefixCount map(PrefixCount value) throws Exception {
			count.update(count.value() + 1);
			return value;
		}

		@Override
		public void open(Configuration conf) throws IOException {
			count = getRuntimeContext().getOperatorState("count", 0L, false);
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count.value();
		}
		
	}
	
	private static class OnceFailingAggregator extends RichFlatMapFunction<PrefixCount, PrefixCount> 
		implements Checkpointed<HashMap<String, PrefixCount>> {

		private static volatile boolean hasFailed = false;

		private final HashMap<String, PrefixCount> aggregationMap = new HashMap<String, PrefixCount>();
		
		private final long numElements;
		
		private long failurePos;
		private long count;
		

		OnceFailingAggregator(long numElements) {
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
		public void flatMap(PrefixCount value, Collector<PrefixCount> out) throws Exception {
			count++;
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}
			
			PrefixCount curr = aggregationMap.get(value.prefix);
			if (curr == null) {
				aggregationMap.put(value.prefix, value);
				out.collect(value);
			}
			else {
				curr.count += value.count;
				out.collect(curr);
			}
		}

		@Override
		public HashMap<String, PrefixCount> snapshotState(long checkpointId, long checkpointTimestamp) {
			return aggregationMap;
		}

		@Override
		public void restoreState(HashMap<String, PrefixCount> state) {
			aggregationMap.putAll(state);
		}
	}

	private static class ValidatingSink extends RichSinkFunction<PrefixCount> 
			implements Checkpointed<HashMap<Character, Long>> {

		@SuppressWarnings("unchecked")
		private static Map<Character, Long>[] maps = (Map<Character, Long>[]) new Map<?, ?>[PARALLELISM];
		
		private HashMap<Character, Long> counts = new HashMap<Character, Long>();

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
		public void close() throws Exception {
			maps[getRuntimeContext().getIndexOfThisSubtask()] = counts;
		}

		@Override
		public HashMap<Character, Long> snapshotState(long checkpointId, long checkpointTimestamp) {
			return counts;
		}

		@Override
		public void restoreState(HashMap<Character, Long> state) {
			counts.putAll(state);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom Type Classes
	// --------------------------------------------------------------------------------------------

	public static class PrefixCount implements Serializable {

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
