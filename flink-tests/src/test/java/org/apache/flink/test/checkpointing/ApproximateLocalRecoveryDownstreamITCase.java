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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.test.util.TestUtils.tryExecute;

/**
 * To test approximate downstream failover.
 *
 * <p>If a task fails, all its downstream tasks restart, including itself.
 * - Test the failed task can reconnect successfully.
 * - Test partial records are cleaned up correctly.
 * - Test only downstream are tasks restart.
 */
@Ignore("Approximate local recovery has currently no scheduler support")
public class ApproximateLocalRecoveryDownstreamITCase extends TestLogger {
	private static final int BUFFER_SIZE = 4096;

	@Rule
	public MiniClusterWithClientResource cluster = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(createConfig())
			.setNumberTaskManagers(4)
			.setNumberSlotsPerTaskManager(1)
			.build());

	@Rule
	public final Timeout timeout = Timeout.millis(300000L);

	/**
	 * Test the following topology.
	 * <pre>
	 *     (source1/1) -----> (map1/1) -----> (sink1/1)
	 * </pre>
	 * (map1/1) fails, (map1/1) and (sink1/1) restart
	 */
	@Test
	public void localTaskFailureRecoveryThreeTasks() throws Exception {
		final int failAfterElements = 150;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env
			.setParallelism(1)
			.setBufferTimeout(0)
			.setMaxParallelism(128)
			.disableOperatorChaining()
			.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.getCheckpointConfig().enableApproximateLocalRecovery(true);

		env.addSource(new AppSourceFunction())
			.slotSharingGroup("source")
			.map(new FailingMapper<>(failAfterElements))
			.slotSharingGroup("map")
			.addSink(new ValidatingAtMostOnceSink(300))
			.slotSharingGroup("sink");

		FailingMapper.failedBefore = false;
		tryExecute(env, "testThreeTasks");
	}

	/**
	 * Test the following topology.
	 * <pre>
	 *     (source1/1) -----> (map1/2) -----> (sink1/1)
	 *         |                                ^
	 *         -------------> (map2/2) ---------|
	 * </pre>
	 * (map1/2) fails, (map1/2) and (sink1/1) restart
	 */
	@Test
	public void localTaskFailureRecoveryTwoMapTasks() throws Exception {
		final int failAfterElements = 20;
		final int keyByChannelNumber = 2;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env
			.setParallelism(1)
			.setBufferTimeout(0)
			.disableOperatorChaining()
			.setMaxParallelism(128)
			.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
		env.getCheckpointConfig().enableApproximateLocalRecovery(true);

		env.addSource(new AppSourceFunction(BUFFER_SIZE, env.getMaxParallelism(), keyByChannelNumber))
			.slotSharingGroup("source")
			.keyBy(InvertedKeyTuple::key)
			.map(new FailingMapper<>(failAfterElements))
			.setParallelism(keyByChannelNumber)
			.slotSharingGroup("map")
			.addSink(new ValidatingAtMostOnceSink(200, keyByChannelNumber))
			.slotSharingGroup("sink");

		FailingMapper.failedBefore = false;
		tryExecute(env, "testTwoMapTasks");
	}

	/**
	 * source function used to generate InvertedKeyTuple.
	 *
	 * <p>InvertedKeyTuple includes four fields:
	 * - index: increased upon each tuple is generated.
	 * - key: based on which the tuple to be partitioned for downstream consumption (a map, as shown in the testcase for example).
	 * - selectedChannel: the selected channel (the selected instance of the downstream operator) the tuple to be sent to.
	 *                    Usually, selectedChannel is decided by the hash-partition function applied on the key.
	 *                    However in this case, we invert the process by selecting the channel to be sent first, and then
	 *                    decide a possible key the tuple may contain. In this way, we can control exactly how many
	 *                    tuples are sent to each instance of the downstream operator.
	 * - longOrShortString: either long or short string used to test network transition upon reconnection.
	 *
	 * <p>AppSourceFunction makes sure tuples generated are evenly distributed amongst downstream operator instances.
	 */
	private static class AppSourceFunction extends RichParallelSourceFunction<InvertedKeyTuple> {
		private final String shortString = "I am a very long string to test partial records hohoho hahaha ";
		private final String longOrShortString;
		private final int maxParallelism;
		private final int numberOfChannels;
		private final int[] keys;
		private int index = 0;
		private volatile boolean running = true;

		// short-length string
		AppSourceFunction() {
			this.longOrShortString = shortString;
			this.maxParallelism = 128;
			this.numberOfChannels = 1;
			this.keys = initKeys(numberOfChannels);
		}

		// long-length string
		AppSourceFunction(int bufferSize, int maxParallelism, int numberOfChannels) {
			this.maxParallelism = maxParallelism;
			this.numberOfChannels = numberOfChannels;
			this.keys = initKeys(numberOfChannels);

			StringBuilder builder = new StringBuilder(shortString);
			for (int i = 0; i <= 2 * bufferSize / shortString.length() + 1; i++) {
				builder.append(shortString);
			}
			this.longOrShortString = builder.toString();
		}

		@Override
		public void run(SourceContext<InvertedKeyTuple> ctx) throws Exception{
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					if (index % 100 == 0) {
						Thread.sleep(50);
					}
					int selectedChannel = index % numberOfChannels;
					int key = keys[selectedChannel];
					ctx.collect(new InvertedKeyTuple(index, key, selectedChannel, longOrShortString));
				}
				index++;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		// Find the first key which falls into the key group of each downstream channel
		private int[] initKeys(int numberOfChannels) {
			int[] keys = new int[numberOfChannels];

			for (int i = 0; i < numberOfChannels; i++) {
				int key = 0;
				while (key < 1000 && selectedChannel(key) != i) {
					key++;
				}
				assert key < 1000 : "Can not find a key within number 1000";
				keys[i] = key;
			}

			return keys;
		}

		private int selectedChannel(int key) {
			return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
		}
	}

	/** Fails once the first map instance reaches failCount. */
	private static class FailingMapper<T> extends RichMapFunction<T, T>  {
		private static final long serialVersionUID = 6334389850158703L;

		private static volatile boolean failedBefore;

		private final int failCount;
		private int numElementsTotal;

		private boolean failer;

		FailingMapper(int failCount) {
			this.failCount = failCount;
		}

		@Override
		public void open(Configuration parameters) {
			failer = getRuntimeContext().getIndexOfThisSubtask() == 0;
		}

		@Override
		public T map(T value) throws Exception {
			numElementsTotal++;

			if (!failedBefore) {
				Thread.sleep(10);

				if (failer && numElementsTotal >= failCount) {
					failedBefore = true;
					throw new Exception("Artificial Test Failure");
				}
			}

			return value;
		}
	}

	/**
	 * Validating sink to make sure each selectedChannel (of map) gets at least numElementsTotal elements.
	 *
	 * <p>Notice that the source generates tuples evenly distributed amongst downstream operator (map) instances.
	 * Besides, the index generated is continuous and monotonic increasing as long as the source is not restarted.
	 * Hence,
	 * - In the case of approximate local recovery is NOT enabled, where the entire job, including source, is restarted
	 *   after map fails, source regenerates tuples indexed from 0. Upon reach numElementsTotal,
	 *   the maximal possible index is numElementsTotal * numberOfInputChannels - 1
	 * - In the case of approximate local recovery is enabled, where only the downstream of the failed task restart,
	 *   source does not regenerating tuples, the maximal possible index > numElementsTotal * numberOfInputChannels - 1
	 */
	private static class ValidatingAtMostOnceSink extends RichSinkFunction<InvertedKeyTuple> {
		private static final long serialVersionUID = 1748426382527469932L;
		private final int numElementsTotal;
		private final int[] numElements;
		private final Integer[] indexReachingNumElements;
		private final int numberOfInputChannels;

		ValidatingAtMostOnceSink(int numElementsTotal, int numberOfInputChannels) {
			this.numElementsTotal = numElementsTotal;
			this.numberOfInputChannels = numberOfInputChannels;
			this.numElements = new int[numberOfInputChannels];
			this.indexReachingNumElements = new Integer[numberOfInputChannels];
		}

		ValidatingAtMostOnceSink(int numElementsTotal) {
			this.numElementsTotal = numElementsTotal;
			this.numberOfInputChannels = 1;
			this.numElements = new int[numberOfInputChannels];
			this.indexReachingNumElements = new Integer[numberOfInputChannels];
		}

		@Override
		public void invoke(InvertedKeyTuple tuple, Context ctx) throws Exception {
			assert tuple.selectedChannel < numberOfInputChannels;
			numElements[tuple.selectedChannel]++;

			boolean allReachNumElementsTotal = true;
			for (int i = 0; i < numberOfInputChannels; i++) {
				if (numElements[i] == numElementsTotal) {
					indexReachingNumElements[i] = tuple.index;
				} else if (numElements[i] < numElementsTotal) {
					allReachNumElementsTotal = false;
				}
			}
			if (allReachNumElementsTotal) {
				assert Collections.max(Arrays.asList(indexReachingNumElements)) >= numElementsTotal * numberOfInputChannels;
				throw new SuccessException();
			}
		}
	}

	private static Configuration createConfig() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse(Integer.toString(BUFFER_SIZE)));

		return config;
	}

	private static class InvertedKeyTuple {
		int index;
		/** Key based on which the tuple is partitioned. */
		int key;
		/** The selected channel of this tuple after key-partitioned. */
		int selectedChannel;
		String longOrShortString;

		InvertedKeyTuple(int index, int key, int selectedChannel, String longOrShortString) {
			this.index = index;
			this.key = key;
			this.selectedChannel = selectedChannel;
			this.longOrShortString = longOrShortString;
		}

		int key() {
			return key;
		}
	}
}
