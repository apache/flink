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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A simple test that runs a streaming topology with checkpointing enabled. This differs from
 * {@link org.apache.flink.test.checkpointing.StreamCheckpointingITCase} in that it contains
 * a TwoInput (or co-) Task.
 *
 * <p>This checks whether checkpoint barriers correctly trigger TwoInputTasks and also whether
 * this barriers are correctly forwarded.
 *
 * <p>The test triggers a failure after a while and verifies that, after completion, the
 * state reflects the "exactly once" semantics.
 */
@SuppressWarnings({"serial", "deprecation"})
public class CoStreamCheckpointingITCase extends AbstractTestBase {

	private static final long NUM_STRINGS = 10_000L;
	private static final int PARALLELISM = 4;

	/**
	 * Runs the following program.
	 * <pre>
	 *     [ (source)->(filter)->(map) ] -> [ (co-map) ] -> [ (map) ] -> [ (groupBy/reduce)->(sink) ]
	 * </pre>
	 */
	@Test
	public void testCoStreamCheckpointingProgram() throws Exception {
		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		env.enableCheckpointing(50);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

		DataStream<String> stream = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS, NUM_STRINGS / 5));

		stream
				// -------------- first vertex, chained to the source ----------------
				.filter(new StringRichFilterFunction())

				// -------------- second vertex - stateful ----------------
				.connect(stream).flatMap(new LeftIdentityCoRichFlatMapFunction())

				// -------------- third vertex - stateful  ----------------
				.map(new StringPrefixCountRichMapFunction())
				.startNewChain()
				.map(new StatefulCounterFunction())

				// -------------- fourth vertex - reducer (failing) and the sink ----------------
				.keyBy("prefix")
				.reduce(new OnceFailingReducer(NUM_STRINGS))
				.addSink(new SinkFunction<PrefixCount>() {

					@Override
					public void invoke(PrefixCount value) {
						// Do nothing here
					}
				});

		TestUtils.tryExecute(env, "Fault Tolerance Test");

		// validate the result

		long filterSum = 0;
		for (long l : StringRichFilterFunction.counts) {
			filterSum += l;
		}

		long coMapSum = 0;
		for (long l : LeftIdentityCoRichFlatMapFunction.counts) {
			coMapSum += l;
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
		assertEquals(NUM_STRINGS, coMapSum);
		assertEquals(NUM_STRINGS, mapSum);
		assertEquals(NUM_STRINGS, countSum);
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------

	/**
	 * A generating source that is slow before the first two checkpoints went through
	 * and will indefinitely stall at a certain point to allow the checkpoint to complete.
	 *
	 * <p>After the checkpoints are through, it continues with full speed.
	 */
	private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<String>
			implements ListCheckpointed<Integer>, CheckpointListener {

		private static volatile int numCompletedCheckpoints = 0;

		private final long numElements;
		private final long checkpointLatestAt;

		private int index = -1;

		private volatile boolean isRunning = true;

		StringGeneratingSourceFunction(long numElements, long checkpointLatestAt) {
			this.numElements = numElements;
			this.checkpointLatestAt = checkpointLatestAt;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			final Random rnd = new Random();
			final StringBuilder stringBuilder = new StringBuilder();

			final Object lockingObject = ctx.getCheckpointLock();

			final int step = getRuntimeContext().getNumberOfParallelSubtasks();
			if (index < 0) {
				// not been restored, so initialize
				index = getRuntimeContext().getIndexOfThisSubtask();
			}

			while (isRunning && index < numElements) {
				char first = (char) ((index % 40) + 40);

				stringBuilder.setLength(0);
				stringBuilder.append(first);

				String result = randomString(stringBuilder, rnd);

				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (lockingObject) {
					index += step;
					ctx.collect(result);
				}

				if (numCompletedCheckpoints < 2) {
					// not yet completed enough checkpoints, so slow down
					if (index < checkpointLatestAt) {
						// mild slow down
						Thread.sleep(1);
					} else {
						// wait until the checkpoints are completed
						while (isRunning && numCompletedCheckpoints < 2) {
							Thread.sleep(5);
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.index);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.index = state.get(0);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				numCompletedCheckpoints++;
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
	}

	private static class StatefulCounterFunction extends RichMapFunction<PrefixCount, PrefixCount>
			implements ListCheckpointed<Long> {

		static long[] counts = new long[PARALLELISM];

		private long count;

		@Override
		public PrefixCount map(PrefixCount value) throws Exception {
			count++;
			return value;
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.count);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.count = state.get(0);
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

	private static class StringRichFilterFunction extends RichFilterFunction<String> implements ListCheckpointed<Long> {

		static long[] counts = new long[PARALLELISM];

		private long count = 0L;

		@Override
		public boolean filter(String value) {
			count++;
			return value.length() < 100;
		}

		@Override
		public void close() {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.count);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.count = state.get(0);
		}
	}

	private static class StringPrefixCountRichMapFunction extends RichMapFunction<String, PrefixCount> implements ListCheckpointed<Long> {

		static long[] counts = new long[PARALLELISM];

		private long count;

		@Override
		public PrefixCount map(String value) {
			count += 1;
			return new PrefixCount(value.substring(0, 1), value, 1L);
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.count);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.count = state.get(0);
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}
	}

	private static class LeftIdentityCoRichFlatMapFunction extends RichCoFlatMapFunction<String, String, String> implements ListCheckpointed<Long> {

		static long[] counts = new long[PARALLELISM];

		private long count;

		@Override
		public void flatMap1(String value, Collector<String> out) {
			count += 1;

			out.collect(value);
		}

		@Override
		public void flatMap2(String value, Collector<String> out) {
			// we ignore the values from the second input
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.count);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.count = state.get(0);
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}
	}

	/**
	 * POJO storing a prefix, value, and count.
	 */
	public static class PrefixCount implements Serializable {

		public String prefix;
		public String value;
		public long count;

		@SuppressWarnings("unused")
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
