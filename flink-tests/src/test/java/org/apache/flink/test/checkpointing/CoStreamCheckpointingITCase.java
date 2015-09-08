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
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A simple test that runs a streaming topology with checkpointing enabled. This differs from
 * {@link org.apache.flink.test.checkpointing.StreamCheckpointingITCase} in that it contains
 * a TwoInput (or co-) Task.
 *
 * <p>
 * This checks whether checkpoint barriers correctly trigger TwoInputTasks and also whether
 * this barriers are correctly forwarded.
 *
 * <p>
 * This uses a mixture of Operators with the {@link Checkpointed} interface and the new
 * {@link org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext#getOperatorState}
 * method.
 *
 * <p>
 * The test triggers a failure after a while and verifies that, after completion, the
 * state reflects the "exactly once" semantics.
 */
@SuppressWarnings("serial")
public class CoStreamCheckpointingITCase extends StreamFaultToleranceTestBase {

	final long NUM_STRINGS = 10_000_000L;

	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *     [ (source)->(filter)->(map) ] -> [ (co-map) ] -> [ (map) ] -> [ (groupBy/reduce)->(sink) ]
	 * </pre>
	 */
	@Override
	public void testProgram(StreamExecutionEnvironment env) {

		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);

		DataStream<String> stream = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS));

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
				.groupBy("prefix")
				.reduce(new OnceFailingReducer(NUM_STRINGS))
				.addSink(new SinkFunction<PrefixCount>() {

					@Override
					public void invoke(PrefixCount value) {
						// Do nothing here
					}
				});
	}

	@Override
	public void postSubmit() {
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

		if (!StringPrefixCountRichMapFunction.restoreCalledAtLeastOnce) {
			System.err.println("Test inconclusive: Restore was never called on counting Map function.");
		}

		if (!LeftIdentityCoRichFlatMapFunction.restoreCalledAtLeastOnce) {
			System.err.println("Test inconclusive: Restore was never called on counting CoMap function.");
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

	private static class StringGeneratingSourceFunction extends RichSourceFunction<String>
			implements  ParallelSourceFunction<String> {

		private final long numElements;

		private Random rnd;
		private StringBuilder stringBuilder;

		private OperatorState<Integer> index;
		private int step;

		private volatile boolean isRunning;

		static final long[] counts = new long[PARALLELISM];
		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = index.value();
		}


		StringGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			rnd = new Random();
			stringBuilder = new StringBuilder();
			step = getRuntimeContext().getNumberOfParallelSubtasks();


			index = getRuntimeContext().getOperatorState("index", getRuntimeContext().getIndexOfThisSubtask(), false);

			isRunning = true;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index.value() < numElements) {
				char first = (char) ((index.value() % 40) + 40);

				stringBuilder.setLength(0);
				stringBuilder.append(first);

				String result = randomString(stringBuilder, rnd);

				synchronized (lockingObject) {
					index.update(index.value() + step);
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
	}

	private static class StatefulCounterFunction extends RichMapFunction<PrefixCount, PrefixCount> {

		private OperatorState<Long> count;
		static final long[] counts = new long[PARALLELISM];

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

	private static class StringRichFilterFunction extends RichFilterFunction<String> implements Checkpointed<Long> {

		Long count = 0L;
		static final long[] counts = new long[PARALLELISM];

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
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return count;
		}

		@Override
		public void restoreState(Long state) {
			count = state;
		}
	}

	private static class StringPrefixCountRichMapFunction extends RichMapFunction<String, PrefixCount> implements Checkpointed<Long> {
		
		static final long[] counts = new long[PARALLELISM];
		static volatile boolean restoreCalledAtLeastOnce = false;
		
		private long count;

		@Override
		public PrefixCount map(String value) {
			count += 1;
			return new PrefixCount(value.substring(0, 1), value, 1L);
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return count;
		}

		@Override
		public void restoreState(Long state) {
			restoreCalledAtLeastOnce = true;
			count = state;
			if (count == 0) {
				throw new RuntimeException("Restore from beginning");
			}
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}
	}

	private static class LeftIdentityCoRichFlatMapFunction extends RichCoFlatMapFunction<String, String, String> implements Checkpointed<Long> {

		static final long[] counts = new long[PARALLELISM];
		static volatile boolean restoreCalledAtLeastOnce = false;
		
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
		public Long snapshotState(long checkpointId, long checkpointTimestamp) {
			return count;
		}

		@Override
		public void restoreState(Long state) {
			restoreCalledAtLeastOnce = true;
			count = state;
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}
	}
}
