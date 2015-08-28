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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 * 
 * The test triggers a failure after a while and verifies that, after
 * completion, the state reflects the "exactly once" semantics.
 * 
 * It is designed to check partitioned states.
 */
@SuppressWarnings("serial")
public class PartitionedStateCheckpointingITCase extends StreamFaultToleranceTestBase {

	final long NUM_STRINGS = 10_000_000L;

	@Override
	public void testProgram(StreamExecutionEnvironment env) {
		assertTrue("Broken test setup", (NUM_STRINGS/2) % 40 == 0);

		DataStream<Integer> stream1 = env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2));
		DataStream<Integer> stream2 = env.addSource(new IntGeneratingSourceFunction(NUM_STRINGS / 2));

		stream1.union(stream2)
				.groupBy(new IdentityKeySelector<Integer>())
				.map(new OnceFailingPartitionedSum(NUM_STRINGS))
				.keyBy(0)
				.addSink(new CounterSink());
	}

	@Override
	public void postSubmit() {
		// verify that we counted exactly right
		for (Entry<Integer, Long> sum : OnceFailingPartitionedSum.allSums.entrySet()) {
			assertEquals(new Long(sum.getKey() * NUM_STRINGS / 40), sum.getValue());
		}
		for (Long count : CounterSink.allCounts.values()) {
			assertEquals(new Long(NUM_STRINGS / 40), count);
		}

		assertEquals(40, CounterSink.allCounts.size());
		assertEquals(40, OnceFailingPartitionedSum.allSums.size());
	}

	// --------------------------------------------------------------------------------------------
	// Custom Functions
	// --------------------------------------------------------------------------------------------

	private static class IntGeneratingSourceFunction extends RichParallelSourceFunction<Integer> {

		private final long numElements;

		private OperatorState<Integer> index;
		private int step;

		private volatile boolean isRunning;

		static final long[] counts = new long[PARALLELISM];

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = index.value();
		}

		IntGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			step = getRuntimeContext().getNumberOfParallelSubtasks();

			index = getRuntimeContext().getOperatorState("index",
					getRuntimeContext().getIndexOfThisSubtask(), false);

			isRunning = true;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index.value() < numElements) {

				synchronized (lockingObject) {
					index.update(index.value() + step);
					ctx.collect(index.value() % 40);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class OnceFailingPartitionedSum extends RichMapFunction<Integer, Tuple2<Integer, Long>> {

		private static Map<Integer, Long> allSums = new ConcurrentHashMap<Integer, Long>();
		private static volatile boolean hasFailed = false;

		private final long numElements;

		private long failurePos;
		private long count;

		private OperatorState<Long> sum;

		OnceFailingPartitionedSum(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext()
					.getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext()
					.getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
			sum = getRuntimeContext().getOperatorState("sum", 0L, true);
		}

		@Override
		public Tuple2<Integer, Long> map(Integer value) throws Exception {
			count++;
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}

			long currentSum = sum.value() + value;
			sum.update(currentSum);
			allSums.put(value, currentSum);
			return new Tuple2<Integer, Long>(value, currentSum);
		}
	}

	private static class CounterSink extends RichSinkFunction<Tuple2<Integer, Long>> {

		private static Map<Integer, Long> allCounts = new ConcurrentHashMap<Integer, Long>();

		private OperatorState<Long> counts;

		@Override
		public void open(Configuration parameters) throws IOException {
			counts = getRuntimeContext().getOperatorState("count", 0L, true);
		}

		@Override
		public void invoke(Tuple2<Integer, Long> value) throws Exception {
			long currentCount = counts.value() + 1;
			counts.update(currentCount);
			allCounts.put(value.f0, currentCount);

		}
	}
	
	private static class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}

	}
}
