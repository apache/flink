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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;

import org.apache.flink.shaded.guava18.com.google.common.collect.EvictingQueue;

import org.junit.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 * Integration test ensuring that the persistent state defined by the implementations
 * of {@link AbstractUdfStreamOperator} is correctly restored in case of recovery from
 * a failure.
 *
 * <p>The topology currently tests the proper behaviour of the {@link StreamGroupedReduce} operator.
 */
@SuppressWarnings("serial")
public class UdfStreamOperatorCheckpointingITCase extends StreamFaultToleranceTestBase {

	private static final long NUM_INPUT = 500_000L;
	private static final int NUM_OUTPUT = 1_000;

	/**
	 * Assembles a stream of a grouping field and some long data. Applies reduce functions
	 * on this stream.
	 */
	@Override
	public void testProgram(StreamExecutionEnvironment env) {

		// base stream
		KeyedStream<Tuple2<Integer, Long>, Tuple> stream = env.addSource(new StatefulMultipleSequence())
				.keyBy(0);

		stream
				// testing built-in aggregate
				.min(1)
				// failure generation
				.map(new OnceFailingIdentityMapFunction(NUM_INPUT))
				.keyBy(0)
				.addSink(new MinEvictingQueueSink());

		stream
				// testing UDF reducer
				.reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
					@Override
					public Tuple2<Integer, Long> reduce(
							Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				})
				.keyBy(0)
				.addSink(new SumEvictingQueueSink());
	}

	@Override
	public void postSubmit() {

		// Note that these checks depend on the ordering of the input

		// Checking the result of the built-in aggregate
		for (int i = 0; i < PARALLELISM; i++) {
			for (Long value : MinEvictingQueueSink.queues[i]) {
				Assert.assertTrue("Value different from 1 found, was " + value + ".", value == 1);
			}
		}

		// Checking the result of the UDF reducer
		for (int i = 0; i < PARALLELISM; i++) {
			long prevCount = NUM_INPUT - NUM_OUTPUT;
			long sum = prevCount * (prevCount + 1) / 2;
			while (!SumEvictingQueueSink.queues[i].isEmpty()) {
				sum += ++prevCount;
				Long value = SumEvictingQueueSink.queues[i].remove();
				Assert.assertTrue("Unexpected reduce value " + value + " instead of " + sum + ".", value == sum);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Produces a sequence multiple times for each parallelism instance of downstream operators,
	 * augmented by the designated parallel subtaskId. The source is not parallel to ensure order.
	 */
	private static class StatefulMultipleSequence extends RichSourceFunction<Tuple2<Integer, Long>>
			implements ListCheckpointed<Long> {

		private long count;

		@Override
		public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
			Object lock = ctx.getCheckpointLock();

			while (count < NUM_INPUT){
				synchronized (lock){
					for (int i = 0; i < PARALLELISM; i++) {
						ctx.collect(Tuple2.of(i, count + 1));
					}
					count++;
				}
			}
		}

		@Override
		public void cancel() {}

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

	/**
	 * Mapper that causes one failure between seeing 40% to 70% of the records.
	 */
	private static class OnceFailingIdentityMapFunction
			extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>
			implements ListCheckpointed<Long> {

		private static volatile boolean hasFailed = false;

		private final long numElements;

		private long failurePos;
		private long count;

		public OnceFailingIdentityMapFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
		}

		@Override
		public Tuple2<Integer, Long> map(Tuple2<Integer, Long> value) throws Exception {
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}
			count++;
			return value;
		}

		@Override
		public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(count);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			if (!state.isEmpty()) {
				count = state.get(0);
			}
		}
	}

	/**
	 * Sink that emits the output to an evicting queue storing the last {@link #NUM_OUTPUT} elements.
	 * A separate queue is initiated for each group, apply a grouping prior to this operator to avoid
	 * parallel access of the queues.
	 */
	private static class MinEvictingQueueSink implements SinkFunction<Tuple2<Integer, Long>> {

		public static Queue<Long>[] queues = new Queue[PARALLELISM];

		@Override
		public void invoke(Tuple2<Integer, Long> value) throws Exception {
			if (queues[value.f0] == null) {
				queues[value.f0] = EvictingQueue.create(NUM_OUTPUT);
			}
			queues[value.f0].add(value.f1);
		}
	}

	/**
	 * Sink that emits the output to an evicting queue storing the last {@link #NUM_OUTPUT} elements.
	 * A separate queue is initiated for each group, apply a grouping prior to this operator to avoid
	 * parallel access of the queues.
	 */
	private static class SumEvictingQueueSink implements SinkFunction<Tuple2<Integer, Long>> {

		public static Queue<Long>[] queues = new Queue[PARALLELISM];

		@Override
		public void invoke(Tuple2<Integer, Long> value) throws Exception {
			if (queues[value.f0] == null) {
				queues[value.f0] = EvictingQueue.create(NUM_OUTPUT);
			}
			queues[value.f0].add(value.f1);
		}
	}
}
