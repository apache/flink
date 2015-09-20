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

import com.google.common.collect.EvictingQueue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;
import org.apache.flink.util.Collector;
import org.junit.Assert;

import java.util.Queue;
import java.util.Random;

/**
 * Integration test ensuring that the persistent state defined by the implementations
 * of {@link AbstractUdfStreamOperator} is correctly restored in case of recovery from
 * a failure.
 *
 * <p>
 * The topology currently tests the proper behaviour of the {@link StreamGroupedReduce}
 * operator.
 */
@SuppressWarnings("serial")
public class UdfStreamOperatorCheckpointingITCase extends StreamFaultToleranceTestBase {

	final private static long NUM_INPUT = 2_500_000L;
	final private static int NUM_OUTPUT = 1_000;

	/**
	 * Assembles a stream of a grouping field and some long data. Applies reduce functions
	 * on this stream.
	 */
	@Override
	public void testProgram(StreamExecutionEnvironment env) {

		// base stream
		GroupedDataStream<Tuple2<Integer, Long>> stream = env.addSource(new StatefulMultipleSequence())
				.groupBy(0);


		stream
				// testing built-in aggregate
				.min(1)
				// failure generation
				.map(new OnceFailingIdentityMapFunction(NUM_INPUT))
				.groupBy(0)
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
				.groupBy(0)
				.addSink(new SumEvictingQueueSink());

		stream
				// testing UDF folder
				.fold(Tuple2.of(0, 0L), new FoldFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>() {
					@Override
					public Tuple2<Integer, Long> fold(
							Tuple2<Integer, Long> accumulator, Tuple2<Integer, Long> value) throws Exception {
						return Tuple2.of(value.f0, accumulator.f1 + value.f1);
					}
				})
				.groupBy(0)
				.addSink(new FoldEvictingQueueSink());
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

		// Checking the result of the UDF folder
		for (int i = 0; i < PARALLELISM; i++) {
			long prevCount = NUM_INPUT - NUM_OUTPUT;
			long sum = prevCount * (prevCount + 1) / 2;
			while (!FoldEvictingQueueSink.queues[i].isEmpty()) {
				sum += ++prevCount;
				Long value = FoldEvictingQueueSink.queues[i].remove();
				Assert.assertTrue("Unexpected fold value " + value + " instead of " + sum + ".", value == sum);
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
	private static class StatefulMultipleSequence extends RichSourceFunction<Tuple2<Integer, Long>>{

		private transient OperatorState<Long> count;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			count = getRuntimeContext().getOperatorState("count", 0L, false);
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
			Object lock = ctx.getCheckpointLock();

			while (count.value() < NUM_INPUT){
				synchronized (lock){
					for (int i = 0; i < PARALLELISM; i++) {
						ctx.collect(Tuple2.of(i, count.value() + 1));
					}
					count.update(count.value() + 1);
				}
			}
		}

		@Override
		public void cancel() {
		}
	}

	/**
	 * Mapper that causes one failure between seeing 40% to 70% of the records.
	 */
	private static class OnceFailingIdentityMapFunction
			extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>> {

		private static volatile boolean hasFailed = false;

		private final long numElements;

		private long failurePos;
		private OperatorState<Long> count;

		public OnceFailingIdentityMapFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = getRuntimeContext().getOperatorState("count", 0L, false);
		}

		@Override
		public Tuple2<Integer, Long> map(Tuple2<Integer, Long> value) throws Exception {
			if (!hasFailed && count.value() >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}
			count.update(count.value() + 1);
			return value;
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

	/**
	 * Sink that emits the output to an evicting queue storing the last {@link #NUM_OUTPUT} elements.
	 * A separate queue is initiated for each group, apply a grouping prior to this operator to avoid
	 * parallel access of the queues.
	 */
	private static class FoldEvictingQueueSink implements SinkFunction<Tuple2<Integer, Long>> {

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
