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

package org.apache.flink.test.preaggregatedaccumulators.utils;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * A test Program with the following job graph:
 * <pre>
 *  A     B
 *   \   /
 *   \  /
 *   \ /
 *    C
 * </pre>
 *
 * <p>Each task of B outputs numbers from 1 to MAX_NUMBER, and each task of C outputs the received numbers.
 * During this process, each task of A will commit a value equals to its subtask index, and numbers
 * match these indices will be filtered out. Pre-aggregated accumulators are used to help B to finish the filter in
 * advance, and C will execute the final filter in case the accumulators do not arrive in time.
 */
public class PreAggregatedAccumulatorProgram {
	private static final int PARALLELISM = 4;
	private static final int MAX_NUMBER = 1000;
	private static final String ACCUMULATOR_NAME = "test";
	private static final IntegerSetAccumulator.IntegerSet EXPECTED_ACCUMULATOR_QUERY_RESULT = new IntegerSetAccumulator.IntegerSet();
	static {
		for (int i = 0; i < PARALLELISM; ++i) {
			EXPECTED_ACCUMULATOR_QUERY_RESULT.getIntegers().add(i);
		}
	}

	/**
	 * The execution contexts of all the executions. Each execution context encapsulates the latches and the result map
	 * used in one execution. These objects can not be created as the variables of the user functions since the user
	 * functions go through multiple copies during the execution.
	 */
	private static final ConcurrentHashMap<String, ExecutionContext> EXECUTION_CONTEXTS = new ConcurrentHashMap<>();

	/**
	 * Executes a test job and acquire the computation result.
	 *
	 * @param executionMode The execution mode required.
	 * @param env The stream execution environment.
	 * @return The result of the execution.
	 */
	public static Map<Integer, Integer> executeJob(ExecutionMode executionMode,
													StreamExecutionEnvironment env) throws Exception {
		env.setParallelism(PARALLELISM);

		// Create a random id for the context of this execution so that different execution will not interfere with others.
		final String contextId = RandomStringUtils.random(16);
		EXECUTION_CONTEXTS.put(contextId, new ExecutionContext(executionMode));

		DataStream<Integer> left = env.addSource(new AccumulatorProducerSourceFunction(contextId)).name("producer");
		DataStream<Integer> right = env.addSource(new AccumulatorConsumerSourceFunction(contextId)).name("consumer");
		left.connect(right).flatMap(new IntegerJoinFunction(contextId));

		env.execute();

		ExecutionContext executionContext = EXECUTION_CONTEXTS.remove(contextId);
		return executionContext.getNumberReceived();
	}

	/**
	 * Compares the execution result. The expected result should be a map of
	 * {PARALLELISM: PARALLELISM, PARALLELISM + 1: PARALLELISM, ..., MAX_NUMBER: PARALLELISM}.
	 *
	 * @param numberReceived The execution result.
	 */
	public static void assertResultConsistent(Map<Integer, Integer> numberReceived) {
		for (int i = 0; i < PARALLELISM; ++i) {
			assertFalse(numberReceived.containsKey(i));
		}

		for (int i = PARALLELISM; i < MAX_NUMBER; ++i) {
			assertEquals(new Integer(PARALLELISM), numberReceived.get(i));
		}
	}

	/**
	 * Execution mode of the test program, which controls the execution order of the two sources.
	 */
	public enum ExecutionMode {
		/**
		 * The accumulator consumer tasks do not send data till the query completing, which ensures
		 * the accumulator takes effective.
		 */
		CONSUMER_WAIT_QUERY_FINISH,

		/**
		 * The accumulator producer tasks do not send data till the consumer tasks finish, which
		 * ensures the accumulator do not take effective.
		 */
		PRODUCER_WAIT_CONSUMER_FINISH,

		/**
		 * Do not limit the execution order of the producer and consumer tasks.
		 */
		RANDOM
	}

	/**
	 * Wrapper class for global objects used in test program, including the latches to coordinate the
	 * execution order and the map to hold the result.
	 */
	public static class ExecutionContext {
		private final ExecutionMode executionMode;
		private final CountDownLatch consumerFinishedLatch = new CountDownLatch(PARALLELISM);
		private final ConcurrentHashMap<Integer, Integer> numberReceived = new ConcurrentHashMap<>();

		ExecutionContext(ExecutionMode executionMode) {
			this.executionMode = executionMode;
		}

		ExecutionMode getExecutionMode() {
			return executionMode;
		}

		CountDownLatch getConsumerFinishedLatch() {
			return consumerFinishedLatch;
		}

		ConcurrentHashMap<Integer, Integer> getNumberReceived() {
			return numberReceived;
		}
	}

	/**
	 * The source who produces the tested accumulator.
	 */
	private static class AccumulatorProducerSourceFunction extends RichParallelSourceFunction<Integer> {
		private final String contextId;

		AccumulatorProducerSourceFunction(String contextId) {
			this.contextId = contextId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addPreAggregatedAccumulator(ACCUMULATOR_NAME, new IntegerSetAccumulator());
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			ExecutionMode executionMode = EXECUTION_CONTEXTS.get(contextId).getExecutionMode();
			CountDownLatch consumerFinished = EXECUTION_CONTEXTS.get(contextId).getConsumerFinishedLatch();

			int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

			if (subtaskIndex == 0 && executionMode == ExecutionMode.PRODUCER_WAIT_CONSUMER_FINISH) {
				consumerFinished.await();
			}

			getRuntimeContext().getPreAggregatedAccumulator(ACCUMULATOR_NAME).add(subtaskIndex);
			getRuntimeContext().commitPreAggregatedAccumulator(ACCUMULATOR_NAME);
		}

		@Override
		public void cancel() {
		}
	}

	/**
	 * The source who consumes the tested accumulator.
	 */
	private static class AccumulatorConsumerSourceFunction extends RichParallelSourceFunction<Integer> {
		private final String contextId;
		private CompletableFuture<Accumulator<Integer, IntegerSetAccumulator.IntegerSet>> filterCompletableFuture;

		private boolean accumulatorCompared = false;

		AccumulatorConsumerSourceFunction(String contextId) {
			this.contextId = contextId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			this.filterCompletableFuture = getRuntimeContext().queryPreAggregatedAccumulator(ACCUMULATOR_NAME);
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			ExecutionMode executionMode = EXECUTION_CONTEXTS.get(contextId).getExecutionMode();
			CountDownLatch consumerFinished = EXECUTION_CONTEXTS.get(contextId).getConsumerFinishedLatch();

			if (executionMode == ExecutionMode.CONSUMER_WAIT_QUERY_FINISH) {
				filterCompletableFuture.join();
			}

			for (int i = 0; i < MAX_NUMBER; ++i) {
				if (filterCompletableFuture.isDone()) {
					final Accumulator<Integer, IntegerSetAccumulator.IntegerSet> accumulator = filterCompletableFuture.get();

					if (!accumulatorCompared) {
						assertEquals(EXPECTED_ACCUMULATOR_QUERY_RESULT, accumulator.getLocalValue());
						accumulatorCompared = true;
					}

					if (!accumulator.getLocalValue().getIntegers().contains(i)) {
						ctx.collect(i);
					}
				} else {
					ctx.collect(i);
				}
			}

			consumerFinished.countDown();
		}

		@Override
		public void cancel() {

		}
	}

	/**
	 * A fake join task to do the final filtering to ensure the output numbers are expected.
	 */
	private static class IntegerJoinFunction implements CoFlatMapFunction<Integer, Integer, Integer> {
		private final String contextId;

		IntegerJoinFunction(String contextId) {
			this.contextId = contextId;
		}

		@Override
		public void flatMap1(Integer value, Collector<Integer> out) {
			// Do nothing.
		}

		@Override
		public void flatMap2(Integer value, Collector<Integer> out) {
			ConcurrentHashMap<Integer, Integer> numberReceived = EXECUTION_CONTEXTS.get(contextId).getNumberReceived();

			if (value >= PARALLELISM) {
				numberReceived.compute(value, (k, v) -> v == null ? 1 : v + 1);
			}
		}
	}
}
