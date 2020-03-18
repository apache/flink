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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


/**
 * Tests for GlobalAggregate functionality.
 */
public class GlobalAggregateITCase extends AbstractTestBase {

	@Test
	public void testSuccessfulUpdateToGlobalAggregate() throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		streamExecutionEnvironment
			.addSource(new TestSourceFunction(new IntegerAggregateFunction(), false))
			.addSink(new DiscardingSink<>());

		streamExecutionEnvironment.execute();
	}

	@Test
	public void testExceptionThrowingAggregateFunction() throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		streamExecutionEnvironment
			.addSource(new TestSourceFunction(new ExceptionThrowingAggregateFunction(), true))
			.addSink(new DiscardingSink<>());

		streamExecutionEnvironment.execute();
	}

	/**
	 * Source Function that uses updateGlobalAggregate() functionality exposed via StreamingRuntimeContext
	 * to validate communication with JobMaster and test both failure and sucess scenarios.
	 */
	private static class TestSourceFunction extends RichSourceFunction<Integer> {

		private GlobalAggregateManager aggregateManager = null;
		private final AggregateFunction<Integer, Integer, Integer> aggregateFunction;
		private final boolean expectFailures;

		public TestSourceFunction(AggregateFunction<Integer, Integer, Integer> aggregateFunction, boolean expectFailures) {
			this.aggregateFunction = aggregateFunction;
			this.expectFailures = expectFailures;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
			aggregateManager = runtimeContext.getGlobalAggregateManager();
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			Integer expectedAccumulator = 0;
			int exceptionCount = 0;
			for (int i = 0; i < 5; i++) {
				Integer actualAccumlator = 0;
				try {
					actualAccumlator = aggregateManager.updateGlobalAggregate("testAgg", i, aggregateFunction);
					expectedAccumulator += i;
				}
				catch (IOException e) {
					exceptionCount++;
				}

				if (expectFailures) {
					assertEquals(i + 1, exceptionCount);
				}
				else {
					assertEquals(expectedAccumulator, actualAccumlator);
				}
			}
		}

		@Override
		public void cancel() {

		}

	}

	/**
	 * Simple integer aggregate function.
	 */
	private static class IntegerAggregateFunction implements
		AggregateFunction<Integer, Integer, Integer> {

		@Override
		public Integer createAccumulator() {
			return 0;
		}

		@Override
		public Integer add(Integer value, Integer accumulator) {
			return value + accumulator;
		}

		@Override
		public Integer getResult(Integer accumulator) {
			return accumulator;
		}

		@Override
		public Integer merge(Integer accumulatorA, Integer accumulatorB) {
			return add(accumulatorA, accumulatorB);
		}
	}

	/**
	 * Aggregate function that throws NullPointerException.
	 */
	private static class ExceptionThrowingAggregateFunction implements
		AggregateFunction<Integer, Integer, Integer> {

		@Override
		public Integer createAccumulator() {
			return 0;
		}

		@Override
		public Integer add(Integer value, Integer accumulator) {
			throw new NullPointerException("test");
		}

		@Override
		public Integer getResult(Integer accumulator) {
			return accumulator;
		}

		@Override
		public Integer merge(Integer accumulatorA, Integer accumulatorB) {
			return add(accumulatorA, accumulatorB);
		}
	}
}
