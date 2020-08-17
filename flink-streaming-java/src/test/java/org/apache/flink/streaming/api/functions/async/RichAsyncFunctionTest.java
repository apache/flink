/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link RichAsyncFunction}.
 */
public class RichAsyncFunctionTest {

	/**
	 * Test the set of iteration runtime context methods in the context of a
	 * {@link RichAsyncFunction}.
	 */
	@Test
	public void testIterationRuntimeContext() throws Exception {
		RichAsyncFunction<Integer, Integer> function = new RichAsyncFunction<Integer, Integer>() {
			private static final long serialVersionUID = -2023923961609455894L;

			@Override
			public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
				// no op
			}
		};

		int superstepNumber = 42;

		IterationRuntimeContext mockedIterationRuntimeContext = mock(IterationRuntimeContext.class);
		when(mockedIterationRuntimeContext.getSuperstepNumber()).thenReturn(superstepNumber);
		function.setRuntimeContext(mockedIterationRuntimeContext);

		IterationRuntimeContext iterationRuntimeContext = function.getIterationRuntimeContext();

		assertEquals(superstepNumber, iterationRuntimeContext.getSuperstepNumber());

		try {
			iterationRuntimeContext.getIterationAggregator("foobar");
			fail("Expected getIterationAggregator to fail with unsupported operation exception");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			iterationRuntimeContext.getPreviousIterationAggregate("foobar");
			fail("Expected getPreviousIterationAggregator to fail with unsupported operation exception");
		} catch (UnsupportedOperationException e) {
			// expected
		}
	}

	/**
	 * Test the set of runtime context methods in the context of a {@link RichAsyncFunction}.
	 */
	@Test
	public void testRuntimeContext() throws Exception {
		RichAsyncFunction<Integer, Integer> function = new RichAsyncFunction<Integer, Integer>() {
			private static final long serialVersionUID = 1707630162838967972L;

			@Override
			public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
				// no op
			}
		};

		final String taskName = "foobarTask";
		final MetricGroup metricGroup = new UnregisteredMetricsGroup();
		final int numberOfParallelSubtasks = 42;
		final int indexOfSubtask = 43;
		final int attemptNumber = 1337;
		final String taskNameWithSubtask = "barfoo";
		final ExecutionConfig executionConfig = mock(ExecutionConfig.class);
		final ClassLoader userCodeClassLoader = mock(ClassLoader.class);

		RuntimeContext mockedRuntimeContext = mock(RuntimeContext.class);

		when(mockedRuntimeContext.getTaskName()).thenReturn(taskName);
		when(mockedRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
		when(mockedRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(numberOfParallelSubtasks);
		when(mockedRuntimeContext.getIndexOfThisSubtask()).thenReturn(indexOfSubtask);
		when(mockedRuntimeContext.getAttemptNumber()).thenReturn(attemptNumber);
		when(mockedRuntimeContext.getTaskNameWithSubtasks()).thenReturn(taskNameWithSubtask);
		when(mockedRuntimeContext.getExecutionConfig()).thenReturn(executionConfig);
		when(mockedRuntimeContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);

		function.setRuntimeContext(mockedRuntimeContext);

		RuntimeContext runtimeContext = function.getRuntimeContext();

		assertEquals(taskName, runtimeContext.getTaskName());
		assertEquals(metricGroup, runtimeContext.getMetricGroup());
		assertEquals(numberOfParallelSubtasks, runtimeContext.getNumberOfParallelSubtasks());
		assertEquals(indexOfSubtask, runtimeContext.getIndexOfThisSubtask());
		assertEquals(attemptNumber, runtimeContext.getAttemptNumber());
		assertEquals(taskNameWithSubtask, runtimeContext.getTaskNameWithSubtasks());
		assertEquals(executionConfig, runtimeContext.getExecutionConfig());
		assertEquals(userCodeClassLoader, runtimeContext.getUserCodeClassLoader());

		try {
			runtimeContext.getDistributedCache();
			fail("Expected getDistributedCached to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getState(new ValueStateDescriptor<>("foobar", Integer.class, 42));
			fail("Expected getState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getListState(new ListStateDescriptor<>("foobar", Integer.class));
			fail("Expected getListState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getReducingState(new ReducingStateDescriptor<>("foobar", new ReduceFunction<Integer>() {
				private static final long serialVersionUID = 2136425961884441050L;

				@Override
				public Integer reduce(Integer value1, Integer value2) throws Exception {
					return value1;
				}
			}, Integer.class));
			fail("Expected getReducingState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getAggregatingState(new AggregatingStateDescriptor<>("foobar", new AggregateFunction<Integer, Integer, Integer>() {

				@Override
				public Integer createAccumulator() {
					return null;
				}

				@Override
				public Integer add(Integer value, Integer accumulator) {
					return null;
				}

				@Override
				public Integer getResult(Integer accumulator) {
					return null;
				}

				@Override
				public Integer merge(Integer a, Integer b) {
					return null;
				}
			}, Integer.class));
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getMapState(new MapStateDescriptor<>("foobar", Integer.class, String.class));
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.addAccumulator("foobar", new Accumulator<Integer, Integer>() {
				private static final long serialVersionUID = -4673320336846482358L;

				@Override
				public void add(Integer value) {
					// no op
				}

				@Override
				public Integer getLocalValue() {
					return null;
				}

				@Override
				public void resetLocal() {

				}

				@Override
				public void merge(Accumulator<Integer, Integer> other) {

				}

				@Override
				public Accumulator<Integer, Integer> clone() {
					return null;
				}
			});
			fail("Expected addAccumulator to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getAccumulator("foobar");
			fail("Expected getAccumulator to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getIntCounter("foobar");
			fail("Expected getIntCounter to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getLongCounter("foobar");
			fail("Expected getLongCounter to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getDoubleCounter("foobar");
			fail("Expected getDoubleCounter to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getHistogram("foobar");
			fail("Expected getHistogram to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.hasBroadcastVariable("foobar");
			fail("Expected hasBroadcastVariable to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getBroadcastVariable("foobar");
			fail("Expected getBroadcastVariable to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getBroadcastVariableWithInitializer("foobar", new BroadcastVariableInitializer<Object, Object>() {
				@Override
				public Object initializeBroadcastVariable(Iterable<Object> data) {
					return null;
				}
			});
			fail("Expected getBroadcastVariableWithInitializer to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}
	}
}
