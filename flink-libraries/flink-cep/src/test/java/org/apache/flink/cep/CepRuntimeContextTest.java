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

package org.apache.flink.cep;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.pattern.conditions.RichAndCondition;
import org.apache.flink.cep.pattern.conditions.RichCompositeIterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichNotCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link CepRuntimeContext}.
 */
public class CepRuntimeContextTest {

	@Test
	public void testRichCompositeIterativeCondition() throws Exception {
		RichIterativeCondition<Integer> first = new TestRichIterativeCondition();
		RichIterativeCondition<Integer> second = new TestRichIterativeCondition();
		RichIterativeCondition<Integer> third = new TestRichIterativeCondition();

		RichCompositeIterativeCondition function = new RichCompositeIterativeCondition(first, second, third) {
			@Override
			public boolean filter(Object value, Context ctx) throws Exception {
				return false;
			}
		};
		function.setRuntimeContext(mock(RuntimeContext.class));

		assertTrue(first.getRuntimeContext() instanceof CepRuntimeContext);
		assertTrue(second.getRuntimeContext() instanceof CepRuntimeContext);
		assertTrue(third.getRuntimeContext() instanceof CepRuntimeContext);
	}

	@Test
	public void testRichAndCondition() throws Exception {
		RichIterativeCondition<Integer> left = new TestRichIterativeCondition();
		RichIterativeCondition<Integer> right = new TestRichIterativeCondition();

		RichAndCondition function = new RichAndCondition<>(left, right);
		function.setRuntimeContext(mock(RuntimeContext.class));

		assertTrue(left.getRuntimeContext() instanceof CepRuntimeContext);
		assertTrue(right.getRuntimeContext() instanceof CepRuntimeContext);
	}

	@Test
	public void testRichOrCondition() throws Exception {
		RichIterativeCondition<Integer> left = new TestRichIterativeCondition();
		RichIterativeCondition<Integer> right = new TestRichIterativeCondition();

		RichOrCondition function = new RichOrCondition<>(left, right);
		function.setRuntimeContext(mock(RuntimeContext.class));

		assertTrue(left.getRuntimeContext() instanceof CepRuntimeContext);
		assertTrue(right.getRuntimeContext() instanceof CepRuntimeContext);
	}

	@Test
	public void testRichNotCondition() {
		RichIterativeCondition<Integer> original = new TestRichIterativeCondition();

		RichNotCondition function = new RichNotCondition<>(original);
		function.setRuntimeContext(mock(RuntimeContext.class));

		assertTrue(original.getRuntimeContext() instanceof CepRuntimeContext);
	}

	@Test
	public void testRichPatternSelectFunction() {
		verifyRuntimeContext(new TestRichPatternSelectFunction());
	}

	@Test
	public void testRichPatternFlatSelectFunction() {
		verifyRuntimeContext(new TestRichPatternFlatSelectFunction());
	}

	@Test
	public void testRichIterativeCondition() {
		verifyRuntimeContext(new TestRichIterativeCondition());
	}

	private void verifyRuntimeContext(final RichFunction function) {
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

		assertTrue(runtimeContext instanceof CepRuntimeContext);
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
			runtimeContext.getReducingState(new ReducingStateDescriptor<>(
				"foobar",
				mock(ReduceFunction.class),
				Integer.class));
			fail("Expected getReducingState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getAggregatingState(new AggregatingStateDescriptor<>(
				"foobar",
				mock(AggregateFunction.class),
				Integer.class));
			fail("Expected getAggregatingState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getFoldingState(new FoldingStateDescriptor<>(
				"foobar",
				0,
				mock(FoldFunction.class),
				Integer.class));
			fail("Expected getFoldingState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.getMapState(new MapStateDescriptor<>("foobar", Integer.class, String.class));
			fail("Expected getMapState to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}

		try {
			runtimeContext.addAccumulator("foobar", mock(Accumulator.class));
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
			runtimeContext.getAllAccumulators();
			fail("Expected getAllAccumulators to fail with unsupported operation exception.");
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
			runtimeContext.getBroadcastVariableWithInitializer(
				"foobar",
				mock(BroadcastVariableInitializer.class));
			fail("Expected getBroadcastVariableWithInitializer to fail with unsupported operation exception.");
		} catch (UnsupportedOperationException e) {
			// expected
		}
	}

	private static class TestRichIterativeCondition extends RichIterativeCondition<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Integer value, Context<Integer> ctx) throws Exception {
			return false;
		}
	}

	private static class TestRichPatternSelectFunction extends RichPatternSelectFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer select(Map<String, List<Integer>> pattern) throws Exception {
			return null;
		}
	}

	private static class TestRichPatternFlatSelectFunction extends RichPatternFlatSelectFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatSelect(Map<String, List<Integer>> pattern, Collector<Integer> out) throws Exception {
			// no op
		}
	}
}
