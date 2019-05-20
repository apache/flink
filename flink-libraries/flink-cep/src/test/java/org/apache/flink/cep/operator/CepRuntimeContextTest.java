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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.cep.operator.CepOperatorTestUtilities.getCepTestHarness;
import static org.apache.flink.cep.operator.CepRuntimeContextTest.MockProcessFunctionAsserter.assertFunction;
import static org.apache.flink.cep.utils.CepOperatorBuilder.createOperatorForNFA;
import static org.apache.flink.cep.utils.EventBuilder.event;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link CepRuntimeContext}.
 */
public class CepRuntimeContextTest extends TestLogger {

	@Test
	public void testCepRuntimeContextIsSetInNFA() throws Exception {

		@SuppressWarnings("unchecked")
		final NFA<Event> mockNFA = mock(NFA.class);

		try (
			OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness = getCepTestHarness(
				createOperatorForNFA(mockNFA).build())) {

			harness.open();
			verify(mockNFA).open(any(CepRuntimeContext.class), any(Configuration.class));
		}
	}

	@Test
	public void testCepRuntimeContextIsSetInProcessFunction() throws Exception {

		final VerifyRuntimeContextProcessFunction processFunction = new VerifyRuntimeContextProcessFunction();

		try (
			OneInputStreamOperatorTestHarness<Event, Event> harness = getCepTestHarness(
				createOperatorForNFA(getSingleElementAlwaysTrueNFA())
					.withFunction(processFunction)
					.build())) {

			harness.open();
			Event record = event().withName("A").build();
			harness.processElement(record, 0);

			assertFunction(processFunction)
				.checkOpenCalled()
				.checkCloseCalled()
				.checkProcessMatchCalled();
		}
	}

	private NFA<Event> getSingleElementAlwaysTrueNFA() {
		return NFACompiler.compileFactory(Pattern.<Event>begin("A"), false).createNFA();
	}

	@Test
	public void testCepRuntimeContext() {
		final String taskName = "foobarTask";
		final MetricGroup metricGroup = new UnregisteredMetricsGroup();
		final int numberOfParallelSubtasks = 42;
		final int indexOfSubtask = 43;
		final int attemptNumber = 1337;
		final String taskNameWithSubtask = "barfoo";
		final ExecutionConfig executionConfig = mock(ExecutionConfig.class);
		final ClassLoader userCodeClassLoader = mock(ClassLoader.class);
		final DistributedCache distributedCache = mock(DistributedCache.class);

		RuntimeContext mockedRuntimeContext = mock(RuntimeContext.class);

		when(mockedRuntimeContext.getTaskName()).thenReturn(taskName);
		when(mockedRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
		when(mockedRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(numberOfParallelSubtasks);
		when(mockedRuntimeContext.getIndexOfThisSubtask()).thenReturn(indexOfSubtask);
		when(mockedRuntimeContext.getAttemptNumber()).thenReturn(attemptNumber);
		when(mockedRuntimeContext.getTaskNameWithSubtasks()).thenReturn(taskNameWithSubtask);
		when(mockedRuntimeContext.getExecutionConfig()).thenReturn(executionConfig);
		when(mockedRuntimeContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
		when(mockedRuntimeContext.getDistributedCache()).thenReturn(distributedCache);

		RuntimeContext runtimeContext = new CepRuntimeContext(mockedRuntimeContext);

		assertEquals(taskName, runtimeContext.getTaskName());
		assertEquals(metricGroup, runtimeContext.getMetricGroup());
		assertEquals(numberOfParallelSubtasks, runtimeContext.getNumberOfParallelSubtasks());
		assertEquals(indexOfSubtask, runtimeContext.getIndexOfThisSubtask());
		assertEquals(attemptNumber, runtimeContext.getAttemptNumber());
		assertEquals(taskNameWithSubtask, runtimeContext.getTaskNameWithSubtasks());
		assertEquals(executionConfig, runtimeContext.getExecutionConfig());
		assertEquals(userCodeClassLoader, runtimeContext.getUserCodeClassLoader());
		assertEquals(distributedCache, runtimeContext.getDistributedCache());

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

	/* Test Utils */
	static class MockProcessFunctionAsserter {
		private final VerifyRuntimeContextProcessFunction function;

		static MockProcessFunctionAsserter assertFunction(VerifyRuntimeContextProcessFunction function) {
			return new MockProcessFunctionAsserter(function);
		}

		private MockProcessFunctionAsserter(VerifyRuntimeContextProcessFunction function) {
			this.function = function;
		}

		MockProcessFunctionAsserter checkOpenCalled() {
			assertThat(function.openCalled, is(true));
			return this;
		}

		MockProcessFunctionAsserter checkCloseCalled() {
			assertThat(function.openCalled, is(true));
			return this;
		}

		MockProcessFunctionAsserter checkProcessMatchCalled() {
			assertThat(function.processMatchCalled, is(true));
			return this;
		}
	}

	private static class VerifyRuntimeContextProcessFunction extends PatternProcessFunction<Event, Event> {

		boolean openCalled = false;
		boolean closeCalled = false;
		boolean processMatchCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			verifyContext();
			openCalled = true;
		}

		private void verifyContext() {
			if (!(getRuntimeContext() instanceof CepRuntimeContext)) {
				fail("Runtime context was not wrapped in CepRuntimeContext");
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
			verifyContext();
			closeCalled = true;
		}

		@Override
		public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Event> out) throws Exception {
			verifyContext();
			processMatchCalled = true;
		}
	}
}
