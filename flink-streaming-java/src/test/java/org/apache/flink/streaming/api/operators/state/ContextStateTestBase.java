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

package org.apache.flink.streaming.api.operators.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.context.ContextStateHelper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for context keyed states.
 */
public abstract class ContextStateTestBase {

	protected KeyedOneInputStreamOperatorTestHarness<String, String, String> testHarness;

	protected IdleOperator<String, String> testOperator;

	protected ContextStateHelper stateBinder;

	protected abstract StateBackend getStateBackend();

	@Before
	public void init() throws Exception {
		testOperator = new IdleOperator<>();

		testHarness = new KeyedOneInputStreamOperatorTestHarness<>(testOperator, new IdentityKeySelector<>(), BasicTypeInfo.STRING_TYPE_INFO);
		testHarness.setStateBackend(getStateBackend());
		testHarness.open();

		stateBinder = new ContextStateHelper(
			testOperator.getKeyContext(), testOperator.getExecutionConfig(), testOperator.getInternalStateBackend());
	}

	@After
	public void close() throws Exception{
		testHarness.close();
	}

	@Test
	public void testContextReducingState() throws Exception {
		final ReducingStateDescriptor<Long> stateDescr =
			new ReducingStateDescriptor<>("my-state", (a, b) -> a + b, Long.class);

		ReducingState<Long> state = stateBinder.createReducingState(stateDescr);

		testOperator.setCurrentKey("abc");
		assertNull(state.get());

		testOperator.setCurrentKey("def");
		assertNull(state.get());
		state.add(17L);
		assertEquals(17L, state.get().longValue());
		state.add(11L);
		assertEquals(28L, state.get().longValue());

		testOperator.setCurrentKey("abc");
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		assertNull(state.get());
		state.add(1L);
		assertEquals(1L, state.get().longValue());
		state.add(2L);
		assertEquals(3L, state.get().longValue());

		testOperator.setCurrentKey("def");
		assertEquals(28L, state.get().longValue());
		state.clear();
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		state.add(3L);
		assertEquals(6L, state.get().longValue());
		state.add(2L);
		assertEquals(8L, state.get().longValue());
		state.add(1L);
		assertEquals(9L, state.get().longValue());

		testOperator.setCurrentKey("def");
		assertNull(state.get());
	}

	@Test
	public void testContextFoldingState() throws Exception {
		FoldingStateDescriptor<Integer, String> stateDescr = new FoldingStateDescriptor<>("id",
			"Fold-Initial:",
			new AppendingFold(),
			String.class);

		FoldingState<Integer, String> state = stateBinder.createFoldingState(stateDescr);

		// some modifications to the state
		testOperator.setCurrentKey("1");
		assertNull(state.get());
		state.add(1);
		assertEquals("Fold-Initial:,1", state.get());
		testOperator.setCurrentKey("2");
		assertNull(state.get());
		state.add(2);
		assertEquals("Fold-Initial:,2", state.get());

		// make some more modifications
		testOperator.setCurrentKey("1");
		state.clear();
		state.add(101);
		assertEquals("Fold-Initial:,101", state.get());
		testOperator.setCurrentKey("2");
		state.add(102);
		assertEquals("Fold-Initial:,2,102", state.get());
		testOperator.setCurrentKey("3");
		state.add(103);
		assertEquals("Fold-Initial:,103", state.get());
	}

	@Test
	public void testContextAggregatingStateWithImmutableAccumulator() throws Exception {
		final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
			new AggregatingStateDescriptor<>("my-state", new ImmutableAggregatingAddingFunction(), Long.class);

		AggregatingState<Long, Long> state = stateBinder.createAggregatingState(stateDescr);

		testOperator.setCurrentKey("abc");
		assertNull(state.get());

		testOperator.setCurrentKey("def");
		assertNull(state.get());
		state.add(17L);
		assertEquals(17L, state.get().longValue());
		state.add(11L);
		assertEquals(28L, state.get().longValue());

		testOperator.setCurrentKey("abc");
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		assertNull(state.get());
		state.add(1L);
		assertEquals(1L, state.get().longValue());
		state.add(2L);
		assertEquals(3L, state.get().longValue());

		testOperator.setCurrentKey("def");
		assertEquals(28L, state.get().longValue());
		state.clear();
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		state.add(3L);
		assertEquals(6L, state.get().longValue());
		state.add(2L);
		assertEquals(8L, state.get().longValue());
		state.add(1L);
		assertEquals(9L, state.get().longValue());

		testOperator.setCurrentKey("def");
		assertNull(state.get());
	}

	@Test
	public void testAggregatingStateWithMutableAccumulator() throws Exception {

		final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
			new AggregatingStateDescriptor<>("my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

		AggregatingState<Long, Long> state = stateBinder.createAggregatingState(stateDescr);

		testOperator.setCurrentKey("abc");
		assertNull(state.get());

		testOperator.setCurrentKey("def");
		assertNull(state.get());
		state.add(17L);
		state.add(11L);
		assertEquals(28L, state.get().longValue());

		testOperator.setCurrentKey("abc");
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		assertNull(state.get());
		state.add(1L);
		state.add(2L);

		testOperator.setCurrentKey("def");
		assertEquals(28L, state.get().longValue());
		state.clear();
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		state.add(3L);
		state.add(2L);
		state.add(1L);

		testOperator.setCurrentKey("def");
		assertNull(state.get());

		testOperator.setCurrentKey("g");
		assertEquals(9L, state.get().longValue());
		state.clear();
	}

	private static class IdleOperator<IN, OUT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT> {

		@Override
		public void processElement(StreamRecord<IN> elements) throws Exception {

		}

		@Override
		public void endInput() throws Exception {

		}
	}

	private static final class IdentityKeySelector<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) {
			return value;
		}
	}

	private static class AppendingFold implements FoldFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String fold(String acc, Integer value) throws Exception {
			return acc + "," + value;
		}
	}

	@SuppressWarnings("serial")
	private static class ImmutableAggregatingAddingFunction implements AggregateFunction<Long, Long, Long> {

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(Long value, Long accumulator) {
			return accumulator += value;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	@SuppressWarnings("serial")
	private static class MutableAggregatingAddingFunction implements AggregateFunction<Long, MutableLong, Long> {

		@Override
		public MutableLong createAccumulator() {
			return new MutableLong();
		}

		@Override
		public MutableLong add(Long value, MutableLong accumulator) {
			accumulator.value += value;
			return accumulator;
		}

		@Override
		public Long getResult(MutableLong accumulator) {
			return accumulator.value;
		}

		@Override
		public MutableLong merge(MutableLong a, MutableLong b) {
			a.value += b.value;
			return a;
		}
	}

	private static final class MutableLong {
		long value;
	}
}
