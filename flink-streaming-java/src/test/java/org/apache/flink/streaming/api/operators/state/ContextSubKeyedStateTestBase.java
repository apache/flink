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
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.context.ContextStateHelper;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for context sub-keyed states.
 */
public abstract class ContextSubKeyedStateTestBase {

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
	public void testContextSubKeyedReducingState() throws Exception {
		final ReducingStateDescriptor<Long> stateDescr =
			new ReducingStateDescriptor<>("my-state", (a, b) -> a + b, Long.class);

		InternalReducingState<Object, Integer, Long> state =
			(InternalReducingState<Object, Integer, Long>) stateBinder.getOrCreateKeyedState(IntSerializer.INSTANCE, stateDescr);

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		final Long expectedResult = 165L;

		// populate the different namespaces
		//  - abc spreads the values over three namespaces
		//  - def spreads teh values over two namespaces (one empty)
		//  - ghi is empty
		//  - jkl has all elements already in the target namespace
		//  - mno has all elements already in one source namespace

		testOperator.setCurrentKey("abc");
		state.setCurrentNamespace(namespace1);
		state.add(33L);
		state.add(55L);

		state.setCurrentNamespace(namespace2);
		state.add(22L);
		state.add(11L);

		state.setCurrentNamespace(namespace3);
		state.add(44L);

		testOperator.setCurrentKey("def");
		state.setCurrentNamespace(namespace1);
		state.add(11L);
		state.add(44L);

		state.setCurrentNamespace(namespace3);
		state.add(22L);
		state.add(55L);
		state.add(33L);

		testOperator.setCurrentKey("jkl");
		state.setCurrentNamespace(namespace1);
		state.add(11L);
		state.add(22L);
		state.add(33L);
		state.add(44L);
		state.add(55L);

		testOperator.setCurrentKey("mno");
		state.setCurrentNamespace(namespace3);
		state.add(11L);
		state.add(22L);
		state.add(33L);
		state.add(44L);
		state.add(55L);

		testOperator.setCurrentKey("abc");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("def");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("ghi");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertNull(state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("jkl");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("mno");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());
	}

	@Test
	public void testContextAggregatingStateWithImmutableAccumulator() throws Exception {
		final AggregatingStateDescriptor<Long, Long, Long> stateDescr =
			new AggregatingStateDescriptor<>("my-state", new ImmutableAggregatingAddingFunction(), Long.class);

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		final Long expectedResult = 165L;

		InternalAggregatingState<Object, Integer, Long, Long, Long> state =
			(InternalAggregatingState<Object, Integer, Long, Long, Long>) stateBinder.getOrCreateKeyedState(IntSerializer.INSTANCE, stateDescr);

		// populate the different namespaces
		//  - abc spreads the values over three namespaces
		//  - def spreads the values over two namespaces (one empty)
		//  - ghi is empty
		//  - jkl has all elements already in the target namespace
		//  - mno has all elements already in one source namespace

		testOperator.setCurrentKey("abc");
		state.setCurrentNamespace(namespace1);
		state.add(33L);
		state.add(55L);

		state.setCurrentNamespace(namespace2);
		state.add(22L);
		state.add(11L);

		state.setCurrentNamespace(namespace3);
		state.add(44L);

		testOperator.setCurrentKey("def");
		state.setCurrentNamespace(namespace1);
		state.add(11L);
		state.add(44L);

		state.setCurrentNamespace(namespace3);
		state.add(22L);
		state.add(55L);
		state.add(33L);

		testOperator.setCurrentKey("jkl");
		state.setCurrentNamespace(namespace1);
		state.add(11L);
		state.add(22L);
		state.add(33L);
		state.add(44L);
		state.add(55L);

		testOperator.setCurrentKey("mno");
		state.setCurrentNamespace(namespace3);
		state.add(11L);
		state.add(22L);
		state.add(33L);
		state.add(44L);
		state.add(55L);

		testOperator.setCurrentKey("abc");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("def");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("ghi");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertNull(state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("jkl");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("mno");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());
	}

	@Test
	public void testAggregatingStateMergingWithMutableAccumulator() throws Exception {

		final AggregatingStateDescriptor<Long, MutableLong, Long> stateDescr =
			new AggregatingStateDescriptor<>("my-state", new MutableAggregatingAddingFunction(), MutableLong.class);

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		final Long expectedResult = 165L;

		InternalAggregatingState<Object, Integer, Long, Long, Long> state =
			(InternalAggregatingState<Object, Integer, Long, Long, Long>) stateBinder.getOrCreateKeyedState(IntSerializer.INSTANCE, stateDescr);

		// populate the different namespaces
		//  - abc spreads the values over three namespaces
		//  - def spreads the values over two namespaces (one empty)
		//  - ghi is empty
		//  - jkl has all elements already in the target namespace
		//  - mno has all elements already in one source namespace

		testOperator.setCurrentKey("abc");
		state.setCurrentNamespace(namespace1);
		state.add(33L);
		state.add(55L);

		state.setCurrentNamespace(namespace2);
		state.add(22L);
		state.add(11L);

		state.setCurrentNamespace(namespace3);
		state.add(44L);

		testOperator.setCurrentKey("def");
		state.setCurrentNamespace(namespace1);
		state.add(11L);
		state.add(44L);

		state.setCurrentNamespace(namespace3);
		state.add(22L);
		state.add(55L);
		state.add(33L);

		testOperator.setCurrentKey("jkl");
		state.setCurrentNamespace(namespace1);
		state.add(11L);
		state.add(22L);
		state.add(33L);
		state.add(44L);
		state.add(55L);

		testOperator.setCurrentKey("mno");
		state.setCurrentNamespace(namespace3);
		state.add(11L);
		state.add(22L);
		state.add(33L);
		state.add(44L);
		state.add(55L);

		testOperator.setCurrentKey("abc");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("def");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("ghi");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertNull(state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("jkl");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());

		testOperator.setCurrentKey("mno");
		state.mergeNamespaces(namespace1, asList(namespace2, namespace3));
		state.setCurrentNamespace(namespace1);
		assertEquals(expectedResult, state.get());
		state.setCurrentNamespace(namespace2);
		assertNull(state.get());
		state.setCurrentNamespace(namespace3);
		assertNull(state.get());
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
