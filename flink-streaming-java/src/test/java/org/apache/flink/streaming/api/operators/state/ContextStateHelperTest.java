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
import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.context.ContextStateHelper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Tests for {@link ContextStateHelper}.
 */
public class ContextStateHelperTest {

	@Test
	public void testStateCache() throws Exception {
		final IdleOperator<Integer, Integer> operator = new IdleOperator<>();
		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		final ContextStateHelper contextStateHelper = new ContextStateHelper(
			operator.getKeyContext(), operator.getExecutionConfig(), operator.getInternalStateBackend());

		TypeSerializer<Integer> nameSpaceSerializer = IntSerializer.INSTANCE;

		ValueStateDescriptor<Integer> valueStateDescriptor1 =
			new ValueStateDescriptor<>("v1", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor2 =
			new ValueStateDescriptor<>("v2", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor3 =
			new ValueStateDescriptor<>("v3", IntSerializer.INSTANCE);

		assertNull(contextStateHelper.getLastState());
		assertNull(contextStateHelper.getLastStateName());

		ValueState<Integer> valueState1 =
			contextStateHelper.getPartitionedState(0, nameSpaceSerializer, valueStateDescriptor1);
		assertEquals(valueStateDescriptor1.getName(), contextStateHelper.getLastStateName());
		assertSame(valueState1, contextStateHelper.getLastState());

		ValueState<Integer> valueState2 =
			contextStateHelper.getPartitionedState(0, nameSpaceSerializer, valueStateDescriptor2);
		assertNotSame(valueState1, valueState2);
		assertEquals(valueStateDescriptor2.getName(), contextStateHelper.getLastStateName());
		assertSame(valueState2, contextStateHelper.getLastState());

		assertSame(valueState1,
			contextStateHelper.getPartitionedState(1, nameSpaceSerializer, valueStateDescriptor1));
		assertEquals(valueStateDescriptor1.getName(), contextStateHelper.getLastStateName());
		assertSame(valueState1, contextStateHelper.getLastState());

		ValueState<Integer> valueState3 =
			contextStateHelper.getPartitionedState(0, nameSpaceSerializer, valueStateDescriptor3);
		assertNotSame(valueState1, valueState3);
		assertNotSame(valueState2, valueState3);
		assertEquals(valueStateDescriptor3.getName(), contextStateHelper.getLastStateName());
		assertSame(valueState3, contextStateHelper.getLastState());

		testHarness.close();
	}

	@Test
	public void testCreateKeyedState() throws Exception {
		final IdleOperator<Integer, Integer> idleOperator = new IdleOperator<>();

		final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(idleOperator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		final ContextStateHelper binder = new ContextStateHelper(idleOperator.getKeyContext(), idleOperator.getExecutionConfig(), idleOperator.getInternalStateBackend());

		ValueStateDescriptor<Integer> valueStateDescriptor1 =
			new ValueStateDescriptor<>("v1", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor2 =
			new ValueStateDescriptor<>("v2", IntSerializer.INSTANCE);

		ValueState<Integer> valueState1;

		ValueState<Integer> valueState2;

		ListStateDescriptor<Integer> listStateDescriptor1 =
			new ListStateDescriptor<>("l1", IntSerializer.INSTANCE);

		ListStateDescriptor<Integer> listStateDescriptor2 =
			new ListStateDescriptor<>("l2", IntSerializer.INSTANCE);

		ListState<Integer> listState1;

		ListState<Integer> listState2;

		MapStateDescriptor<Integer, Integer> mapStateDescriptor1 =
			new MapStateDescriptor<>("m1", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		MapStateDescriptor<Integer, Integer> mapStateDescriptor2 =
			new MapStateDescriptor<>("m2", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		MapState<Integer, Integer> mapState1;

		MapState<Integer, Integer> mapState2;

		SortedMapStateDescriptor<Integer, Integer> sortedMapStateDescriptor1 =
			new SortedMapStateDescriptor<>("sm1", new NaturalComparator<>(), IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		SortedMapStateDescriptor<Integer, Integer> sortedMapStateDescriptor2 =
			new SortedMapStateDescriptor<>("sm2", new NaturalComparator<>(), IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		SortedMapState<Integer, Integer> sortedMapState1;

		SortedMapState<Integer, Integer> sortedMapState2;

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor1 =
			new FoldingStateDescriptor<>("f1", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor2 =
			new FoldingStateDescriptor<>("f2", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingState<Integer, Integer> foldingState1;

		FoldingState<Integer, Integer> foldingState2;

		ReducingStateDescriptor<Integer> reducingStateDescriptor1 =
			new ReducingStateDescriptor<>("r1", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingStateDescriptor<Integer>reducingStateDescriptor2 =
			new ReducingStateDescriptor<>("r2", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingState<Integer> reducingState1;

		ReducingState<Integer> reducingState2;

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor1 =
			new AggregatingStateDescriptor<>("a1", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor2 =
			new AggregatingStateDescriptor<>("a2", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingState<Integer, Integer> aggregatingState1;

		AggregatingState<Integer, Integer> aggregatingState2;

		valueState1 = binder.createValueState(valueStateDescriptor1);
		valueState2 = binder.createValueState(valueStateDescriptor2);
		assertNotSame(valueState1, valueState2);
		assertSame(valueState1,
			binder.createValueState(valueStateDescriptor1));
		assertSame(valueState2,
			binder.createValueState(valueStateDescriptor2));

		listState1 = binder.createListState(listStateDescriptor1);
		listState2 = binder.createListState(listStateDescriptor2);
		assertNotSame(listState1, listState2);
		assertSame(listState1,
			binder.createListState(listStateDescriptor1));
		assertSame(listState2,
			binder.createListState(listStateDescriptor2));

		mapState1 = binder.createMapState(mapStateDescriptor1);
		mapState2 = binder.createMapState(mapStateDescriptor2);
		assertNotSame(mapState1, mapState2);
		assertSame(mapState1,
			binder.createMapState(mapStateDescriptor1));
		assertSame(mapState2,
			binder.createMapState(mapStateDescriptor2));

		sortedMapState1 = binder.createSortedMapState(sortedMapStateDescriptor1);
		sortedMapState2 = binder.createSortedMapState(sortedMapStateDescriptor2);
		assertNotSame(sortedMapState1, sortedMapState2);
		assertSame(sortedMapState1,
			binder.createSortedMapState(sortedMapStateDescriptor1));
		assertSame(sortedMapState2,
			binder.createSortedMapState(sortedMapStateDescriptor2));

		foldingState1 = binder.createFoldingState(foldingStateDescriptor1);
		foldingState2 = binder.createFoldingState(foldingStateDescriptor2);
		assertNotSame(foldingState1, foldingState2);
		assertSame(foldingState1,
			binder.createFoldingState(foldingStateDescriptor1));
		assertSame(foldingState2,
			binder.createFoldingState(foldingStateDescriptor2));

		reducingState1 = binder.createReducingState(reducingStateDescriptor1);
		reducingState2 = binder.createReducingState(reducingStateDescriptor2);
		assertNotSame(reducingState1, reducingState2);
		assertSame(reducingState1,
			binder.createReducingState(reducingStateDescriptor1));
		assertSame(reducingState2,
			binder.createReducingState(reducingStateDescriptor2));

		aggregatingState1 = binder.createAggregatingState(aggregatingStateDescriptor1);
		aggregatingState2 = binder.createAggregatingState(aggregatingStateDescriptor2);
		assertNotSame(aggregatingState1, aggregatingState2);
		assertSame(aggregatingState1,
			binder.createAggregatingState(aggregatingStateDescriptor1));
		assertSame(aggregatingState2,
			binder.createAggregatingState(aggregatingStateDescriptor2));

		testHarness.close();
	}

	@Test
	public void testCreateSubKeyedState() throws Exception {
		final IdleOperator<Integer, Integer> idleOperator = new IdleOperator<>();

		final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(idleOperator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.open();

		final ContextStateHelper contextStateHelper = new ContextStateHelper(
			idleOperator.getKeyContext(), idleOperator.getExecutionConfig(), idleOperator.getInternalStateBackend());

		TypeSerializer<Integer> namespaceSerializer = IntSerializer.INSTANCE;

		ValueStateDescriptor<Integer> valueStateDescriptor1 =
			new ValueStateDescriptor<>("v1", IntSerializer.INSTANCE);

		ValueStateDescriptor<Integer> valueStateDescriptor2 =
			new ValueStateDescriptor<>("v2", IntSerializer.INSTANCE);

		ValueState<Integer> valueState1;

		ValueState<Integer> valueState2;

		ListStateDescriptor<Integer> listStateDescriptor1 =
			new ListStateDescriptor<>("l1", IntSerializer.INSTANCE);

		ListStateDescriptor<Integer> listStateDescriptor2 =
			new ListStateDescriptor<>("l2", IntSerializer.INSTANCE);

		ListState<Integer> listState1;

		ListState<Integer> listState2;

		MapStateDescriptor<Integer, Integer> mapStateDescriptor1 =
			new MapStateDescriptor<>("m1", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		MapStateDescriptor<Integer, Integer> mapStateDescriptor2 =
			new MapStateDescriptor<>("m2", IntSerializer.INSTANCE, IntSerializer.INSTANCE);

		MapState<Integer, Integer> mapState1;

		MapState<Integer, Integer> mapState2;

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor1 =
			new FoldingStateDescriptor<>("f1", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor2 =
			new FoldingStateDescriptor<>("f2", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingState<Integer, Integer> foldingState1;

		FoldingState<Integer, Integer> foldingState2;

		ReducingStateDescriptor<Integer>reducingStateDescriptor1 =
			new ReducingStateDescriptor<>("r1", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingStateDescriptor<Integer>reducingStateDescriptor2 =
			new ReducingStateDescriptor<>("r2", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingState<Integer> reducingState1;

		ReducingState<Integer> reducingState2;

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor1 =
			new AggregatingStateDescriptor<>("a1", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor2 =
			new AggregatingStateDescriptor<>("a2", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingState<Integer, Integer> aggregatingState1;

		AggregatingState<Integer, Integer> aggregatingState2;

		// test getSubKeyedStateWithNamespace
		valueState1 = contextStateHelper.getPartitionedState(0, namespaceSerializer, valueStateDescriptor1);
		valueState2 = contextStateHelper.getPartitionedState(0, namespaceSerializer, valueStateDescriptor2);
		assertNotSame(valueState1, valueState2);
		assertSame(valueState1,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, valueStateDescriptor1));
		assertSame(valueState2,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, valueStateDescriptor2));

		listState1 = contextStateHelper.getPartitionedState(0, namespaceSerializer, listStateDescriptor1);
		listState2 = contextStateHelper.getPartitionedState(0, namespaceSerializer, listStateDescriptor2);
		assertNotSame(listState1, listState2);
		assertSame(listState1,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, listStateDescriptor1));
		assertSame(listState2,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, listStateDescriptor2));

		mapState1 = contextStateHelper.getPartitionedState(0, namespaceSerializer, mapStateDescriptor1);
		mapState2 = contextStateHelper.getPartitionedState(0, namespaceSerializer, mapStateDescriptor2);
		assertNotSame(mapState1, mapState2);
		assertSame(mapState1,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, mapStateDescriptor1));
		assertSame(mapState2,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, mapStateDescriptor2));

		foldingState1 = contextStateHelper.getPartitionedState(0, namespaceSerializer, foldingStateDescriptor1);
		foldingState2 = contextStateHelper.getPartitionedState(0, namespaceSerializer, foldingStateDescriptor2);
		assertNotSame(foldingState1, foldingState2);
		assertSame(foldingState1,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, foldingStateDescriptor1));
		assertSame(foldingState2,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, foldingStateDescriptor2));

		reducingState1 = contextStateHelper.getPartitionedState(0, namespaceSerializer, reducingStateDescriptor1);
		reducingState2 = contextStateHelper.getPartitionedState(0, namespaceSerializer, reducingStateDescriptor2);
		assertNotSame(reducingState1, reducingState2);
		assertSame(reducingState1,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, reducingStateDescriptor1));
		assertSame(reducingState2,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, reducingStateDescriptor2));

		aggregatingState1 = contextStateHelper.getPartitionedState(0, namespaceSerializer, aggregatingStateDescriptor1);
		aggregatingState2 = contextStateHelper.getPartitionedState(0, namespaceSerializer, aggregatingStateDescriptor2);
		assertNotSame(aggregatingState1, aggregatingState2);
		assertSame(aggregatingState1,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, aggregatingStateDescriptor1));
		assertSame(aggregatingState2,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, aggregatingStateDescriptor2));

		// test getContextSubKeyedAppendingState
		ListStateDescriptor<Integer> listStateDescriptor3 =
			new ListStateDescriptor<>("l3", IntSerializer.INSTANCE);

		ListState listState3;

		FoldingStateDescriptor<Integer, Integer> foldingStateDescriptor3 =
			new FoldingStateDescriptor<>("f3", 0, new AddFoldFunction(), IntSerializer.INSTANCE);

		FoldingState foldingState3;

		ReducingStateDescriptor<Integer>reducingStateDescriptor3 =
			new ReducingStateDescriptor<>("r3", new AddReduceFunction(), IntSerializer.INSTANCE);

		ReducingState reducingState3;

		AggregatingStateDescriptor<Integer, Integer, Integer> aggregatingStateDescriptor3 =
			new AggregatingStateDescriptor<>("a3", new AddAggFunction(), IntSerializer.INSTANCE);

		AggregatingState aggregatingState3;

		listState3 = contextStateHelper.getOrCreateKeyedState(namespaceSerializer, listStateDescriptor3);
		assertNotSame(listState1, listState3);
		assertNotSame(listState2, listState3);
		assertSame(listState1,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, listStateDescriptor1));
		assertSame(listState2,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, listStateDescriptor2));
		assertSame(listState3,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, listStateDescriptor3));
		assertSame(listState3,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, listStateDescriptor3));

		foldingState3 = contextStateHelper.getOrCreateKeyedState(namespaceSerializer, foldingStateDescriptor3);
		assertNotSame(foldingState1, foldingState3);
		assertNotSame(foldingState2, foldingState3);
		assertSame(foldingState1,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, foldingStateDescriptor1));
		assertSame(foldingState2,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, foldingStateDescriptor2));
		assertSame(foldingState3,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, foldingStateDescriptor3));
		assertSame(foldingState3,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, foldingStateDescriptor3));

		reducingState3 = contextStateHelper.getOrCreateKeyedState(namespaceSerializer, reducingStateDescriptor3);
		assertNotSame(reducingState1, reducingState3);
		assertNotSame(reducingState2, reducingState3);
		assertSame(reducingState1,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, reducingStateDescriptor1));
		assertSame(reducingState2,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, reducingStateDescriptor2));
		assertSame(reducingState3,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, reducingStateDescriptor3));
		assertSame(reducingState3,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, reducingStateDescriptor3));

		aggregatingState3 = contextStateHelper.getOrCreateKeyedState(namespaceSerializer, aggregatingStateDescriptor3);
		assertNotSame(aggregatingState1, aggregatingState3);
		assertNotSame(aggregatingState2, aggregatingState3);
		assertSame(aggregatingState1,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, aggregatingStateDescriptor1));
		assertSame(aggregatingState2,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, aggregatingStateDescriptor2));
		assertSame(aggregatingState3,
			contextStateHelper.getOrCreateKeyedState(namespaceSerializer, aggregatingStateDescriptor3));
		assertSame(aggregatingState3,
			contextStateHelper.getPartitionedState(0, namespaceSerializer, aggregatingStateDescriptor3));
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

	private static final class AddFoldFunction implements FoldFunction<Integer, Integer>	{

		@Override
		public Integer fold(Integer acc, Integer value) {
			return acc + value;
		}
	}

	private static final class AddReduceFunction implements ReduceFunction<Integer> {

		@Override
		public Integer reduce(Integer value1, Integer value2) {
			return value1 + value2;
		}
	}

	private static final class AddAggFunction implements AggregateFunction<Integer, Integer, Integer> {

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
		public Integer merge(Integer a, Integer b) {
			return a + b;
		}
	}

}
