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

package org.apache.flink.streaming.api.state;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.LocalStateHandle.LocalStateHandleProvider;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;

public class StatefulFunctionTest {

	@SuppressWarnings("unchecked")
	@Test
	public void simpleStateTest() throws Exception {

		StatefulMapper mapper = new StatefulMapper();
		MockContext context = new MockContext(false, mapper);
		mapper.setRuntimeContext(context);
		mapper.open(null);

		assertEquals(Arrays.asList("1", "2", "3", "4", "5"),
				applyOnSequence(mapper, 1, 5, context.state));
		assertEquals((Integer) 5, context.state.getState());

		byte[] serializedState = InstantiationUtil.serializeObject(context.state
				.snapshotState(1, 1));

		StatefulMapper restoredMapper = new StatefulMapper();
		MockContext restoredContext = new MockContext(false, restoredMapper);
		restoredMapper.setRuntimeContext(context);
		restoredMapper.open(null);

		assertEquals(null, restoredContext.state.getState());

		Map<Serializable, StateHandle<Integer>> deserializedState = (Map<Serializable, StateHandle<Integer>>) InstantiationUtil
				.deserializeObject(serializedState, Thread.currentThread().getContextClassLoader());

		restoredContext.state.restoreState(deserializedState);

		assertEquals((Integer) 5, restoredContext.state.getState());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void partitionedStateTest() throws Exception {
		StatefulMapper mapper = new StatefulMapper();
		MockContext context = new MockContext(true, mapper);
		mapper.setRuntimeContext(context);
		mapper.open(null);

		assertEquals(Arrays.asList("1", "2", "3", "4", "5"),
				applyOnSequence(mapper, 1, 5, context.state));
		assertEquals(ImmutableMap.of(0, 2, 1, 3), context.state.getPartitionedState());

		byte[] serializedState = InstantiationUtil.serializeObject(context.state
				.snapshotState(1, 1));

		StatefulMapper restoredMapper = new StatefulMapper();
		MockContext restoredContext = new MockContext(true, restoredMapper);
		restoredMapper.setRuntimeContext(context);
		restoredMapper.open(null);

		assertEquals(null, restoredContext.state.getState());

		Map<Serializable, StateHandle<Integer>> deserializedState = (Map<Serializable, StateHandle<Integer>>) InstantiationUtil
				.deserializeObject(serializedState, Thread.currentThread().getContextClassLoader());

		restoredContext.state.restoreState(deserializedState);

		assertEquals(ImmutableMap.of(0, 2, 1, 3), restoredContext.state.getPartitionedState());

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> List<T> applyOnSequence(MapFunction<Integer, T> mapper, int from, int to,
			StreamOperatorState state) throws Exception {
		List<T> output = new ArrayList<T>();
		for (int i = from; i <= to; i++) {
			if (state instanceof PartitionedStreamOperatorState) {
				((PartitionedStreamOperatorState) state).setCurrentInput(i);
			}
			output.add(mapper.map(i));
		}
		return output;
	}

	public static class ModKey implements KeySelector<Integer, Serializable> {

		private static final long serialVersionUID = 4193026742083046736L;

		int base;

		public ModKey(int base) {
			this.base = base;
		}

		@Override
		public Integer getKey(Integer value) throws Exception {
			return value % base;
		}

	}

	public static class StatefulMapper extends RichMapFunction<Integer, String> {

		private static final long serialVersionUID = -9007873655253339356L;
		OperatorState<Integer> opState;

		@Override
		public String map(Integer value) throws Exception {
			opState.updateState(opState.getState() + 1);
			return value.toString();
		}

		@Override
		public void open(Configuration conf) {
			opState = getRuntimeContext().getOperatorState(0);
		}
	}

	public static class MockContext implements RuntimeContext {

		StreamOperatorState<Integer, Integer> state;

		public MockContext(boolean isPartitionedState, StatefulMapper mapper) {
			if (isPartitionedState) {
				this.state = new PartitionedStreamOperatorState<Integer, Integer, Integer>(
						new LocalStateHandleProvider<Integer>(), new ModKey(2));
			} else {
				this.state = new StreamOperatorState<Integer, Integer>(
						new LocalStateHandleProvider<Integer>());
			}
		}
		
		public String getTaskName() {return null;}
		public int getNumberOfParallelSubtasks() {return 0;}
		public int getIndexOfThisSubtask() {return 0;}
		public ExecutionConfig getExecutionConfig() {return null;}
		public ClassLoader getUserCodeClassLoader() {return null;}
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {}
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {return null;}
		public HashMap<String, Accumulator<?, ?>> getAllAccumulators() {return null;}	
		public IntCounter getIntCounter(String name) {return null;}	
		public LongCounter getLongCounter(String name) {return null;}
		public DoubleCounter getDoubleCounter(String name) {return null;}
		public Histogram getHistogram(String name) {return null;}		
		public <RT> List<RT> getBroadcastVariable(String name) {return null;}
		public <T, C> C getBroadcastVariableWithInitializer(String name,
				BroadcastVariableInitializer<T, C> initializer) {return null;}
		public DistributedCache getDistributedCache() {return null;}

		@SuppressWarnings("unchecked")
		@Override
		public <S, C extends Serializable> OperatorState<S> getOperatorState(S defaultState,
				StateCheckpointer<S, C> checkpointer) {
			state.setDefaultState((Integer) defaultState);
			return (OperatorState<S>) state;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <S extends Serializable> OperatorState<S> getOperatorState(S defaultState) {
			state.setDefaultState((Integer) defaultState);
			return (OperatorState<S>) state;
		}

	}

}
