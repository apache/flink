/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link CollectSinkFunction}.
 */
public class CollectSinkFunctionTest extends TestLogger {

	private static final int MAX_RESULTS_PER_BATCH = 3;
	private static final String LIST_ACC_NAME = "tableCollectList";
	private static final String OFFSET_ACC_NAME = "tableCollectOffset";

	private CollectSinkFunction<Row> function;
	private CollectSinkOperatorCoordinator coordinator;

	private TypeSerializer<Row> serializer;
	private StreamingRuntimeContext runtimeContext;
	private MockOperatorEventGateway gateway;

	@Before
	public void before() throws Exception {
		serializer = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO).createSerializer(new ExecutionConfig());
		runtimeContext = new MockStreamingRuntimeContext(false, 1, 0);
		gateway = new MockOperatorEventGateway();
		function = new CollectSinkFunction<>(serializer, MAX_RESULTS_PER_BATCH, LIST_ACC_NAME, OFFSET_ACC_NAME);
		coordinator = new CollectSinkOperatorCoordinator();
		coordinator.start();

		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);
	}

	@After
	public void after() throws Exception {
		coordinator.close();
	}

	@Test
	public void testProtocol() throws Exception {
		openFunction();
		for (int i = 0; i < 6; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse<Row> response = sendRequest("", 0);
		Assert.assertEquals(0, response.getOffset());
		String version = response.getVersion();

		response = sendRequest(version, 0);
		assertResponseEquals(response, version, 0, Long.MIN_VALUE, Arrays.asList(0, 1, 2));

		response = sendRequest(version, 4);
		assertResponseEquals(response, version, 4, Long.MIN_VALUE, Arrays.asList(4, 5));

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 6; i < 10; i++) {
			function.invoke(Row.of(i), null);
		}

		// invalid request
		response = sendRequest(version, 5);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Collections.emptyList());

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Arrays.asList(6, 7, 8));

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Arrays.asList(6, 7, 8));

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 10, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 10; i < 16; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 12, Long.MIN_VALUE, Arrays.asList(12, 13, 14));

		// this is a normal shutdown
		function.accumulateFinalResults();
		function.close();

		assertAccumulatorResult(12, Arrays.asList(12, 13, 14, 15));
	}

	@Test
	public void testCheckpoint() throws Exception {
		FunctionInitializationContext functionInitializationContext = new MockFunctionInitializationContext();
		function.initializeState(functionInitializationContext);
		openFunction();
		for (int i = 0; i < 2; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse<Row> response = sendRequest("", 0);
		Assert.assertEquals(0, response.getOffset());
		String version = response.getVersion();

		response = sendRequest(version, 0);
		assertResponseEquals(response, version, 0, Long.MIN_VALUE, Arrays.asList(0, 1));

		for (int i = 2; i < 6; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 3);
		assertResponseEquals(response, version, 3, Long.MIN_VALUE, Arrays.asList(3, 4, 5));

		// CollectSinkFunction is not using this context
		function.snapshotState(null);
		function.notifyCheckpointComplete(0);

		response = sendRequest(version, 4);
		assertResponseEquals(response, version, 4, 0, Arrays.asList(4, 5));

		for (int i = 6; i < 9; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, 0, Arrays.asList(6, 7, 8));

		// this is an exceptional shutdown
		function.close();

		function.initializeState(functionInitializationContext);
		openFunction();

		for (int i = 9; i < 12; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 4);
		Assert.assertEquals(3, response.getOffset());
		version = response.getVersion();

		response = sendRequest(version, 4);
		assertResponseEquals(response, version, 4, Long.MIN_VALUE, Arrays.asList(4, 5, 9));

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Arrays.asList(9, 10, 11));

		// CollectSinkFunction is not using this context
		function.snapshotState(null);
		function.notifyCheckpointComplete(1);

		function.invoke(Row.of(12), null);

		response = sendRequest(version, 7);
		assertResponseEquals(response, version, 7, 1, Arrays.asList(10, 11, 12));

		// this is an exceptional shutdown
		function.close();

		function.initializeState(functionInitializationContext);
		openFunction();

		response = sendRequest(version, 7);
		Assert.assertEquals(6, response.getOffset());
		version = response.getVersion();

		response = sendRequest(version, 7);
		assertResponseEquals(response, version, 7, Long.MIN_VALUE, Arrays.asList(10, 11));

		response = sendRequest(version, 9);
		assertResponseEquals(response, version, 9, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 13; i < 16; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 9);
		assertResponseEquals(response, version, 9, Long.MIN_VALUE, Arrays.asList(13, 14, 15));

		// CollectSinkFunction is not using this context
		function.snapshotState(null);
		function.notifyCheckpointComplete(2);

		// this is an exceptional shutdown
		function.close();

		function.initializeState(functionInitializationContext);
		openFunction();

		response = sendRequest(version, 12);
		Assert.assertEquals(9, response.getOffset());
		version = response.getVersion();

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 12, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 16; i < 20; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 12, Long.MIN_VALUE, Arrays.asList(16, 17, 18));

		// this is a normal shutdown
		function.accumulateFinalResults();
		function.close();

		assertAccumulatorResult(12, Arrays.asList(16, 17, 18, 19));
	}

	private void openFunction() throws Exception {
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getNextEvent());
	}

	@SuppressWarnings("unchecked")
	private CollectCoordinationResponse<Row> sendRequest(
			String version,
			long offset) throws Exception {
		CollectCoordinationRequest request = new CollectCoordinationRequest(version, offset);
		return ((CollectCoordinationResponse) coordinator.handleCoordinationRequest(request).get());
	}

	private void assertResponseEquals(
			CollectCoordinationResponse<Row> response,
			String version,
			long offset,
			long lastCheckpointId,
			List<Integer> expected) throws IOException {
		Assert.assertEquals(version, response.getVersion());
		Assert.assertEquals(offset, response.getOffset());
		Assert.assertEquals(lastCheckpointId, response.getLastCheckpointId());
		List<Row> results = response.getResults(serializer);
		Assert.assertEquals(expected.size(), results.size());
		for (int i = 0; i < expected.size(); i++) {
			Row row = results.get(i);
			Assert.assertEquals(1, row.getArity());
			Assert.assertEquals(expected.get(i), row.getField(0));
		}
	}

	@SuppressWarnings("unchecked")
	private void assertAccumulatorResult(long expectedOffset, List<Integer> expectedResults) throws Exception {
		Accumulator listAccumulator = runtimeContext.getAccumulator(LIST_ACC_NAME);
		ArrayList<byte[]> serializedResult = ((SerializedListAccumulator) listAccumulator).getLocalValue();
		List<Row> accResult = SerializedListAccumulator.deserializeList(serializedResult, serializer);
		Assert.assertEquals(expectedResults.size(), accResult.size());
		for (int i = 0; i < expectedResults.size(); i++) {
			Row row = accResult.get(i);
			Assert.assertEquals(1, row.getArity());
			Assert.assertEquals(expectedResults.get(i), row.getField(0));
		}
		Accumulator offsetAccumulator = runtimeContext.getAccumulator(OFFSET_ACC_NAME);
		long offset = ((LongCounter) offsetAccumulator).getLocalValue();
		Assert.assertEquals(expectedOffset, offset);
	}

	private static class MockOperatorStateStore implements OperatorStateStore {

		private Map<String, ListState> stateMap;

		private MockOperatorStateStore() {
			this.stateMap = new HashMap<>();
		}

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) throws Exception {
			return null;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			String name = stateDescriptor.getName();
			stateMap.putIfAbsent(name, new TestUtils.MockListState());
			return stateMap.get(name);
		}

		@Override
		public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<String> getRegisteredStateNames() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<String> getRegisteredBroadcastStateNames() {
			throw new UnsupportedOperationException();
		}

		@Override
		public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception {
			return getListState(stateDescriptor);
		}

		@Override
		public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
			throw new UnsupportedOperationException();
		}
	}

	private static class MockFunctionInitializationContext implements FunctionInitializationContext {

		private final OperatorStateStore operatorStateStore;

		private MockFunctionInitializationContext() {
			operatorStateStore = new MockOperatorStateStore();
		}

		@Override
		public boolean isRestored() {
			throw new UnsupportedOperationException();
		}

		@Override
		public OperatorStateStore getOperatorStateStore() {
			return operatorStateStore;
		}

		@Override
		public KeyedStateStore getKeyedStateStore() {
			throw new UnsupportedOperationException();
		}
	}

	private static class MockOperatorEventGateway implements OperatorEventGateway {

		private final LinkedList<OperatorEvent> events;

		private MockOperatorEventGateway() {
			events = new LinkedList<>();
		}

		@Override
		public void sendEventToCoordinator(OperatorEvent event) {
			events.add(event);
		}

		public OperatorEvent getNextEvent() {
			return events.removeFirst();
		}
	}
}
