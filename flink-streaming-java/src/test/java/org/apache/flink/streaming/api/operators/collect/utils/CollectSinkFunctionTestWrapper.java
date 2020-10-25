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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorCoordinator;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper class for creating, checkpointing and closing
 * {@link org.apache.flink.streaming.api.operators.collect.CollectSinkFunction} for tests.
 */
public class CollectSinkFunctionTestWrapper<IN> {

	public static final String ACCUMULATOR_NAME = "tableCollectAccumulator";

	private static final int SOCKET_TIMEOUT_MILLIS = 1000;
	private static final int FUTURE_TIMEOUT_MILLIS = 10000;
	private static final int MAX_RETIRES = 100;

	private final TypeSerializer<IN> serializer;
	private final int maxBytesPerBatch;

	private final IOManager ioManager;
	private final StreamingRuntimeContext runtimeContext;
	private final MockOperatorEventGateway gateway;
	private final CollectSinkOperatorCoordinator coordinator;
	private final MockFunctionInitializationContext functionInitializationContext;

	private CollectSinkFunction<IN> function;

	public CollectSinkFunctionTestWrapper(TypeSerializer<IN> serializer, int maxBytesPerBatch) throws Exception {
		this.serializer = serializer;
		this.maxBytesPerBatch = maxBytesPerBatch;

		this.ioManager = new IOManagerAsync();
		MockEnvironment environment = new MockEnvironmentBuilder()
			.setTaskName("mockTask")
			.setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
			.setIOManager(ioManager)
			.build();
		this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
		this.gateway = new MockOperatorEventGateway();

		this.coordinator = new CollectSinkOperatorCoordinator(SOCKET_TIMEOUT_MILLIS);
		this.coordinator.start();

		this.functionInitializationContext = new MockFunctionInitializationContext();
	}

	public void closeWrapper() throws Exception {
		coordinator.close();
		ioManager.close();
	}

	public CollectSinkOperatorCoordinator getCoordinator() {
		return coordinator;
	}

	public void openFunction() throws Exception {
		function = new CollectSinkFunction<>(serializer, maxBytesPerBatch, ACCUMULATOR_NAME);
		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getNextEvent());
	}

	public void openFunctionWithState() throws Exception {
		functionInitializationContext.getOperatorStateStore().revertToLastSuccessCheckpoint();
		function = new CollectSinkFunction<>(serializer, maxBytesPerBatch, ACCUMULATOR_NAME);
		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);
		function.initializeState(functionInitializationContext);
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getNextEvent());
	}

	public void invoke(IN record) throws Exception {
		function.invoke(record, null);
	}

	public void checkpointFunction(long checkpointId) throws Exception {
		function.snapshotState(new MockFunctionSnapshotContext(checkpointId));
		functionInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
	}

	public void checkpointComplete(long checkpointId) {
		function.notifyCheckpointComplete(checkpointId);
		functionInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
	}

	public void closeFunctionNormally() throws Exception {
		// this is a normal shutdown
		function.accumulateFinalResults();
		function.close();
	}

	public void closeFunctionAbnormally() throws Exception {
		// this is an exceptional shutdown
		function.close();
		coordinator.subtaskFailed(0, null);
	}

	public CollectCoordinationResponse sendRequestAndGetResponse(String version, long offset) throws Exception {
		CollectCoordinationResponse response;
		for (int i = 0; i < MAX_RETIRES; i++) {
			response = sendRequest(version, offset);
			if (response.getLastCheckpointedOffset() >= 0) {
				return response;
			}
		}
		throw new RuntimeException("Too many retries in sendRequestAndGetValidResponse");
	}

	private CollectCoordinationResponse sendRequest(String version, long offset) throws Exception {
		CollectCoordinationRequest request = new CollectCoordinationRequest(version, offset);
		// we add a timeout to not block the tests if it fails
		return ((CollectCoordinationResponse) coordinator
			.handleCoordinationRequest(request).get(FUTURE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
	}

	public Tuple2<Long, CollectCoordinationResponse> getAccumulatorResults() throws Exception {
		ArrayList<byte[]> accLocalValue = getAccumulatorLocalValue();
		List<byte[]> serializedResults =
			SerializedListAccumulator.deserializeList(accLocalValue, BytePrimitiveArraySerializer.INSTANCE);
		Assert.assertEquals(1, serializedResults.size());
		byte[] serializedResult = serializedResults.get(0);
		return CollectSinkFunction.deserializeAccumulatorResult(serializedResult);
	}

	@SuppressWarnings("unchecked")
	public ArrayList<byte[]> getAccumulatorLocalValue() {
		Accumulator accumulator = runtimeContext.getAccumulator(ACCUMULATOR_NAME);
		return ((SerializedListAccumulator) accumulator).getLocalValue();
	}
}
