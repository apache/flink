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
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.CollectRequestSender;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionInitializationContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.api.operators.collect.utils.TestCollectClient;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link CollectSinkFunction}.
 */
public class CollectSinkFunctionTest extends TestLogger {

	private static final int MAX_RESULTS_PER_BATCH = 3;
	private static final String ACCUMULATOR_NAME = "tableCollectAccumulator";
	private static final long TIME_OUT_MILLIS = 10000;
	private static final int MAX_RETIRES = 100;

	private static final TypeSerializer<Row> serializer =
		new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO).createSerializer(new ExecutionConfig());

	private CollectSinkFunction<Row> function;
	private CollectSinkOperatorCoordinator coordinator;
	private MockFunctionInitializationContext functionInitializationContext;
	private boolean jobFinished;

	private IOManager ioManager;
	private StreamingRuntimeContext runtimeContext;
	private MockOperatorEventGateway gateway;

	@Before
	public void before() throws Exception {
		ioManager = new IOManagerAsync();
		runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, ioManager);
		gateway = new MockOperatorEventGateway();
		coordinator = new CollectSinkOperatorCoordinator();
		coordinator.start();

		// only used in checkpointed tests
		functionInitializationContext = new MockFunctionInitializationContext();

		jobFinished = false;
	}

	@After
	public void after() throws Exception {
		coordinator.close();
		ioManager.close();
	}

	@Test
	public void testUncheckpointedProtocol() throws Exception {
		openFunction();
		for (int i = 0; i < 6; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse<Row> response = sendRequestAndGetValidResponse("", 0);
		Assert.assertEquals(0, response.getLastCheckpointedOffset());
		String version = response.getVersion();

		response = sendRequestAndGetValidResponse(version, 0);
		assertResponseEquals(response, version, 0, Arrays.asList(0, 1, 2));

		response = sendRequestAndGetValidResponse(version, 4);
		assertResponseEquals(response, version, 0, Arrays.asList(4, 5));

		response = sendRequestAndGetValidResponse(version, 6);
		assertResponseEquals(response, version, 0, Collections.emptyList());

		for (int i = 6; i < 10; i++) {
			function.invoke(Row.of(i), null);
		}

		// invalid request
		response = sendRequestAndGetValidResponse(version, 5);
		assertResponseEquals(response, version, 0, Collections.emptyList());

		response = sendRequestAndGetValidResponse(version, 6);
		assertResponseEquals(response, version, 0, Arrays.asList(6, 7, 8));

		response = sendRequestAndGetValidResponse(version, 6);
		assertResponseEquals(response, version, 0, Arrays.asList(6, 7, 8));

		response = sendRequestAndGetValidResponse(version, 12);
		assertResponseEquals(response, version, 0, Collections.emptyList());

		for (int i = 10; i < 16; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 12);
		assertResponseEquals(response, version, 0, Arrays.asList(12, 13, 14));

		finishJob();

		assertAccumulatorResult(12, version, 0, Arrays.asList(12, 13, 14, 15));
	}

	@Test
	public void testCheckpointProtocol() throws Exception {
		openFunctionWithState();
		for (int i = 0; i < 2; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse<Row> response = sendRequestAndGetValidResponse("", 0);
		Assert.assertEquals(0, response.getLastCheckpointedOffset());
		String version = response.getVersion();

		response = sendRequestAndGetValidResponse(version, 0);
		assertResponseEquals(response, version, 0, Arrays.asList(0, 1));

		for (int i = 2; i < 6; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 3);
		assertResponseEquals(response, version, 0, Arrays.asList(3, 4, 5));

		checkpointFunction(1);

		// checkpoint hasn't finished yet
		response = sendRequestAndGetValidResponse(version, 4);
		assertResponseEquals(response, version, 0, Arrays.asList(4, 5));

		checkpointComplete(1);

		// checkpoint finished
		response = sendRequestAndGetValidResponse(version, 4);
		assertResponseEquals(response, version, 3, Arrays.asList(4, 5));

		for (int i = 6; i < 9; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 6);
		assertResponseEquals(response, version, 3, Arrays.asList(6, 7, 8));

		closeFuntionAbnormally();

		openFunctionWithState();

		for (int i = 9; i < 12; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 4);
		Assert.assertEquals(3, response.getLastCheckpointedOffset());
		version = response.getVersion();

		response = sendRequestAndGetValidResponse(version, 4);
		assertResponseEquals(response, version, 3, Arrays.asList(4, 5, 9));

		response = sendRequestAndGetValidResponse(version, 6);
		assertResponseEquals(response, version, 3, Arrays.asList(9, 10, 11));

		checkpointFunction(2);
		checkpointComplete(2);

		function.invoke(Row.of(12), null);

		response = sendRequestAndGetValidResponse(version, 7);
		assertResponseEquals(response, version, 6, Arrays.asList(10, 11, 12));

		closeFuntionAbnormally();

		openFunctionWithState();

		response = sendRequestAndGetValidResponse(version, 7);
		Assert.assertEquals(6, response.getLastCheckpointedOffset());
		version = response.getVersion();

		response = sendRequestAndGetValidResponse(version, 7);
		assertResponseEquals(response, version, 6, Arrays.asList(10, 11));

		response = sendRequest(version, 9);
		assertResponseEquals(response, version, 6, Collections.emptyList());

		for (int i = 13; i < 17; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 9);
		assertResponseEquals(response, version, 6, Arrays.asList(13, 14, 15));

		checkpointFunction(3);
		checkpointComplete(3);

		closeFuntionAbnormally();

		openFunctionWithState();

		response = sendRequestAndGetValidResponse(version, 12);
		Assert.assertEquals(9, response.getLastCheckpointedOffset());
		version = response.getVersion();

		response = sendRequestAndGetValidResponse(version, 12);
		assertResponseEquals(response, version, 9, Collections.singletonList(16));

		for (int i = 17; i < 20; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 12);
		assertResponseEquals(response, version, 9, Arrays.asList(16, 17, 18));

		// this checkpoint will not complete
		checkpointFunction(4);

		closeFuntionAbnormally();

		openFunctionWithState();

		response = sendRequestAndGetValidResponse(version, 12);
		Assert.assertEquals(9, response.getLastCheckpointedOffset());
		version = response.getVersion();

		response = sendRequestAndGetValidResponse(version, 12);
		assertResponseEquals(response, version, 9, Collections.singletonList(16));

		for (int i = 20; i < 23; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequestAndGetValidResponse(version, 12);
		assertResponseEquals(response, version, 9, Arrays.asList(16, 20, 21));

		finishJob();

		assertAccumulatorResult(12, version, 9, Arrays.asList(16, 20, 21, 22));
	}

	@Test
	public void testUncheckpointedFunction() throws Exception {
		// run multiple times for this random test
		for (int testCount = 30; testCount > 0; testCount--) {
			List<Integer> expected = new ArrayList<>();
			for (int i = 0; i < 50; i++) {
				expected.add(i);
			}
			UncheckpointedDataFeeder feeder = new UncheckpointedDataFeeder(expected);
			TestCollectClient<Row> client = new TestCollectClient<>(
				serializer,
				new TestCollectRequestSender(),
				() -> jobFinished);

			runFunctionWithClient(feeder, client);
			assertResultsEqualAfterSort(expected, client.getResults());

			after();
			before();
		}
	}

	@Test
	public void testCheckpointedFunction() throws Exception {
		// run multiple times for this random test
		for (int testCount = 30; testCount > 0; testCount--) {
			List<Integer> expected = new ArrayList<>();
			for (int i = 0; i < 50; i++) {
				expected.add(i);
			}
			CheckpointedDataFeeder feeder = new CheckpointedDataFeeder(expected);
			TestCollectClient<Row> client = new TestCollectClient<>(
				serializer,
				new TestCollectRequestSender(),
				() -> jobFinished);

			runFunctionWithClient(feeder, client);
			assertResultsEqualAfterSort(expected, client.getResults());

			after();
			before();
		}
	}

	private void runFunctionWithClient(Thread feeder, Thread client) throws Exception {
		Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> {
			feeder.interrupt();
			client.interrupt();
		};
		feeder.setUncaughtExceptionHandler(exceptionHandler);
		client.setUncaughtExceptionHandler(exceptionHandler);

		feeder.start();
		client.start();
		feeder.join();
		client.join();
	}

	private void openFunction() throws Exception {
		function = new CollectSinkFunction<>(serializer, MAX_RESULTS_PER_BATCH, ACCUMULATOR_NAME);
		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getNextEvent());
	}

	private void openFunctionWithState() throws Exception {
		functionInitializationContext.getOperatorStateStore().revertToLastSuccessCheckpoint();
		function = new CollectSinkFunction<>(serializer, MAX_RESULTS_PER_BATCH, ACCUMULATOR_NAME);
		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);
		function.initializeState(functionInitializationContext);
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getNextEvent());
	}

	private void checkpointFunction(long checkpointId) throws Exception {
		function.snapshotState(new MockFunctionSnapshotContext(checkpointId));
		functionInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
	}

	private void checkpointComplete(long checkpointId) throws Exception {
		function.notifyCheckpointComplete(checkpointId);
		functionInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
	}

	private void closeFuntionAbnormally() throws Exception {
		// this is an exceptional shutdown
		function.close();
		coordinator.subtaskFailed(0);
	}

	private void finishJob() throws Exception {
		// this is a normal shutdown
		function.accumulateFinalResults();
		function.close();

		jobFinished = true;
	}

	@SuppressWarnings("unchecked")
	private CollectCoordinationResponse<Row> sendRequest(
			String version,
			long offset) throws Exception {
		CollectCoordinationRequest request = new CollectCoordinationRequest(version, offset);
		// we add a timeout to not block the tests
		return ((CollectCoordinationResponse) coordinator
			.handleCoordinationRequest(request).get(TIME_OUT_MILLIS, TimeUnit.MILLISECONDS));
	}

	private CollectCoordinationResponse<Row> sendRequestAndGetValidResponse(
			String version,
			long offset) throws Exception {
		CollectCoordinationResponse<Row> response;
		for (int i = 0; i < MAX_RETIRES; i++) {
			response = sendRequest(version, offset);
			if (response.getLastCheckpointedOffset() >= 0) {
				return response;
			}
		}
		throw new RuntimeException("Too many retries in sendRequestAndGetValidResponse");
	}

	@SuppressWarnings("unchecked")
	private Tuple2<Long, CollectCoordinationResponse> getAccumualtorResults() throws Exception {
		Accumulator accumulator = runtimeContext.getAccumulator(ACCUMULATOR_NAME);
		ArrayList<byte[]> accLocalValue = ((SerializedListAccumulator) accumulator).getLocalValue();
		List<byte[]> serializedResults =
			SerializedListAccumulator.deserializeList(accLocalValue, BytePrimitiveArraySerializer.INSTANCE);
		Assert.assertEquals(1, serializedResults.size());
		byte[] serializedResult = serializedResults.get(0);
		return CollectSinkFunction.deserializeAccumulatorResult(serializedResult);
	}

	private void assertResponseEquals(
			CollectCoordinationResponse<Row> response,
			String version,
			long lastCheckpointedOffset,
			List<Integer> expected) throws IOException {
		Assert.assertEquals(version, response.getVersion());
		Assert.assertEquals(lastCheckpointedOffset, response.getLastCheckpointedOffset());
		List<Row> results = response.getResults(serializer);
		assertResultsEqual(expected, results);
	}

	private void assertResultsEqual(List<Integer> expected, List<Row> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			Row row = actual.get(i);
			Assert.assertEquals(1, row.getArity());
			Assert.assertEquals(expected.get(i), row.getField(0));
		}
	}

	private void assertResultsEqualAfterSort(List<Integer> expected, List<Row> actual) {
		Collections.sort(expected);
		actual.sort(Comparator.comparingInt(row -> (int) row.getField(0)));
		assertResultsEqual(expected, actual);
	}

	@SuppressWarnings("unchecked")
	private void assertAccumulatorResult(
			long expectedOffset,
			String expectedVersion,
			long expectedLastCheckpointedOffset,
			List<Integer> expectedResults) throws Exception {
		Tuple2<Long, CollectCoordinationResponse> accResults = getAccumualtorResults();
		long offset = accResults.f0;
		CollectCoordinationResponse response = accResults.f1;
		List<Row> actualResults = response.getResults(serializer);

		Assert.assertEquals(expectedOffset, offset);
		Assert.assertEquals(expectedVersion, response.getVersion());
		Assert.assertEquals(expectedLastCheckpointedOffset, response.getLastCheckpointedOffset());
		assertResultsEqual(expectedResults, actualResults);
	}

	private class TestCollectRequestSender implements CollectRequestSender<Row> {

		@Override
		public CollectCoordinationResponse<Row> sendRequest(String version, long offset) throws Exception {
			return CollectSinkFunctionTest.this.sendRequest(version, offset);
		}

		@Override
		public Tuple2<Long, CollectCoordinationResponse> getAccumulatorResults() throws Exception {
			return CollectSinkFunctionTest.this.getAccumualtorResults();
		}
	}

	/**
	 * A thread feeding data to the function. It will fail when half of the data is fed.
	 */
	private class UncheckpointedDataFeeder extends Thread {

		private LinkedList<Integer> data;
		private List<Integer> checkpointedData;
		private boolean failedBefore;

		private UncheckpointedDataFeeder(List<Integer> data) {
			this.data = new LinkedList<>(data);
			this.checkpointedData = new ArrayList<>(data);
			this.failedBefore = false;
		}

		@Override
		public void run() {
			Random random = new Random();

			try {
				openFunction();

				while (data.size() > 0) {
					int size = Math.min(data.size(), random.nextInt(MAX_RESULTS_PER_BATCH * 3) + 1);
					for (int i = 0; i < size; i++) {
						function.invoke(Row.of(data.removeFirst()), null);
					}

					if (!failedBefore && data.size() < checkpointedData.size() / 2) {
						if (random.nextBoolean()) {
							// with 50% chance we fail half-way
							Collections.shuffle(checkpointedData);
							data = new LinkedList<>(checkpointedData);

							closeFuntionAbnormally();
							openFunction();
						}

						failedBefore = true;
					}

					if (random.nextBoolean()) {
						Thread.sleep(random.nextInt(10));
					}
				}

				finishJob();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * A thread feeding data to the function. It will randomly do checkpoint or fail.
	 */
	private class CheckpointedDataFeeder extends Thread {

		private LinkedList<Integer> data;
		private List<Integer> checkpointedData;
		private long checkpointId;
		private long lastSuccessCheckpointId;
		private List<CheckpointCountdown> checkpointCountdowns;

		private CheckpointedDataFeeder(List<Integer> data) {
			this.data = new LinkedList<>(data);
			this.checkpointedData = new ArrayList<>(data);
			this.checkpointId = 0;
			this.lastSuccessCheckpointId = 0;
			this.checkpointCountdowns = new ArrayList<>();
		}

		@Override
		public void run() {
			Random random = new Random();

			try {
				openFunctionWithState();

				while (data.size() > 0) {
					ListIterator<CheckpointCountdown> iterator = checkpointCountdowns.listIterator();
					while (iterator.hasNext()) {
						CheckpointCountdown countdown = iterator.next();
						if (countdown.id < lastSuccessCheckpointId) {
							iterator.remove();
						} else if (countdown.tick()) {
							// complete a checkpoint
							checkpointedData = countdown.data;
							checkpointComplete(countdown.id);
							lastSuccessCheckpointId = countdown.id;
							iterator.remove();
						}
					}

					int r = random.nextInt(10);
					if (r < 6) {
						// with 60% chance we add some data
						int size = Math.min(data.size(), random.nextInt(MAX_RESULTS_PER_BATCH * 3) + 1);
						for (int i = 0; i < size; i++) {
							function.invoke(Row.of(data.removeFirst()), null);
						}
					} else if (r < 9) {
						// with 30% chance we make a checkpoint
						checkpointId++;

						if (random.nextBoolean()) {
							// with 50% chance this checkpoint will succeed in the future
							checkpointCountdowns.add(
								new CheckpointCountdown(checkpointId, data, random.nextInt(3) + 1));
						}

						checkpointFunction(checkpointId);
					} else {
						// with 10% chance we fail
						checkpointCountdowns.clear();

						// we shuffle data to emulate jobs whose result order is undetermined
						Collections.shuffle(checkpointedData);
						data = new LinkedList<>(checkpointedData);

						closeFuntionAbnormally();
						openFunctionWithState();
					}

					if (random.nextBoolean()) {
						Thread.sleep(random.nextInt(10));
					}
				}

				finishJob();
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	private static class CheckpointCountdown {

		private long id;
		private List<Integer> data;
		private int countdown;

		private CheckpointCountdown(long id, List<Integer> data, int countdown) {
			this.id = id;
			this.data = new ArrayList<>(data);
			this.countdown = countdown;
		}

		private boolean tick() {
			if (countdown > 0) {
				countdown--;
				return countdown == 0;
			}
			return false;
		}
	}
}
