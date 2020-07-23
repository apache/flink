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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.collect.utils.CollectSinkFunctionTestWrapper;
import org.apache.flink.streaming.api.operators.collect.utils.TestJobClient;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.streaming.api.operators.collect.utils.CollectSinkFunctionTestWrapper.ACCUMULATOR_NAME;

/**
 * Random IT cases for {@link CollectSinkFunction}.
 * It will perform random insert, random checkpoint and random restart.
 */
public class CollectSinkFunctionRandomITCase extends TestLogger {

	private static final int MAX_RESULTS_PER_BATCH = 3;
	private static final JobID TEST_JOB_ID = new JobID();
	private static final OperatorID TEST_OPERATOR_ID = new OperatorID();

	private static final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;

	private CollectSinkFunctionTestWrapper<Integer> functionWrapper;
	private boolean jobFinished;

	@Test
	public void testUncheckpointedFunction() throws Exception {
		// run multiple times for this random test
		for (int testCount = 30; testCount > 0; testCount--) {
			functionWrapper = new CollectSinkFunctionTestWrapper<>(serializer, MAX_RESULTS_PER_BATCH * 4);
			jobFinished = false;

			List<Integer> expected = new ArrayList<>();
			for (int i = 0; i < 50; i++) {
				expected.add(i);
			}
			Thread feeder = new ThreadWithException(new UncheckpointedDataFeeder(expected));

			List<Integer> actual = runFunctionRandomTest(feeder);
			assertResultsEqualAfterSort(expected, actual);

			functionWrapper.closeWrapper();
		}
	}

	@Test
	public void testCheckpointedFunction() throws Exception {
		// run multiple times for this random test
		for (int testCount = 30; testCount > 0; testCount--) {
			functionWrapper = new CollectSinkFunctionTestWrapper<>(serializer, MAX_RESULTS_PER_BATCH * 4);
			jobFinished = false;

			List<Integer> expected = new ArrayList<>();
			for (int i = 0; i < 50; i++) {
				expected.add(i);
			}
			Thread feeder = new ThreadWithException(new CheckpointedDataFeeder(expected));

			List<Integer> actual = runFunctionRandomTest(feeder);
			assertResultsEqualAfterSort(expected, actual);

			functionWrapper.closeWrapper();
		}
	}

	private List<Integer> runFunctionRandomTest(Thread feeder) throws Exception {
		CollectClient collectClient = new CollectClient();
		Thread client = new ThreadWithException(collectClient);

		Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> {
			feeder.interrupt();
			client.interrupt();
			e.printStackTrace();
		};
		feeder.setUncaughtExceptionHandler(exceptionHandler);
		client.setUncaughtExceptionHandler(exceptionHandler);

		feeder.start();
		client.start();
		feeder.join();
		client.join();

		return collectClient.results;
	}

	private void assertResultsEqualAfterSort(List<Integer> expected, List<Integer> actual) {
		Collections.sort(expected);
		Collections.sort(actual);
		Assert.assertThat(actual, CoreMatchers.is(expected));
	}

	/**
	 * A {@link RunnableWithException} feeding data to the function. It will fail when half of the data is fed.
	 */
	private class UncheckpointedDataFeeder implements RunnableWithException {

		private LinkedList<Integer> data;
		private final List<Integer> originalData;
		private boolean failedBefore;

		private UncheckpointedDataFeeder(List<Integer> data) {
			this.data = new LinkedList<>(data);
			this.originalData = new ArrayList<>(data);
			this.failedBefore = false;
		}

		@Override
		public void run() throws Exception {
			Random random = new Random();
			functionWrapper.openFunction();

			while (data.size() > 0) {
				// feed some data to the function
				int size = Math.min(data.size(), random.nextInt(MAX_RESULTS_PER_BATCH * 3) + 1);
				for (int i = 0; i < size; i++) {
					functionWrapper.invoke(data.removeFirst());
				}

				if (!failedBefore && data.size() < originalData.size() / 2) {
					if (random.nextBoolean()) {
						// with 50% chance we fail half-way
						// we shuffle the data to simulate jobs whose result order is undetermined
						data = new LinkedList<>(originalData);
						Collections.shuffle(data);

						functionWrapper.closeFunctionAbnormally();
						functionWrapper.openFunction();
					}

					failedBefore = true;
				}

				if (random.nextBoolean()) {
					Thread.sleep(random.nextInt(10));
				}
			}

			functionWrapper.closeFunctionNormally();
			jobFinished = true;
		}
	}

	/**
	 * A {@link RunnableWithException} feeding data to the function. It will randomly do checkpoint or fail.
	 */
	private class CheckpointedDataFeeder implements RunnableWithException {

		private LinkedList<Integer> data;
		private List<Integer> checkpointedData;
		private long checkpointId;
		private long lastSuccessCheckpointId;
		private final List<CheckpointCountdown> checkpointCountdowns;

		private CheckpointedDataFeeder(List<Integer> data) {
			this.data = new LinkedList<>(data);
			this.checkpointedData = new ArrayList<>(data);
			this.checkpointId = 0;
			this.lastSuccessCheckpointId = 0;
			this.checkpointCountdowns = new ArrayList<>();
		}

		@Override
		public void run() throws Exception {
			Random random = new Random();
			functionWrapper.openFunctionWithState();

			while (data.size() > 0) {
				// countdown each on-going checkpoint
				ListIterator<CheckpointCountdown> iterator = checkpointCountdowns.listIterator();
				while (iterator.hasNext()) {
					CheckpointCountdown countdown = iterator.next();
					if (countdown.id < lastSuccessCheckpointId) {
						// this checkpoint is stale, throw it away
						iterator.remove();
					} else if (countdown.tick()) {
						// complete a checkpoint
						checkpointedData = countdown.data;
						functionWrapper.checkpointComplete(countdown.id);
						lastSuccessCheckpointId = countdown.id;
						iterator.remove();
					}
				}

				int r = random.nextInt(10);
				if (r < 6) {
					// with 60% chance we add some data
					int size = Math.min(data.size(), random.nextInt(MAX_RESULTS_PER_BATCH * 3) + 1);
					for (int i = 0; i < size; i++) {
						functionWrapper.invoke(data.removeFirst());
					}
				} else if (r < 9) {
					// with 30% chance we make a checkpoint
					checkpointId++;

					if (random.nextBoolean()) {
						// with 50% chance this checkpoint will succeed in the future
						checkpointCountdowns.add(
							new CheckpointCountdown(checkpointId, data, random.nextInt(3) + 1));
					}

					functionWrapper.checkpointFunction(checkpointId);
				} else {
					// with 10% chance we fail
					checkpointCountdowns.clear();

					// we shuffle data to simulate jobs whose result order is undetermined
					Collections.shuffle(checkpointedData);
					data = new LinkedList<>(checkpointedData);

					functionWrapper.closeFunctionAbnormally();
					functionWrapper.openFunctionWithState();
				}

				if (random.nextBoolean()) {
					Thread.sleep(random.nextInt(10));
				}
			}

			functionWrapper.closeFunctionNormally();
			jobFinished = true;
		}
	}

	/**
	 * Countdown for a checkpoint which will succeed in the future.
	 */
	private static class CheckpointCountdown {

		private final long id;
		private final List<Integer> data;
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

	/**
	 * A {@link RunnableWithException} collecting results with the collecting iterator.
	 */
	private class CollectClient implements RunnableWithException {

		private final List<Integer> results;
		private final CollectResultIterator<Integer> iterator;

		private CollectClient() {
			this.results = new ArrayList<>();

			this.iterator = new CollectResultIterator<>(
				new CheckpointedCollectResultBuffer<>(serializer),
				CompletableFuture.completedFuture(TEST_OPERATOR_ID),
				ACCUMULATOR_NAME,
				0
			);

			TestJobClient.JobInfoProvider infoProvider = new TestJobClient.JobInfoProvider() {

				@Override
				public boolean isJobFinished() {
					return jobFinished;
				}

				@Override
				public Map<String, OptionalFailure<Object>> getAccumulatorResults() {
					Map<String, OptionalFailure<Object>> accumulatorResults = new HashMap<>();
					accumulatorResults.put(
						ACCUMULATOR_NAME,
						OptionalFailure.of(functionWrapper.getAccumulatorLocalValue()));
					return accumulatorResults;
				}
			};

			TestJobClient jobClient = new TestJobClient(
				TEST_JOB_ID,
				TEST_OPERATOR_ID,
				functionWrapper.getCoordinator(),
				infoProvider);

			iterator.setJobClient(jobClient);
		}

		@Override
		public void run() throws Exception {
			Random random = new Random();

			while (iterator.hasNext()) {
				results.add(iterator.next());
				if (random.nextBoolean()) {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}

			iterator.close();
		}
	}

	/**
	 * A subclass of thread which wraps a {@link RunnableWithException} for the ease of tests.
	 */
	private static class ThreadWithException extends Thread {

		private final RunnableWithException runnable;

		private ThreadWithException(RunnableWithException runnable) {
			this.runnable = runnable;
		}

		@Override
		public void run() {
			try {
				runnable.run();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
