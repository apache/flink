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

package org.apache.flink.test.query;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnSuccess;
import akka.dispatch.Recover;
import akka.pattern.Patterns;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.messages.JobManagerMessages.JobFound;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateMessage;
import org.apache.flink.runtime.query.QueryableStateClient;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.ResponseRunningTasks;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.JobStatusIs;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobStatus;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryableStateITCase extends TestLogger {

	private final static FiniteDuration TEST_TIMEOUT = new FiniteDuration(100, TimeUnit.SECONDS);

	private final static ActorSystem TEST_ACTOR_SYSTEM = AkkaUtils.createDefaultActorSystem();

	private final static int NUM_TMS = 2;
	private final static int NUM_SLOTS_PER_TM = 4;
	private final static int NUM_SLOTS = NUM_TMS * NUM_SLOTS_PER_TM;

	/**
	 * Shared between all the test. Make sure to have at least NUM_SLOTS
	 * available after your test finishes, e.g. cancel the job you submitted.
	 */
	private static TestingCluster cluster;

	@BeforeClass
	public static void setup() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
			config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
			config.setBoolean(QueryableStateOptions.SERVER_ENABLE, true);
			config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);

			cluster = new TestingCluster(config, false);
			cluster.start(true);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@AfterClass
	public static void tearDown() {
		try {
			cluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		if (TEST_ACTOR_SYSTEM != null) {
			TEST_ACTOR_SYSTEM.shutdown();
		}
	}

	/**
	 * Runs a simple topology producing random (key, 1) pairs at the sources (where
	 * number of keys is in fixed in range 0...numKeys). The records are keyed and
	 * a reducing queryable state instance is created, which sums up the records.
	 *
	 * After submitting the job in detached mode, the QueryableStateCLient is used
	 * to query the counts of each key in rounds until all keys have non-zero counts.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testQueryableState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();
		final int numKeys = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;

		try {
			//
			// Test program
			//
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestKeyRangeSource(numKeys));

			// Reducing state
			ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState = new ReducingStateDescriptor<>(
					"any-name",
					new SumReduce(),
					source.getType());

			final String queryName = "hakuna-matata";

			final QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState(queryName, reducingState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			cluster.submitJobDetached(jobGraph);

			//
			// Start querying
			//
			jobId = jobGraph.getJobID();

			final AtomicLongArray counts = new AtomicLongArray(numKeys);

			final FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);

			boolean allNonZero = false;
			while (!allNonZero && deadline.hasTimeLeft()) {
				allNonZero = true;

				final List<Future<byte[]>> futures = new ArrayList<>(numKeys);

				for (int i = 0; i < numKeys; i++) {
					final int key = i;

					if (counts.get(key) > 0) {
						// Skip this one
						continue;
					} else {
						allNonZero = false;
					}

					final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
							key,
							queryableState.getKeySerializer(),
							VoidNamespace.INSTANCE,
							VoidNamespaceSerializer.INSTANCE);

					Future<byte[]> serializedResult = getKvStateWithRetries(
							client,
							jobId,
							queryName,
							key,
							serializedKey,
							retryDelay);

					serializedResult.onSuccess(new OnSuccess<byte[]>() {
						@Override
						public void onSuccess(byte[] result) throws Throwable {
							Tuple2<Integer, Long> value = KvStateRequestSerializer.deserializeValue(
									result,
									queryableState.getValueSerializer());

							counts.set(key, value.f1);

							assertEquals("Key mismatch", key, value.f0.intValue());
						}
					}, TEST_ACTOR_SYSTEM.dispatcher());

					futures.add(serializedResult);
				}

				Future<Iterable<byte[]>> futureSequence = Futures.sequence(
						futures,
						TEST_ACTOR_SYSTEM.dispatcher());

				Await.ready(futureSequence, deadline.timeLeft());
			}

			assertTrue("Not all keys are non-zero", allNonZero);

			// All should be non-zero
			for (int i = 0; i < numKeys; i++) {
				long count = counts.get(i);
				assertTrue("Count at position " + i + " is " + count, count > 0);
			}
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	/**
	 * Queries a random key and waits for some checkpoints to complete. After
	 * that the task manager where this key was held is killed. Then query the
	 * key again and check for the expected Exception. Finally, add another
	 * task manager and re-query the key (expecting a count >= the previous
	 * one).
	 */
	@Test
	public void testQueryableStateWithTaskManagerFailure() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();
		final int numKeys = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;

		try {
			//
			// Test program
			//
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));
			env.getCheckpointConfig().setCheckpointInterval(1000);

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestKeyRangeSource(numKeys));

			// Reducing state
			ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState = new ReducingStateDescriptor<>(
					"any-name",
					new SumReduce(),
					source.getType());

			final String queryName = "hakuna-matata";

			final QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState(queryName, reducingState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			cluster.submitJobDetached(jobGraph);

			//
			// Start querying
			//
			jobId = jobGraph.getJobID();

			final int key = ThreadLocalRandom.current().nextInt(numKeys);

			// Query a random key
			final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
					key,
					queryableState.getKeySerializer(),
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE);

			long countForKey = 0;

			boolean success = false;
			while (!success && deadline.hasTimeLeft()) {
				final FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
				Future<byte[]> serializedResultFuture = getKvStateWithRetries(
						client,
						jobId,
						queryName,
						key,
						serializedKey,
						retryDelay);

				byte[] serializedResult = Await.result(serializedResultFuture, deadline.timeLeft());

				Tuple2<Integer, Long> result = KvStateRequestSerializer.deserializeValue(
						serializedResult,
						queryableState.getValueSerializer());

				countForKey = result.f1;

				assertEquals("Key mismatch", key, result.f0.intValue());
				success = countForKey > 1000; // Wait for some progress
			}

			assertTrue("No progress for count", countForKey > 1000);

			long currentCheckpointId = TestKeyRangeSource.LATEST_CHECKPOINT_ID.get();
			long waitUntilCheckpointId = currentCheckpointId + 5;

			// Wait for some checkpoint after the query result
			while (deadline.hasTimeLeft() &&
					TestKeyRangeSource.LATEST_CHECKPOINT_ID.get() < waitUntilCheckpointId) {
				Thread.sleep(500);
			}

			assertTrue("Did not complete enough checkpoints to continue",
					TestKeyRangeSource.LATEST_CHECKPOINT_ID.get() >= waitUntilCheckpointId);

			//
			// Find out on which TaskManager the KvState is located and kill that TaskManager
			//
			// This is the subtask index
			int keyGroupIndex = MathUtils.murmurHash(key) % NUM_SLOTS;

			// Find out which task manager holds this key
			Future<ExecutionGraph> egFuture = cluster.getLeaderGateway(deadline.timeLeft())
					.ask(new RequestExecutionGraph(jobId), deadline.timeLeft())
					.mapTo(ClassTag$.MODULE$.<ExecutionGraphFound>apply(ExecutionGraphFound.class))
					.map(new Mapper<TestingJobManagerMessages.ExecutionGraphFound, ExecutionGraph>() {
						@Override
						public ExecutionGraph apply(ExecutionGraphFound found) {
							return (ExecutionGraph) found.executionGraph();
						}
					}, TEST_ACTOR_SYSTEM.dispatcher());
			ExecutionGraph eg = Await.result(egFuture, deadline.timeLeft());

			Future<KvStateLocation> locationFuture = cluster
					.getLeaderGateway(deadline.timeLeft())
					.ask(new KvStateMessage.LookupKvStateLocation(jobId, queryName), deadline.timeLeft())
					.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class));

			KvStateLocation location = Await.result(locationFuture, deadline.timeLeft());

			ExecutionAttemptID executionId = eg.getJobVertex(location.getJobVertexId())
					.getTaskVertices()[keyGroupIndex]
					.getCurrentExecutionAttempt()
					.getAttemptId();

			List<ActorRef> taskManagers = cluster.getTaskManagersAsJava();
			ActorRef taskManagerToKill = null;
			for (ActorRef taskManager : taskManagers) {
				Future<ResponseRunningTasks> runningFuture = Patterns.ask(
						taskManager,
						TestingTaskManagerMessages.getRequestRunningTasksMessage(),
						deadline.timeLeft().toMillis())
						.mapTo(ClassTag$.MODULE$.<ResponseRunningTasks>apply(ResponseRunningTasks.class));

				ResponseRunningTasks running = Await.result(runningFuture, deadline.timeLeft());

				if (running.asJava().containsKey(executionId)) {
					taskManagerToKill = taskManager;
					break;
				}
			}

			assertNotNull("Did not find TaskManager holding the key", taskManagerToKill);

			// Kill the task manager
			taskManagerToKill.tell(PoisonPill.getInstance(), ActorRef.noSender());

			success = false;
			for (int i = 0; i < 10 && !success; i++) {
				try {
					// Wait for the expected error. We might have to retry if
					// the query is very fast.
					Await.result(client.getKvState(jobId, queryName, key, serializedKey), deadline.timeLeft());
					Thread.sleep(500);
				} catch (Throwable ignored) {
					success = true;
				}
			}

			assertTrue("Query did not fail", success);

			// Now start another task manager
			cluster.addTaskManager();

			final FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
			Future<byte[]> serializedResultFuture = getKvStateWithRetries(
					client,
					jobId,
					queryName,
					key,
					serializedKey,
					retryDelay);

			byte[] serializedResult = Await.result(serializedResultFuture, deadline.timeLeft());

			Tuple2<Integer, Long> result = KvStateRequestSerializer.deserializeValue(
					serializedResult,
					queryableState.getValueSerializer());

			assertTrue("Count moved backwards", result.f1 >= countForKey);
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	/**
	 * Tests that duplicate query registrations fail the job at the JobManager.
	 */
	@Test
	public void testDuplicateRegistrationFailsJob() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();
		final int numKeys = 1024;

		JobID jobId = null;

		try {
			//
			// Test program
			//
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestKeyRangeSource(numKeys));

			// Reducing state
			ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState = new ReducingStateDescriptor<>(
					"any-name",
					new SumReduce(),
					source.getType());

			final String queryName = "duplicate-me";

			final QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState(queryName, reducingState);

			final QueryableStateStream<Integer, Tuple2<Integer, Long>> duplicate =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState(queryName);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			Future<TestingJobManagerMessages.JobStatusIs> failedFuture = cluster
					.getLeaderGateway(deadline.timeLeft())
					.ask(new NotifyWhenJobStatus(jobId, JobStatus.FAILED), deadline.timeLeft())
					.mapTo(ClassTag$.MODULE$.<JobStatusIs>apply(JobStatusIs.class));

			cluster.submitJobDetached(jobGraph);

			JobStatusIs jobStatus = Await.result(failedFuture, deadline.timeLeft());
			assertEquals(JobStatus.FAILED, jobStatus.state());

			// Get the job and check the cause
			JobFound jobFound = Await.result(
					cluster.getLeaderGateway(deadline.timeLeft())
							.ask(new JobManagerMessages.RequestJob(jobId), deadline.timeLeft())
							.mapTo(ClassTag$.MODULE$.<JobFound>apply(JobFound.class)),
					deadline.timeLeft());

			String failureCause = jobFound.executionGraph().getFailureCauseAsString();

			assertTrue("Not instance of SuppressRestartsException", failureCause.startsWith("org.apache.flink.runtime.execution.SuppressRestartsException"));
			int causedByIndex = failureCause.indexOf("Caused by: ");
			String subFailureCause = failureCause.substring(causedByIndex + "Caused by: ".length());
			assertTrue("Not caused by IllegalStateException", subFailureCause.startsWith("java.lang.IllegalStateException"));
			assertTrue("Exception does not contain registration name", subFailureCause.contains(queryName));
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}
		}
	}

	/**
	 * Tests simple value state queryable state instance. Each source emits
	 * (subtaskIndex, 0)..(subtaskIndex, numElements) tuples, which are then
	 * queried. The tests succeeds after each subtask index is queried with
	 * value numElements (the latest element updated the state).
	 */
	@Test
	public void testValueState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// Value state
			ValueStateDescriptor<Tuple2<Integer, Long>> valueState = new ValueStateDescriptor<>(
					"any",
					source.getType(),
					null);

			QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState("hakuna", valueState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// Now query
			long expected = numElements;

			FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
			for (int key = 0; key < NUM_SLOTS; key++) {
				final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
						key,
						queryableState.getKeySerializer(),
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				boolean success = false;
				while (deadline.hasTimeLeft() && !success) {
					Future<byte[]> future = getKvStateWithRetries(client,
							jobId,
							queryableState.getQueryableStateName(),
							key,
							serializedKey,
							retryDelay);

					byte[] serializedValue = Await.result(future, deadline.timeLeft());

					Tuple2<Integer, Long> value = KvStateRequestSerializer.deserializeValue(
							serializedValue,
							queryableState.getValueSerializer());

					assertEquals("Key mismatch", key, value.f0.intValue());
					if (expected == value.f1) {
						success = true;
					} else {
						// Retry
						Thread.sleep(50);
					}
				}

				assertTrue("Did not succeed query", success);
			}
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	/**
	 * Tests simple value state queryable state instance. Each source emits
	 * (subtaskIndex, 0)..(subtaskIndex, numElements) tuples, which are then
	 * queried. The tests succeeds after each subtask index is queried with
	 * value numElements (the latest element updated the state).
	 *
	 * This is the same as the simple value state test, but uses the API shortcut.
	 */
	@Test
	public void testValueStateShortcut() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// Value state shortcut
			QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState("matata");

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// Now query
			long expected = numElements;

			FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
			for (int key = 0; key < NUM_SLOTS; key++) {
				final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
						key,
						queryableState.getKeySerializer(),
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				boolean success = false;
				while (deadline.hasTimeLeft() && !success) {
					Future<byte[]> future = getKvStateWithRetries(client,
							jobId,
							queryableState.getQueryableStateName(),
							key,
							serializedKey,
							retryDelay);

					byte[] serializedValue = Await.result(future, deadline.timeLeft());

					Tuple2<Integer, Long> value = KvStateRequestSerializer.deserializeValue(
							serializedValue,
							queryableState.getValueSerializer());

					assertEquals("Key mismatch", key, value.f0.intValue());
					if (expected == value.f1) {
						success = true;
					} else {
						// Retry
						Thread.sleep(50);
					}
				}

				assertTrue("Did not succeed query", success);
			}
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	/**
	 * Tests simple list state queryable state instance. Each source emits
	 * (subtaskIndex, 0)..(subtaskIndex, numElements) tuples, which are then
	 * queried. The tests succeeds after each subtask index is queried with
	 * a list of size numElements and each emitted tuple is part of the list.
	 */
	@Test
	public void testListState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 128;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// List state
			ListStateDescriptor<Tuple2<Integer, Long>> listState = new ListStateDescriptor<>(
					"any",
					source.getType());

			QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState("timon", listState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// Now query
			long expected = numElements + 1; // +1 for 0-value

			FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
			for (int key = 0; key < NUM_SLOTS; key++) {
				final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
						key,
						queryableState.getKeySerializer(),
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				boolean success = false;
				while (deadline.hasTimeLeft() && !success) {
					Future<byte[]> future = getKvStateWithRetries(client,
							jobId,
							queryableState.getQueryableStateName(),
							key,
							serializedKey,
							retryDelay);

					byte[] serializedValue = Await.result(future, deadline.timeLeft());

					List<Tuple2<Integer, Long>> list = KvStateRequestSerializer.deserializeList(
							serializedValue,
							queryableState.getValueSerializer());

					if (list.size() == expected) {
						for (int i = 0; i < expected; i++) {
							Tuple2<Integer, Long> elem = list.get(i);
							assertEquals("Key mismatch", key, elem.f0.intValue());
							assertEquals("Value mismatch", i, elem.f1.longValue());
						}

						success = true;
					} else {
						// Retry
						Thread.sleep(50);
					}
				}

				assertTrue("Did not succeed query", success);
			}
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	/**
	 * Tests simple folding state queryable state instance. Each source emits
	 * (subtaskIndex, 0)..(subtaskIndex, numElements) tuples, which are then
	 * queried. The folding state sums these up and maps them to Strings. The
	 * test succeeds after each subtask index is queried with result n*(n+1)/2
	 * (as a String).
	 */
	@Test
	public void testFoldingState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// Folding state
			FoldingStateDescriptor<Tuple2<Integer, Long>, String> foldingState =
					new FoldingStateDescriptor<>(
							"any",
							"0",
							new SumFold(),
							StringSerializer.INSTANCE);

			QueryableStateStream<Integer, String> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState("pumba", foldingState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// Now query
			String expected = Integer.toString(numElements * (numElements + 1) / 2);

			FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
			for (int key = 0; key < NUM_SLOTS; key++) {
				final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
						key,
						queryableState.getKeySerializer(),
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				boolean success = false;
				while (deadline.hasTimeLeft() && !success) {
					Future<byte[]> future = getKvStateWithRetries(client,
							jobId,
							queryableState.getQueryableStateName(),
							key,
							serializedKey,
							retryDelay);

					byte[] serializedValue = Await.result(future, deadline.timeLeft());

					String value = KvStateRequestSerializer.deserializeValue(
							serializedValue,
							queryableState.getValueSerializer());

					if (expected.equals(value)) {
						success = true;
					} else {
						// Retry
						Thread.sleep(50);
					}
				}

				assertTrue("Did not succeed query", success);
			}
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	/**
	 * Tests simple reducing state queryable state instance. Each source emits
	 * (subtaskIndex, 0)..(subtaskIndex, numElements) tuples, which are then
	 * queried. The reducing state instance sums these up. The test succeeds
	 * after each subtask index is queried with result n*(n+1)/2.
	 */
	@Test
	public void testReducingState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(NUM_SLOTS);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// Reducing state
			ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState =
					new ReducingStateDescriptor<>(
							"any",
							new SumReduce(),
							source.getType());

			QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState("jungle", reducingState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// Wait until job is running

			// Now query
			long expected = numElements * (numElements + 1) / 2;

			FiniteDuration retryDelay = new FiniteDuration(1, TimeUnit.SECONDS);
			for (int key = 0; key < NUM_SLOTS; key++) {
				final byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
						key,
						queryableState.getKeySerializer(),
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE);

				boolean success = false;
				while (deadline.hasTimeLeft() && !success) {
					Future<byte[]> future = getKvStateWithRetries(client,
							jobId,
							queryableState.getQueryableStateName(),
							key,
							serializedKey,
							retryDelay);

					byte[] serializedValue = Await.result(future, deadline.timeLeft());

					Tuple2<Integer, Long> value = KvStateRequestSerializer.deserializeValue(
							serializedValue,
							queryableState.getValueSerializer());

					assertEquals("Key mismatch", key, value.f0.intValue());
					if (expected == value.f1) {
						success = true;
					} else {
						// Retry
						Thread.sleep(50);
					}
				}

				assertTrue("Did not succeed query", success);
			}
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(CancellationSuccess.class));

				Await.ready(cancellation, deadline.timeLeft());
			}

			client.shutDown();
		}
	}

	@SuppressWarnings("unchecked")
	private static Future<byte[]> getKvStateWithRetries(
			final QueryableStateClient client,
			final JobID jobId,
			final String queryName,
			final int key,
			final byte[] serializedKey,
			final FiniteDuration retryDelay) {

		return client.getKvState(jobId, queryName, key, serializedKey)
				.recoverWith(new Recover<Future<byte[]>>() {
					@Override
					public Future<byte[]> recover(Throwable failure) throws Throwable {
						if (failure instanceof AssertionError) {
							return Futures.failed(failure);
						} else {
							// At startup some failures are expected
							// due to races. Make sure that they don't
							// fail this test.
							return Patterns.after(
									retryDelay,
									TEST_ACTOR_SYSTEM.scheduler(),
									TEST_ACTOR_SYSTEM.dispatcher(),
									new Callable<Future<byte[]>>() {
										@Override
										public Future<byte[]> call() throws Exception {
											return getKvStateWithRetries(
													client,
													jobId,
													queryName,
													key,
													serializedKey,
													retryDelay);
										}
									});
						}
					}
				}, TEST_ACTOR_SYSTEM.dispatcher());
	}

	/**
	 * Test source producing (key, 0)..(key, maxValue) with key being the sub
	 * task index.
	 *
	 * <p>After all tuples have been emitted, the source waits to be cancelled
	 * and does not immediately finish.
	 */
	private static class TestAscendingValueSource extends RichParallelSourceFunction<Tuple2<Integer, Long>> {

		private final long maxValue;
		private volatile boolean isRunning = true;

		public TestAscendingValueSource(long maxValue) {
			Preconditions.checkArgument(maxValue >= 0);
			this.maxValue = maxValue;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
			// f0 => key
			int key = getRuntimeContext().getIndexOfThisSubtask();
			Tuple2<Integer, Long> record = new Tuple2<>(key, 0L);

			long currentValue = 0;
			while (isRunning && currentValue <= maxValue) {
				synchronized (ctx.getCheckpointLock()) {
					record.f1 = currentValue;
					ctx.collect(record);
				}

				currentValue++;
			}

			while (isRunning) {
				synchronized (this) {
					this.wait();
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;

			synchronized (this) {
				this.notifyAll();
			}
		}

	}

	/**
	 * Test source producing (key, 1) tuples with random key in key range (numKeys).
	 */
	private static class TestKeyRangeSource extends RichParallelSourceFunction<Tuple2<Integer, Long>>
			implements CheckpointListener {

		private final static AtomicLong LATEST_CHECKPOINT_ID = new AtomicLong();
		private final int numKeys;
		private final ThreadLocalRandom random = ThreadLocalRandom.current();
		private volatile boolean isRunning = true;

		public TestKeyRangeSource(int numKeys) {
			this.numKeys = numKeys;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				LATEST_CHECKPOINT_ID.set(0);
			}
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
			// f0 => key
			Tuple2<Integer, Long> record = new Tuple2<>(0, 1L);

			while (isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					record.f0 = random.nextInt(numKeys);
					ctx.collect(record);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				LATEST_CHECKPOINT_ID.set(checkpointId);
			}
		}
	}

	private static class SumFold implements FoldFunction<Tuple2<Integer, Long>, String> {
		@Override
		public String fold(String accumulator, Tuple2<Integer, Long> value) throws Exception {
			long acc = Long.valueOf(accumulator);
			acc += value.f1;
			return Long.toString(acc);
		}
	}

	private static class SumReduce implements ReduceFunction<Tuple2<Integer, Long>> {
		@Override
		public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) throws Exception {
			value1.f1 += value2.f1;
			return value1;
		}
	}

}
