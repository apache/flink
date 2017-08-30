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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.query.QueryableStateClient;
import org.apache.flink.runtime.query.netty.UnknownKeyOrNamespace;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceTypeInfo;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.OnSuccess;
import akka.dispatch.Recover;
import akka.pattern.Patterns;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for queryable state integration tests with a configurable state backend.
 */
public abstract class AbstractQueryableStateITCase extends TestLogger {

	protected static final FiniteDuration TEST_TIMEOUT = new FiniteDuration(10000, TimeUnit.SECONDS);
	private static final FiniteDuration QUERY_RETRY_DELAY = new FiniteDuration(100, TimeUnit.MILLISECONDS);

	protected static ActorSystem testActorSystem;

	/**
	 * State backend to use.
	 */
	protected AbstractStateBackend stateBackend;

	/**
	 * Shared between all the test. Make sure to have at least NUM_SLOTS
	 * available after your test finishes, e.g. cancel the job you submitted.
	 */
	protected static FlinkMiniCluster cluster;

	protected static int maxParallelism;

	@Before
	public void setUp() throws Exception {
		// NOTE: do not use a shared instance for all tests as the tests may brake
		this.stateBackend = createStateBackend();

		Assert.assertNotNull(cluster);

		maxParallelism = cluster.configuration().getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1) *
				cluster.configuration().getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
	}

	/**
	 * Creates a state backend instance which is used in the {@link #setUp()} method before each
	 * test case.
	 *
	 * @return a state backend instance for each unit test
	 */
	protected abstract AbstractStateBackend createStateBackend() throws Exception;

	/**
	 * Runs a simple topology producing random (key, 1) pairs at the sources (where
	 * number of keys is in fixed in range 0...numKeys). The records are keyed and
	 * a reducing queryable state instance is created, which sums up the records.
	 *
	 * <p>After submitting the job in detached mode, the QueryableStateCLient is used
	 * to query the counts of each key in rounds until all keys have non-zero counts.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testQueryableState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();
		final int numKeys = 256;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;

		try {
			//
			// Test program
			//
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
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
						private static final long serialVersionUID = 7143749578983540352L;

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

			boolean allNonZero = false;
			while (!allNonZero && deadline.hasTimeLeft()) {
				allNonZero = true;

				final List<Future<Tuple2<Integer, Long>>> futures = new ArrayList<>(numKeys);

				for (int i = 0; i < numKeys; i++) {
					final int key = i;

					if (counts.get(key) > 0) {
						// Skip this one
						continue;
					} else {
						allNonZero = false;
					}

					Future<Tuple2<Integer, Long>> result = getKvStateWithRetries(
							client,
							jobId,
							queryName,
							key,
							BasicTypeInfo.INT_TYPE_INFO,
							reducingState,
							QUERY_RETRY_DELAY,
							false);

					result.onSuccess(new OnSuccess<Tuple2<Integer, Long>>() {
						@Override
						public void onSuccess(Tuple2<Integer, Long> result) throws Throwable {
							counts.set(key, result.f1);
							assertEquals("Key mismatch", key, result.f0.intValue());
						}
					}, testActorSystem.dispatcher());

					futures.add(result);
				}

				Future<Iterable<Tuple2<Integer, Long>>> futureSequence = Futures.sequence(
						futures,
						testActorSystem.dispatcher());

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
	 * Tests that duplicate query registrations fail the job at the JobManager.
	 *
	 * <b>NOTE: </b> This test is only in the non-HA variant of the tests because
	 * in the HA mode we use the actual JM code which does not recognize the
	 * {@code NotifyWhenJobStatus} message.	 *
	 */
	@Test
	public void testDuplicateRegistrationFailsJob() throws Exception {
		final Deadline deadline = TEST_TIMEOUT.fromNow();
		final int numKeys = 256;

		JobID jobId = null;

		try {
			//
			// Test program
			//
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
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
						private static final long serialVersionUID = -4126824763829132959L;

						@Override
						public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
							return value.f0;
						}
					}).asQueryableState(queryName, reducingState);

			final QueryableStateStream<Integer, Tuple2<Integer, Long>> duplicate =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						private static final long serialVersionUID = -6265024000462809436L;

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
					.ask(new TestingJobManagerMessages.NotifyWhenJobStatus(jobId, JobStatus.FAILED), deadline.timeLeft())
					.mapTo(ClassTag$.MODULE$.<TestingJobManagerMessages.JobStatusIs>apply(TestingJobManagerMessages.JobStatusIs.class));

			cluster.submitJobDetached(jobGraph);

			TestingJobManagerMessages.JobStatusIs jobStatus = Await.result(failedFuture, deadline.timeLeft());
			assertEquals(JobStatus.FAILED, jobStatus.state());

			// Get the job and check the cause
			JobManagerMessages.JobFound jobFound = Await.result(
					cluster.getLeaderGateway(deadline.timeLeft())
							.ask(new JobManagerMessages.RequestJob(jobId), deadline.timeLeft())
							.mapTo(ClassTag$.MODULE$.<JobManagerMessages.JobFound>apply(JobManagerMessages.JobFound.class)),
					deadline.timeLeft());

			String failureCause = jobFound.executionGraph().getFailureCause().getExceptionAsString();

			assertTrue("Not instance of SuppressRestartsException", failureCause.startsWith("org.apache.flink.runtime.execution.SuppressRestartsException"));
			int causedByIndex = failureCause.indexOf("Caused by: ");
			String subFailureCause = failureCause.substring(causedByIndex + "Caused by: ".length());
			assertTrue("Not caused by IllegalStateException", subFailureCause.startsWith("java.lang.IllegalStateException"));
			assertTrue("Exception does not contain registration name", subFailureCause.contains(queryName));
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<JobManagerMessages.CancellationSuccess> cancellation = cluster
						.getLeaderGateway(deadline.timeLeft())
						.ask(new JobManagerMessages.CancelJob(jobId), deadline.timeLeft())
						.mapTo(ClassTag$.MODULE$.<JobManagerMessages.CancellationSuccess>apply(JobManagerMessages.CancellationSuccess.class));

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
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// Value state
			ValueStateDescriptor<Tuple2<Integer, Long>> valueState = new ValueStateDescriptor<>(
					"any",
					source.getType());

			QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						private static final long serialVersionUID = 7662520075515707428L;

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

			executeQuery(deadline, client, jobId, "hakuna", valueState, expected);
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
	 * Similar tests as {@link #testValueState()} but before submitting the
	 * job, we already issue one request which fails.
	 */
	@Test
	public void testQueryNonStartedJobState() throws Exception {
		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
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
					private static final long serialVersionUID = 7480503339992214681L;

					@Override
					public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
						return value.f0;
					}
				}).asQueryableState("hakuna", valueState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			// Now query
			long expected = numElements;

			// query once
			client.getKvState(
					jobId,
					queryableState.getQueryableStateName(),
					0,
					VoidNamespace.INSTANCE,
					BasicTypeInfo.INT_TYPE_INFO,
					VoidNamespaceTypeInfo.INSTANCE,
					valueState);

			cluster.submitJobDetached(jobGraph);

			executeQuery(deadline, client, jobId, "hakuna", valueState, expected);
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
	 * Retry a query for state for keys between 0 and {@link #maxParallelism} until
	 * <tt>expected</tt> equals the value of the result tuple's second field.
	 */
	private void executeQuery(
			final Deadline deadline,
			final QueryableStateClient client,
			final JobID jobId,
			final String queryableStateName,
			final StateDescriptor<?, Tuple2<Integer, Long>> stateDescriptor,
			final long expected) throws Exception {

		for (int key = 0; key < maxParallelism; key++) {
			boolean success = false;
			while (deadline.hasTimeLeft() && !success) {
				Future<Tuple2<Integer, Long>> future = getKvStateWithRetries(client,
					jobId,
					queryableStateName,
					key,
					BasicTypeInfo.INT_TYPE_INFO,
					stateDescriptor,
					QUERY_RETRY_DELAY,
					false);

				Tuple2<Integer, Long> value = Await.result(future, deadline.timeLeft());

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
	}

	/**
	 * Retry a query for state for keys between 0 and {@link #maxParallelism} until
	 * <tt>expected</tt> equals the value of the result tuple's second field.
	 */
	private void executeQuery(
			final Deadline deadline,
			final QueryableStateClient client,
			final JobID jobId,
			final String queryableStateName,
			final TypeSerializer<Tuple2<Integer, Long>> valueSerializer,
			final long expected) throws Exception {

		for (int key = 0; key < maxParallelism; key++) {
			boolean success = false;
			while (deadline.hasTimeLeft() && !success) {
				Future<Tuple2<Integer, Long>> future = getKvStateWithRetries(client,
						jobId,
						queryableStateName,
						key,
						BasicTypeInfo.INT_TYPE_INFO,
						valueSerializer,
						QUERY_RETRY_DELAY,
						false);

				Tuple2<Integer, Long> value = Await.result(future, deadline.timeLeft());

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
	}

	/**
	 * Tests simple value state queryable state instance with a default value
	 * set. Each source emits (subtaskIndex, 0)..(subtaskIndex, numElements)
	 * tuples, the key is mapped to 1 but key 0 is queried which should throw
	 * a {@link UnknownKeyOrNamespace} exception.
	 *
	 * @throws UnknownKeyOrNamespace thrown due querying a non-existent key
	 */
	@Test(expected = UnknownKeyOrNamespace.class)
	public void testValueStateDefault() throws
		Exception, UnknownKeyOrNamespace {

		// Config
		final Deadline deadline = TEST_TIMEOUT.fromNow();

		final int numElements = 1024;

		final QueryableStateClient client = new QueryableStateClient(cluster.configuration());

		JobID jobId = null;
		try {
			StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies
				.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
				.addSource(new TestAscendingValueSource(numElements));

			// Value state
			ValueStateDescriptor<Tuple2<Integer, Long>> valueState =
				new ValueStateDescriptor<>(
					"any",
					source.getType(),
					Tuple2.of(0, 1337L));

			// only expose key "1"
			QueryableStateStream<Integer, Tuple2<Integer, Long>>
				queryableState =
				source.keyBy(
					new KeySelector<Tuple2<Integer, Long>, Integer>() {
						private static final long serialVersionUID = 4509274556892655887L;

						@Override
						public Integer getKey(
							Tuple2<Integer, Long> value) throws
							Exception {
							return 1;
						}
					}).asQueryableState("hakuna", valueState);

			// Submit the job graph
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			jobId = jobGraph.getJobID();

			cluster.submitJobDetached(jobGraph);

			// Now query
			int key = 0;
			Future<Tuple2<Integer, Long>> future = getKvStateWithRetries(client,
				jobId,
				queryableState.getQueryableStateName(),
				key,
				BasicTypeInfo.INT_TYPE_INFO,
				valueState,
				QUERY_RETRY_DELAY,
				true);

			Await.result(future, deadline.timeLeft());
		} finally {
			// Free cluster resources
			if (jobId != null) {
				Future<CancellationSuccess> cancellation = cluster
					.getLeaderGateway(deadline.timeLeft())
					.ask(new JobManagerMessages.CancelJob(jobId),
						deadline.timeLeft())
					.mapTo(ClassTag$.MODULE$.<CancellationSuccess>apply(
						CancellationSuccess.class));

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
	 * <p>This is the same as the simple value state test, but uses the API shortcut.
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
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
			// Very important, because cluster is shared between tests and we
			// don't explicitly check that all slots are available before
			// submitting.
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));

			DataStream<Tuple2<Integer, Long>> source = env
					.addSource(new TestAscendingValueSource(numElements));

			// Value state shortcut
			QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
					source.keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
						private static final long serialVersionUID = 9168901838808830068L;

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

			executeQuery(deadline, client, jobId, "matata",
					queryableState.getValueSerializer(), expected);
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
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
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
						private static final long serialVersionUID = -842809958106747539L;

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

			for (int key = 0; key < maxParallelism; key++) {
				boolean success = false;
				while (deadline.hasTimeLeft() && !success) {
					Future<String> future = getKvStateWithRetries(client,
							jobId,
							queryableState.getQueryableStateName(),
							key,
							BasicTypeInfo.INT_TYPE_INFO,
							foldingState,
							QUERY_RETRY_DELAY,
							false);

					String value = Await.result(future, deadline.timeLeft());
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
			env.setStateBackend(stateBackend);
			env.setParallelism(maxParallelism);
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
						private static final long serialVersionUID = 8470749712274833552L;

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

			executeQuery(deadline, client, jobId, "jungle", reducingState, expected);
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

	private static <K, V> Future<V> getKvStateWithRetries(
			final QueryableStateClient client,
			final JobID jobId,
			final String queryName,
			final K key,
			final TypeInformation<K> keyTypeInfo,
			final TypeSerializer<V> valueTypeSerializer,
			final FiniteDuration retryDelay,
			final boolean failForUnknownKeyOrNamespace) {

		return client.getKvState(jobId, queryName, key, VoidNamespace.INSTANCE, keyTypeInfo, VoidNamespaceTypeInfo.INSTANCE, valueTypeSerializer)
				.recoverWith(new Recover<Future<V>>() {
					@Override
					public Future<V> recover(Throwable failure) throws Throwable {
						if (failure instanceof AssertionError) {
							return Futures.failed(failure);
						} else if (failForUnknownKeyOrNamespace &&
								(failure instanceof UnknownKeyOrNamespace)) {
							return Futures.failed(failure);
						} else {
							// At startup some failures are expected
							// due to races. Make sure that they don't
							// fail this test.
							return Patterns.after(
									retryDelay,
									testActorSystem.scheduler(),
									testActorSystem.dispatcher(),
									new Callable<Future<V>>() {
										@Override
										public Future<V> call() throws Exception {
											return getKvStateWithRetries(
													client,
													jobId,
													queryName,
													key,
													keyTypeInfo,
													valueTypeSerializer,
													retryDelay,
													failForUnknownKeyOrNamespace);
										}
									});
						}
					}
				}, testActorSystem.dispatcher());

	}

	private static <K, V> Future<V> getKvStateWithRetries(
			final QueryableStateClient client,
			final JobID jobId,
			final String queryName,
			final K key,
			final TypeInformation<K> keyTypeInfo,
			final StateDescriptor<?, V> stateDescriptor,
			final FiniteDuration retryDelay,
			final boolean failForUnknownKeyOrNamespace) {

		return client.getKvState(jobId, queryName, key, VoidNamespace.INSTANCE, keyTypeInfo, VoidNamespaceTypeInfo.INSTANCE, stateDescriptor)
				.recoverWith(new Recover<Future<V>>() {
					@Override
					public Future<V> recover(Throwable failure) throws Throwable {
						if (failure instanceof AssertionError) {
							return Futures.failed(failure);
						} else if (failForUnknownKeyOrNamespace &&
								(failure instanceof UnknownKeyOrNamespace)) {
							return Futures.failed(failure);
						} else {
							// At startup some failures are expected
							// due to races. Make sure that they don't
							// fail this test.
							return Patterns.after(
									retryDelay,
									testActorSystem.scheduler(),
									testActorSystem.dispatcher(),
									new Callable<Future<V>>() {
										@Override
										public Future<V> call() throws Exception {
											return getKvStateWithRetries(
													client,
													jobId,
													queryName,
													key,
													keyTypeInfo,
													stateDescriptor,
													retryDelay,
													failForUnknownKeyOrNamespace);
										}
									});
						}
					}
				}, testActorSystem.dispatcher());
	}

	/**
	 * Test source producing (key, 0)..(key, maxValue) with key being the sub
	 * task index.
	 *
	 * <p>After all tuples have been emitted, the source waits to be cancelled
	 * and does not immediately finish.
	 */
	private static class TestAscendingValueSource extends RichParallelSourceFunction<Tuple2<Integer, Long>> {

		private static final long serialVersionUID = 1459935229498173245L;

		private final long maxValue;
		private volatile boolean isRunning = true;

		TestAscendingValueSource(long maxValue) {
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
	protected static class TestKeyRangeSource extends RichParallelSourceFunction<Tuple2<Integer, Long>>
			implements CheckpointListener {
		private static final long serialVersionUID = -5744725196953582710L;

		private static final AtomicLong LATEST_CHECKPOINT_ID = new AtomicLong();
		private final int numKeys;
		private final ThreadLocalRandom random = ThreadLocalRandom.current();
		private volatile boolean isRunning = true;

		TestKeyRangeSource(int numKeys) {
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
				// mild slow down
				Thread.sleep(1);
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

	/**
	 * Test {@link FoldFunction} concatenating the already stored string with the long passed as argument.
	 */
	private static class SumFold implements FoldFunction<Tuple2<Integer, Long>, String> {
		private static final long serialVersionUID = -6249227626701264599L;

		@Override
		public String fold(String accumulator, Tuple2<Integer, Long> value) throws Exception {
			long acc = Long.valueOf(accumulator);
			acc += value.f1;
			return Long.toString(acc);
		}
	}

	/**
	 * Test {@link ReduceFunction} summing up its two arguments.
	 */
	protected static class SumReduce implements ReduceFunction<Tuple2<Integer, Long>> {
		private static final long serialVersionUID = -8651235077342052336L;

		@Override
		public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) throws Exception {
			value1.f1 += value2.f1;
			return value1;
		}
	}

}
