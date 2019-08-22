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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview.JobIdWithStatus;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration.Builder;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIdsWithStatusesOverviewHeaders;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo.VertexTaskDetail;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT case for fine-grained recovery of batch jobs.
 *
 * <p>The test activates the region fail-over strategy to restart only failed producers.
 * The test job is a sequence of non-parallel mappers. Each mapper writes to a blocking partition which means
 * the next mapper starts when the previous is done. The mappers are not chained into one task which makes them
 * separate fail-over regions.
 *
 * <p>The test verifies the fine-grained recovery by including one failure after a random record for each failure
 * strategy in all mappers and comparing expected number of each mapper restarts against the actual restarts.
 * There are multiple failure strategies:
 *
 * <ul>
 *   <li> The {@link ExceptionFailureStrategy} throws an exception in the user function code.
 *   Since all mappers are connected via blocking partitions, which should be re-used on failure, and the consumer
 *   of the mapper wasn't deployed yet, as the consumed partition was not fully produced yet, only the failed mapper
 *   should actually restart.
 *   <li> The {@link TaskExecutorFailureStrategy} abruptly shuts down the task executor. This leads to the loss
 *   of all previously completed and the in-progress mapper result partitions. The fail-over strategy should restart
 *   the current in-progress mapper which will get the {@link PartitionNotFoundException} because the previous result
 *   becomes unavailable and the previous mapper has to be restarted as well. The same should happen subsequently with
 *   all previous mappers. When the source is recomputed, all mappers has to be restarted again to recalculate their
 *   lost results.
 * </ul>
 */
public class BatchFineGrainedRecoveryITCase extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(BatchFineGrainedRecoveryITCase.class);

	private static final int EMITTED_RECORD_NUMBER = 1000;
	private static final int MAP_NUMBER = 3;

	private static final String MAP_PARTITION_TEST_PARTITION_MAPPER = "MapPartition (Test partition mapper ";
	private static final Pattern MAPPER_NUMBER_IN_TASK_NAME_PATTERN =
		Pattern.compile("MapPartition \\(Test partition mapper (\\d+)\\)");

	/**
	 * Number of job failures for all mappers due to backtracking when the produced partitions get lost.
	 *
	 * <p>Each mapper failure produces number of backtracking failures (partition not found) which is the mapper index + 1,
	 * because all previous producers have to be restarted and they firstly will not find the previous result.
	 */
	private static final int ALL_MAPPERS_BACKTRACK_FAILURES = IntStream.range(0, MAP_NUMBER + 1).sum();

	/**
	 * Max number of job failures.
	 *
	 * <p>For each possible mapper failure, it is all possible backtracking failures plus the generated failures themselves of each type.
	 */
	private static final int MAX_JOB_RESTART_ATTEMPTS = ALL_MAPPERS_BACKTRACK_FAILURES + 2 * MAP_NUMBER;

	/**
	 * Expected attempt number for each mapper.
	 */
	private static final int[] EXPECTED_MAP_ATTEMPT_NUMBERS = IntStream
		.range(0, MAP_NUMBER)
		.map(i ->
			// exception failure:
				1 + // this mapper
				i + // previous mappers
			// TM failure:
				(MAP_NUMBER - i - 1) + // subsequent mappers after PartitionNotFoundException
				1 + // this mapper
				1 + // this mapper after PartitionNotFoundException
				i + // previous mappers
				i) // previous mappers after PartitionNotFoundException
		.toArray();

	private static final String TASK_NAME_PREFIX = "Test partition mapper ";

	private static final List<Long> EXPECTED_JOB_OUTPUT =
		LongStream.range(MAP_NUMBER, EMITTED_RECORD_NUMBER + MAP_NUMBER).boxed().collect(Collectors.toList());

	private static TestingMiniCluster miniCluster;

	private static MiniClusterClient client;

	private static AtomicInteger lastTaskManagerIndexInMiniCluster;

	private static final Random rnd = new Random();

	private static GlobalMapFailureTracker failureTracker;

	@SuppressWarnings("OverlyBroadThrowsClause")
	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, PIPELINED_REGION_RESTART_STRATEGY_NAME);

		miniCluster = new TestingMiniCluster(
			new Builder()
				.setNumTaskManagers(1)
				.setNumSlotsPerTaskManager(1)
				.setConfiguration(configuration)
				.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
				.build(),
			null);
		miniCluster.start();

		client = new MiniClusterClient(miniCluster);

		lastTaskManagerIndexInMiniCluster = new AtomicInteger(0);

		failureTracker = new GlobalMapFailureTracker(MAP_NUMBER);
	}

	@After
	public void teardown() throws Exception {
		if (miniCluster != null) {
			miniCluster.close();
		}

		if (client != null) {
			client.close();
		}
	}

	@Test
	public void testProgram() throws Exception {
		ExecutionEnvironment env = createExecutionEnvironment();

		DataSet<Long> input = env.generateSequence(0, EMITTED_RECORD_NUMBER - 1);
		for (int trackingIndex = 0; trackingIndex < MAP_NUMBER; trackingIndex++) {
			input = input
				.mapPartition(new TestPartitionMapper(trackingIndex,  createFailureStrategy(trackingIndex)))
				.name(TASK_NAME_PREFIX + trackingIndex);
		}

		assertThat(input.collect(), is(EXPECTED_JOB_OUTPUT));
		failureTracker.verify(getMapperAttempts());
	}

	private static FailureStrategy createFailureStrategy(int trackingIndex) {
		int failWithExceptionAfterNumberOfProcessedRecords = rnd.nextInt(EMITTED_RECORD_NUMBER) + 1;
		int failTaskExecutorAfterNumberOfProcessedRecords = rnd.nextInt(EMITTED_RECORD_NUMBER) + 1;
		// it has to fail only once during one mapper run so that different failure strategies do not mess up each other stats
		FailureStrategy failureStrategy = new OneTimeFailureStrategy(
			new JoinedFailureStrategy(
				new GloballyTrackingFailureStrategy(
					new ExceptionFailureStrategy(failWithExceptionAfterNumberOfProcessedRecords)),
				new GloballyTrackingFailureStrategy(
					new TaskExecutorFailureStrategy(failTaskExecutorAfterNumberOfProcessedRecords))));
		LOG.info("FailureStrategy for the mapper {}: {}", trackingIndex, failureStrategy);
		return failureStrategy;
	}

	private static ExecutionEnvironment createExecutionEnvironment() {
		@SuppressWarnings("StaticVariableUsedBeforeInitialization")
		ExecutionEnvironment env = new TestEnvironment(miniCluster, 1, true);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(MAX_JOB_RESTART_ATTEMPTS, Time.milliseconds(10)));
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED); // forces all partitions to be blocking
		return env;
	}

	@SuppressWarnings({"StaticVariableUsedBeforeInitialization", "OverlyBroadThrowsClause"})
	private static void restartTaskManager() throws Exception {
		int tmi = lastTaskManagerIndexInMiniCluster.getAndIncrement();
		try {
			miniCluster.terminateTaskExecutor(tmi).get();
		} finally {
			miniCluster.startTaskExecutor();
		}
	}

	private static int[] getMapperAttempts() {
		int[] attempts = new int[MAP_NUMBER];
		//noinspection StaticVariableUsedBeforeInitialization
		client
			.getInternalTaskInfos()
			.stream()
			.filter(t -> t.name.startsWith(MAP_PARTITION_TEST_PARTITION_MAPPER))
			.forEach(t -> attempts[parseMapperNumberFromTaskName(t.name)] = t.attempt);
		return attempts;
	}

	private static int parseMapperNumberFromTaskName(String name) {
		Matcher m = MAPPER_NUMBER_IN_TASK_NAME_PATTERN.matcher(name);
		if (m.matches()) {
			return Integer.parseInt(m.group(1));
		} else {
			throw new FlinkRuntimeException("Failed to find mapper number in its task name: " + name);
		}
	}

	@FunctionalInterface
	private interface FailureStrategy extends Serializable {
		/**
		 * Decides whether to fail and fails the task implicitly or by throwing an exception.
		 *
		 * @param trackingIndex index of the mapper task in the sequence
		 * @return {@code true} if task is failed implicitly or {@code false} if task is not failed
		 * @throws Exception To fail the task explicitly
		 */
		boolean failOrNot(int trackingIndex) throws Exception;
	}

	private static class OneTimeFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final FailureStrategy wrappedFailureStrategy;
		private transient boolean failed;

		private OneTimeFailureStrategy(FailureStrategy wrappedFailureStrategy) {
			this.wrappedFailureStrategy = wrappedFailureStrategy;
		}

		@Override
		public boolean failOrNot(int trackingIndex) throws Exception {
			if (!failed) {
				try {
					boolean failedNow = wrappedFailureStrategy.failOrNot(trackingIndex);
					failed = failedNow;
					return failedNow;
				} catch (Exception e) {
					failed = true;
					throw e;
				}
			}
			return false;
		}

		@Override
		public String toString() {
			return "FailingOnce{" + wrappedFailureStrategy + '}';
		}
	}

	private static class JoinedFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final FailureStrategy[] failureStrategies;

		private JoinedFailureStrategy(FailureStrategy ... failureStrategies) {
			this.failureStrategies = failureStrategies;
		}

		@Override
		public boolean failOrNot(int trackingIndex) throws Exception {
			for (FailureStrategy failureStrategy : failureStrategies) {
				if (failureStrategy.failOrNot(trackingIndex)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public String toString() {
			return String.join(
				" or ",
				(Iterable<String>) () -> Arrays.stream(failureStrategies).map(Object::toString).iterator());
		}
	}

	private static class GloballyTrackingFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final FailureStrategy wrappedFailureStrategy;

		private GloballyTrackingFailureStrategy(FailureStrategy wrappedFailureStrategy) {
			this.wrappedFailureStrategy = wrappedFailureStrategy;
		}

		@Override
		public boolean failOrNot(int trackingIndex) throws Exception {
			return failureTracker.failOrNot(
				trackingIndex,
				wrappedFailureStrategy);
		}

		@Override
		public String toString() {
			return "Tracked{" + wrappedFailureStrategy + '}';
		}
	}

	private static class ExceptionFailureStrategy extends AbstractOnceAfterCallNumberFailureStrategy {
		private static final long serialVersionUID = 1L;

		private ExceptionFailureStrategy(int failAfterCallNumber) {
			super(failAfterCallNumber);
		}

		@Override
		void fail(int trackingIndex) throws FlinkException {
			throw new FlinkException("BAGA-BOOM!!! The user function generated test failure.");
		}
	}

	private static class TaskExecutorFailureStrategy extends AbstractOnceAfterCallNumberFailureStrategy {
		private static final long serialVersionUID = 1L;

		private TaskExecutorFailureStrategy(int failAfterCallNumber) {
			super(failAfterCallNumber);
		}

		@Override
		void fail(int trackingIndex) throws Exception {
			//noinspection OverlyBroadCatchBlock
			try {
				restartTaskManager();
			} catch (InterruptedException e) {
				// ignore the exception, task should have been failed while stopping TM
				Thread.currentThread().interrupt();
			} catch (Throwable t) {
				failureTracker.unrelatedFailure(t);
				throw t;
			}
		}
	}

	private abstract static class AbstractOnceAfterCallNumberFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final UUID id;
		private final int failAfterCallNumber;
		private transient int callCounter;

		private AbstractOnceAfterCallNumberFailureStrategy(int failAfterCallNumber) {
			this.failAfterCallNumber = failAfterCallNumber;
			id = UUID.randomUUID();
		}

		@Override
		public boolean failOrNot(int trackingIndex) throws Exception {
			callCounter++;
			boolean generateFailure = callCounter == failAfterCallNumber;
			if (generateFailure) {
				fail(trackingIndex);
			}
			return generateFailure;
		}

		abstract void fail(int trackingIndex) throws Exception;

		@Override
		public String toString() {
			return this.getClass().getSimpleName() + " (fail after " + failAfterCallNumber + " calls)";
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			return Objects.equals(id, ((AbstractOnceAfterCallNumberFailureStrategy) o).id);
		}

		@Override
		public int hashCode() {
			return id.hashCode();
		}
	}

	private static class GlobalMapFailureTracker {
		private final List<Set<FailureStrategy>> mapFailures;

		private final Object classLock = new Object();
		@GuardedBy("classLock")
		private Throwable unexpectedFailure;

		private GlobalMapFailureTracker(int numberOfMappers) {
			mapFailures = new ArrayList<>(numberOfMappers);
			IntStream.range(0, numberOfMappers).forEach(i -> addNewMapper());
		}

		private int addNewMapper() {
			mapFailures.add(new HashSet<>(2));
			return mapFailures.size() - 1;
		}

		private boolean failOrNot(int index, FailureStrategy failureStrategy) throws Exception {
			boolean alreadyFailed = mapFailures.get(index).contains(failureStrategy);
			boolean failedNow = false;
			try {
				failedNow = !alreadyFailed && failureStrategy.failOrNot(index);
			} catch (Exception e) {
				failedNow = true;
				throw e;
			} finally {
				if (failedNow) {
					mapFailures.get(index).add(failureStrategy);
				}
			}
			return failedNow;
		}

		private void unrelatedFailure(Throwable failure) {
			synchronized (classLock) {
				unexpectedFailure = ExceptionUtils.firstOrSuppressed(failure, unexpectedFailure);
			}
		}

		private void verify(int[] mapAttemptNumbers) {
			synchronized (classLock) {
				if (unexpectedFailure != null) {
					throw new AssertionError("Test failed due to unexpected exception.", unexpectedFailure);
				}
			}
			assertThat(mapAttemptNumbers, is(EXPECTED_MAP_ATTEMPT_NUMBERS));
		}
	}

	private static class TestPartitionMapper extends RichMapPartitionFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		private final int trackingIndex;
		private final FailureStrategy failureStrategy;

		private TestPartitionMapper(int trackingIndex, FailureStrategy failureStrategy) {
			this.trackingIndex = trackingIndex;
			this.failureStrategy = failureStrategy;
		}

		@Override
		public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
			for (Long value : values) {
				failureStrategy.failOrNot(trackingIndex);
				out.collect(value + 1);
			}
		}
	}

	private static class MiniClusterClient implements AutoCloseable {
		private final RestClient restClient;
		private final ExecutorService executorService;
		private final URI restAddress;

		private MiniClusterClient(TestingMiniCluster miniCluster) throws ConfigurationException {
			restAddress = miniCluster.getRestAddress().join();
			executorService = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("Flink-RestClient-IO"));
			restClient = createRestClient();
		}

		private RestClient createRestClient() throws ConfigurationException {
			return new RestClient(
				RestClientConfiguration.fromConfiguration(new UnmodifiableConfiguration(new Configuration())),
				executorService);
		}

		private List<InternalTaskInfo> getInternalTaskInfos() {
			return getJobs()
				.stream()
				.flatMap(jobId -> getJobDetails(jobId).join()
					.getJobVertexInfos()
					.stream()
					.map(info -> Tuple2.of(jobId, info)))
				.flatMap(vertexInfoWithJobId ->
					getJobVertexDetailsInfo(vertexInfoWithJobId.f0, vertexInfoWithJobId.f1.getJobVertexID())
						.getSubtasks()
						.stream()
						.map(subtask -> new InternalTaskInfo(vertexInfoWithJobId.f1.getName(), subtask)))
				.collect(Collectors.toList());
		}

		private Collection<JobID> getJobs() {
			JobIdsWithStatusOverview jobIds =
				sendRequest(
					JobIdsWithStatusesOverviewHeaders.getInstance(),
					EmptyMessageParameters.getInstance())
				.join();
			return jobIds
				.getJobsWithStatus()
				.stream()
				.map(JobIdWithStatus::getJobId)
				.collect(Collectors.toList());
		}

		private CompletableFuture<JobDetailsInfo> getJobDetails(JobID jobId) {
			JobMessageParameters params = new JobMessageParameters();
			params.jobPathParameter.resolve(jobId);
			return sendRequest(JobDetailsHeaders.getInstance(), params);
		}

		private JobVertexDetailsInfo getJobVertexDetailsInfo(JobID jobId, JobVertexID jobVertexID) {
			JobVertexDetailsHeaders detailsHeaders = JobVertexDetailsHeaders.getInstance();
			JobVertexMessageParameters params = new JobVertexMessageParameters();
			params.jobPathParameter.resolve(jobId);
			params.jobVertexIdPathParameter.resolve(jobVertexID);
			return sendRequest(detailsHeaders, params).join();
		}

		private <M extends MessageHeaders<EmptyRequestBody, P, U>, U extends MessageParameters, P extends ResponseBody> CompletableFuture<P>
				sendRequest(M messageHeaders, U messageParameters) {
			try {
				return restClient.sendRequest(
					restAddress.getHost(),
					restAddress.getPort(),
					messageHeaders,
					messageParameters,
					EmptyRequestBody.getInstance());
			} catch (IOException e) {
				return FutureUtils.completedExceptionally(e);
			}
		}

		@Override
		public void close() throws Exception {
			restClient.close();
			executorService.shutdownNow();
		}
	}

	private static class InternalTaskInfo {
		private final String name;
		private final int attempt;

		private InternalTaskInfo(String name, VertexTaskDetail vertexTaskDetail) {
			this.name = name;
			this.attempt = vertexTaskDetail.getAttempt();
		}

		@Override
		public String toString() {
			return name + " (Attempt #" + attempt + ')';
		}
	}
}
