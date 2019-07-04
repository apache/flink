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
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration.Builder;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.configuration.JobManagerOptions.FORCE_PARTITION_RELEASE_ON_CONSUMPTION;
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
 * <p>The test verifies that fine-grained recovery works by randomly including failures in any of the mappers.
 * There are multiple failure strategies:
 *
 * <ul>
 *   <li> The {@link RandomExceptionFailureStrategy} throws an exception in the user function code.
 *   Since all mappers are connected via blocking partitions, which should be re-used on failure, and the consumer
 *   of the mapper wasn't deployed yet, as the consumed partition was not fully produced yet, only the failed mapper
 *   should actually restart.
 *   <li> The {@link RandomTaskExecutorFailureStrategy} abruptly shuts down the task executor. This leads to the loss
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
	private static final int MAX_MAP_FAILURES = 4;
	private static final int MAX_JOB_RESTART_ATTEMPTS = MAP_NUMBER * (MAP_NUMBER + 1) * MAX_MAP_FAILURES / 2;
	private static final String TASK_NAME_PREFIX = "Test partition mapper ";
	private static final List<Long> EXPECTED_JOB_OUTPUT =
		LongStream.range(MAP_NUMBER, EMITTED_RECORD_NUMBER + MAP_NUMBER).boxed().collect(Collectors.toList());

	private static TestingMiniCluster miniCluster;
	private static AtomicInteger lastTaskManagerIndexInMiniCluster;

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setBoolean(FORCE_PARTITION_RELEASE_ON_CONSUMPTION, false);
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

		lastTaskManagerIndexInMiniCluster = new AtomicInteger(0);

		StaticFailureCounter.reset();
		StaticMapFailureTracker.reset();
	}

	@After
	public void teardown() throws Exception {
		if (miniCluster != null) {
			miniCluster.close();
		}
	}

	@Test
	public void testProgram() throws Exception {
		ExecutionEnvironment env = createExecutionEnvironment();

		FailureStrategy failureStrategy = createFailureStrategy();

		DataSet<Long> input = env.generateSequence(0, EMITTED_RECORD_NUMBER - 1);
		for (int trackingIndex = 0; trackingIndex < MAP_NUMBER; trackingIndex++) {
			input = input
				.mapPartition(new TestPartitionMapper(StaticMapFailureTracker.addNewMap(), failureStrategy))
				.name(TASK_NAME_PREFIX + trackingIndex);
		}
		assertThat(input.collect(), is(EXPECTED_JOB_OUTPUT));

		StaticMapFailureTracker.verify();
	}

	private static FailureStrategy createFailureStrategy() {
		CoinToss coin = new CoinToss(1, EMITTED_RECORD_NUMBER);
		return new JoinedFailureStrategy(
			new RandomExceptionFailureStrategy(coin),
			new RandomTaskExecutorFailureStrategy(coin));
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
		miniCluster.terminateTaskExecutor(tmi).get();
		miniCluster.startTaskExecutor();
	}

	@FunctionalInterface
	private interface FailureStrategy extends Serializable {
		boolean failOrNot(int trackingIndex);
	}

	private static class JoinedFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final FailureStrategy[] failureStrategies;

		private JoinedFailureStrategy(FailureStrategy ... failureStrategies) {
			this.failureStrategies = failureStrategies;
		}

		@Override
		public boolean failOrNot(int trackingIndex) {
			return Arrays
				.stream(failureStrategies)
				.anyMatch(failureStrategy -> failureStrategy.failOrNot(trackingIndex));
		}
	}

	private static class RandomExceptionFailureStrategy extends AbstractRandomFailureStrategy {
		private static final long serialVersionUID = 1L;

		private RandomExceptionFailureStrategy(CoinToss coin) {
			super(coin);
		}

		@Override
		void fail(int trackingIndex) {
			StaticMapFailureTracker.mapFailure(trackingIndex);
			throw new FlinkRuntimeException("BAGA-BOOM!!! The user function generated test failure.");
		}
	}

	private static class RandomTaskExecutorFailureStrategy extends AbstractRandomFailureStrategy {
		private static final long serialVersionUID = 1L;

		private RandomTaskExecutorFailureStrategy(CoinToss coin) {
			super(coin);
		}

		@Override
		void fail(int trackingIndex) {
			StaticMapFailureTracker.mapFailureWithBacktracking(trackingIndex);
			try {
				restartTaskManager();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				ExceptionUtils.rethrow(e);
			}
		}
	}

	private abstract static class AbstractRandomFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final CoinToss coin;

		private AbstractRandomFailureStrategy(CoinToss coin) {
			this.coin = coin;
		}

		@Override
		public boolean failOrNot(int trackingIndex) {
			boolean generateFailure = coin.toss() && StaticFailureCounter.failOrNot(trackingIndex);
			if (generateFailure) {
				fail(trackingIndex);
			}
			return generateFailure;
		}

		abstract void fail(int trackingIndex);
	}

	private static class CoinToss implements Serializable {
		private static final long serialVersionUID = 1L;
		private static final Random rnd = new Random();

		private final int probFraction;
		private final int probBase;

		private CoinToss(int probFraction, int probBase) {
			this.probFraction = probFraction;
			this.probBase = probBase;
		}

		private boolean toss() {
			int prob = rnd.nextInt(probBase) + 1;
			return prob <= probFraction;
		}
	}

	private enum StaticFailureCounter {
		;

		private static final Map<Integer, AtomicInteger> failureNumber = new HashMap<>(MAP_NUMBER);

		private static boolean failOrNot(int trackingIndex) {
			return failureNumber.get(trackingIndex).incrementAndGet() < MAX_MAP_FAILURES;
		}

		private static void reset() {
			IntStream.range(0, MAP_NUMBER)
				.forEach(trackingIndex -> failureNumber.put(trackingIndex, new AtomicInteger(0)));
		}
	}

	private enum StaticMapFailureTracker {
		;

		private static final List<AtomicInteger> mapRestarts = new ArrayList<>(MAP_NUMBER);
		private static final List<AtomicInteger> expectedMapRestarts = new ArrayList<>(MAP_NUMBER);
		private static final List<AtomicInteger> mapFailures = new ArrayList<>(MAP_NUMBER);
		private static final List<AtomicInteger> mapFailuresWithBacktracking = new ArrayList<>(MAP_NUMBER);

		private static void reset() {
			mapRestarts.clear();
			expectedMapRestarts.clear();
			mapFailures.clear();
			mapFailuresWithBacktracking.clear();
		}

		private static int addNewMap() {
			mapRestarts.add(new AtomicInteger(0));
			expectedMapRestarts.add(new AtomicInteger(1));
			mapFailures.add(new AtomicInteger(0));
			mapFailuresWithBacktracking.add(new AtomicInteger(0));
			return mapRestarts.size() - 1;
		}

		private static void mapRestart(int index) {
			mapRestarts.get(index).incrementAndGet();
		}

		private static void mapFailure(int index) {
			mapFailures.get(index).incrementAndGet();
			expectedMapRestarts.get(index).incrementAndGet();
		}

		private static void mapFailureWithBacktracking(int index) {
			mapFailuresWithBacktracking.get(index).incrementAndGet();
			IntStream.range(0, index + 1).forEach(i -> expectedMapRestarts.get(i).incrementAndGet());
			IntStream.range(0, index + 1).forEach(i -> expectedMapRestarts.get(i).incrementAndGet());
		}

		private static void verify() {
			printStats();
			assertThat(collect(mapRestarts), is(collect(expectedMapRestarts)));
		}

		private static int[] collect(Collection<AtomicInteger> list) {
			return list.stream().mapToInt(AtomicInteger::get).toArray();
		}

		private static void printStats() {
			LOG.info(
				"Test stats - mapRestarts: {}; expectedMapRestarts: {}; mapFailures: {}; mapFailuresWithBacktracking: {}",
				mapRestarts,
				expectedMapRestarts,
				mapFailures,
				mapFailuresWithBacktracking);
		}
	}

	private static class TestPartitionMapper extends RichMapPartitionFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		private final int trackingIndex;
		private final FailureStrategy failureStrategy;
		private transient boolean failed;

		private TestPartitionMapper(int trackingIndex, FailureStrategy failureStrategy) {
			this.trackingIndex = trackingIndex;
			this.failureStrategy = failureStrategy;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			failed = false;
			StaticMapFailureTracker.mapRestart(trackingIndex);
		}

		@Override
		public void mapPartition(Iterable<Long> values, Collector<Long> out) {
			values.forEach(value -> {
				// suppress subsequent failure generation
				if (!failed) {
					failed = failureStrategy.failOrNot(trackingIndex);
				}
				out.collect(value + 1);
			});
		}
	}
}
