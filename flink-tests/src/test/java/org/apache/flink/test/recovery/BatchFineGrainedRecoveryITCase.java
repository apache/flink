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
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration.Builder;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
 * <p>The test verifies that fine-grained recovery works by randomly incuding failures in any of the mappers.
 * Since all mappers are connected via blocking partitions, which should be re-used on failure, and the consumer
 * of the mapper wasn't deployed yet, as the consumed partition was not fully produced yet, only the failed mapper
 * should actually restart.
 */
public class BatchFineGrainedRecoveryITCase extends TestLogger {
	private static final int EMITTED_RECORD_NUMBER = 1000;
	private static final int MAX_FAILURE_NUMBER = 10;
	private static final int MAP_NUMBER = 3;
	private static final List<Long> EXPECTED_JOB_OUTPUT =
		LongStream.range(MAP_NUMBER, EMITTED_RECORD_NUMBER + MAP_NUMBER).boxed().collect(Collectors.toList());

	private TestingMiniCluster miniCluster;

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setBoolean(FORCE_PARTITION_RELEASE_ON_CONSUMPTION, false);
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, PIPELINED_REGION_RESTART_STRATEGY_NAME);

		miniCluster = new TestingMiniCluster(
			new Builder()
				.setNumTaskManagers(MAP_NUMBER)
				.setNumSlotsPerTaskManager(1)
				.setConfiguration(configuration)
				.build(),
			null);

		miniCluster.start();
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

		StaticFailureCounter.reset();
		StaticMapFailureTracker.reset();

		FailureStrategy failureStrategy = new RandomExceptionFailureStrategy(1, EMITTED_RECORD_NUMBER);

		DataSet<Long> input = env.generateSequence(0, EMITTED_RECORD_NUMBER - 1);
		for (int i = 0; i < MAP_NUMBER; i++) {
			input = input
				.mapPartition(new TestPartitionMapper(StaticMapFailureTracker.addNewMap(), failureStrategy))
				.name("Test partition mapper " + i);
		}
		assertThat(input.collect(), is(EXPECTED_JOB_OUTPUT));

		StaticMapFailureTracker.verify();
	}

	private ExecutionEnvironment createExecutionEnvironment() {
		@SuppressWarnings("StaticVariableUsedBeforeInitialization")
		ExecutionEnvironment env = new TestEnvironment(miniCluster, 1, true);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(MAX_FAILURE_NUMBER, Time.milliseconds(10)));
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED); // forces all partitions to be blocking
		return env;
	}

	private enum StaticMapFailureTracker {
		;

		private static final List<AtomicInteger> mapRestarts = new ArrayList<>(10);
		private static final List<AtomicInteger> expectedMapRestarts = new ArrayList<>(10);

		private static void reset() {
			mapRestarts.clear();
			expectedMapRestarts.clear();
		}

		private static int addNewMap() {
			mapRestarts.add(new AtomicInteger(0));
			expectedMapRestarts.add(new AtomicInteger(1));
			return mapRestarts.size() - 1;
		}

		private static void mapRestart(int index) {
			mapRestarts.get(index).incrementAndGet();
		}

		private static void mapFailure(int index) {
			expectedMapRestarts.get(index).incrementAndGet();
		}

		private static void verify() {
			assertThat(collect(mapRestarts), is(collect(expectedMapRestarts)));
		}

		private static int[] collect(Collection<AtomicInteger> list) {
			return list.stream().mapToInt(AtomicInteger::get).toArray();
		}
	}

	@FunctionalInterface
	private interface FailureStrategy extends Serializable {
		void failOrNot();
	}

	private static class RandomExceptionFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final CoinToss coin;

		private RandomExceptionFailureStrategy(int probFraction, int probBase) {
			this.coin = new CoinToss(probFraction, probBase);
		}

		@Override
		public void failOrNot() {
			if (coin.toss() && StaticFailureCounter.failOrNot()) {
				throw new FlinkRuntimeException("BAGA-BOOM!!! The user function generated test failure.");
			}
		}
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

	private static class TestPartitionMapper extends RichMapPartitionFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		private final int trackingIndex;
		private final FailureStrategy failureStrategy;

		private TestPartitionMapper(int trackingIndex, FailureStrategy failureStrategy) {
			this.trackingIndex = trackingIndex;
			this.failureStrategy = failureStrategy;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			StaticMapFailureTracker.mapRestart(trackingIndex);
		}

		@Override
		public void mapPartition(Iterable<Long> values, Collector<Long> out) {
			values.forEach(value -> {
				failOrNot();
				out.collect(value + 1);
			});
		}

		private void failOrNot() {
			try {
				failureStrategy.failOrNot();
			} catch (Throwable t) {
				StaticMapFailureTracker.mapFailure(trackingIndex);
				throw t;
			}
		}
	}

	private enum StaticFailureCounter {
		;

		private static final AtomicInteger failureNumber = new AtomicInteger(0);

		private static boolean failOrNot() {
			return failureNumber.incrementAndGet() < MAX_FAILURE_NUMBER;
		}

		private static void reset() {
			failureNumber.set(0);
		}
	}
}
