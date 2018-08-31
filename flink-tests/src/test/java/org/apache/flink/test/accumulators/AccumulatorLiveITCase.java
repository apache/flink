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

package org.apache.flink.test.accumulators;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tests the availability of accumulator results during runtime.
 */
public class AccumulatorLiveITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(AccumulatorLiveITCase.class);

	// name of user accumulator
	private static final String ACCUMULATOR_NAME = "test";

	private static final long HEARTBEAT_INTERVAL = 50L;

	// number of heartbeat intervals to check
	private static final int NUM_ITERATIONS = 5;

	private static final List<Integer> inputData = new ArrayList<>(NUM_ITERATIONS);

	static {
		// generate test data
		for (int i = 0; i < NUM_ITERATIONS; i++) {
			inputData.add(i);
		}
	}

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(1)
			.build());

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setString(AkkaOptions.ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		config.setLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);

		return config;
	}

	@Before
	public void resetLatches() throws InterruptedException {
		NotifyingMapper.reset();
	}

	@Test
	public void testBatch() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataSet<Integer> input = env.fromCollection(inputData);
		input
				.flatMap(new NotifyingMapper())
				.output(new DummyOutputFormat());

		// Extract job graph and set job id for the task to notify of accumulator changes.
		JobGraph jobGraph = getJobGraph(env.createProgramPlan());

		submitJobAndVerifyResults(jobGraph);
	}

	@Test
	public void testStreaming() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Integer> input = env.fromCollection(inputData);
		input
				.flatMap(new NotifyingMapper())
				.writeUsingOutputFormat(new DummyOutputFormat()).disableChaining();

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		submitJobAndVerifyResults(jobGraph);
	}

	private static void submitJobAndVerifyResults(JobGraph jobGraph) throws Exception {
		Deadline deadline = Deadline.now().plus(Duration.ofSeconds(30));

		final ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();

		final CheckedThread submissionThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				client.submitJob(jobGraph, AccumulatorLiveITCase.class.getClassLoader());
			}
		};

		submissionThread.start();

		try {
			NotifyingMapper.notifyLatch.await();

			FutureUtils.retrySuccesfulWithDelay(
				() -> {
					try {
						return CompletableFuture.completedFuture(client.getAccumulators(jobGraph.getJobID()));
					} catch (Exception e) {
						return FutureUtils.completedExceptionally(e);
					}
				},
				Time.milliseconds(20),
				deadline,
				accumulators -> accumulators.size() == 1
					&& accumulators.containsKey(ACCUMULATOR_NAME)
					&& (int) accumulators.get(ACCUMULATOR_NAME).getUnchecked() == NUM_ITERATIONS,
				TestingUtils.defaultScheduledExecutor()
			).get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			NotifyingMapper.shutdownLatch.trigger();
		} finally {
			NotifyingMapper.shutdownLatch.trigger();

			// wait for the job to have terminated
			submissionThread.sync();
		}
	}

	/**
	 * UDF that notifies when it changes the accumulator values.
	 */
	private static class NotifyingMapper extends RichFlatMapFunction<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		private static final OneShotLatch notifyLatch = new OneShotLatch();
		private static final OneShotLatch shutdownLatch = new OneShotLatch();

		private final IntCounter counter = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, counter);
		}

		@Override
		public void flatMap(Integer value, Collector<Integer> out) throws Exception {
			counter.add(1);
			out.collect(value);
			LOG.debug("Emitting value {}.", value);
			if (counter.getLocalValuePrimitive() == 5) {
				notifyLatch.trigger();
			}
		}

		@Override
		public void close() throws Exception {
			shutdownLatch.await();
		}

		private static void reset() throws InterruptedException {
			notifyLatch.reset();
			shutdownLatch.reset();
		}
	}

	/**
	 * Outputs format which waits for the previous mapper.
	 */
	private static class DummyOutputFormat implements OutputFormat<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
		}

		@Override
		public void writeRecord(Integer record) throws IOException {
		}

		@Override
		public void close() throws IOException {
		}
	}

	/**
	 * Helpers to generate the JobGraph.
	 */
	private static JobGraph getJobGraph(Plan plan) {
		Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
		JobGraphGenerator jgg = new JobGraphGenerator();
		OptimizedPlan op = pc.compile(plan);
		return jgg.compileJobGraph(op);
	}
}
