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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.fail;

/**
 * Tests the availability of accumulator results during runtime. The test case tests a user-defined
 * accumulator and Flink's internal accumulators for two consecutive tasks.
 *
 * <p>CHAINED[Source -> Map] -> Sink
 *
 * <p>Checks are performed as the elements arrive at the operators. Checks consist of a message sent by
 * the task to the task manager which notifies the job manager and sends the current accumulators.
 * The task blocks until the test has been notified about the current accumulator values.
 *
 * <p>A barrier between the operators ensures that that pipelining is disabled for the streaming test.
 * The batch job reads the records one at a time. The streaming code buffers the records beforehand;
 * that's why exact guarantees about the number of records read are very hard to make. Thus, why we
 * check for an upper bound of the elements read.
 */
public class LegacyAccumulatorLiveITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(LegacyAccumulatorLiveITCase.class);

	private static ActorSystem system;
	private static ActorGateway jobManagerGateway;
	private static ActorRef taskManager;

	private static JobID jobID;
	private static JobGraph jobGraph;

	// name of user accumulator
	private static final String ACCUMULATOR_NAME = "test";

	// number of heartbeat intervals to check
	private static final int NUM_ITERATIONS = 5;

	private static List<String> inputData = new ArrayList<>(NUM_ITERATIONS);

	private static final FiniteDuration TIMEOUT = new FiniteDuration(10, TimeUnit.SECONDS);

	@Before
	public void before() throws Exception {
		system = AkkaUtils.createLocalActorSystem(new Configuration());

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		config.setString(AkkaOptions.ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		TestingCluster testingCluster = new TestingCluster(config, false, true);
		testingCluster.start();

		jobManagerGateway = testingCluster.getLeaderGateway(TestingUtils.TESTING_DURATION());
		taskManager = testingCluster.getTaskManagersAsJava().get(0);

		// generate test data
		for (int i = 0; i < NUM_ITERATIONS; i++) {
			inputData.add(i, String.valueOf(i + 1));
		}

		NotifyingMapper.finished = false;
	}

	@After
	public void after() throws Exception {
		JavaTestKit.shutdownActorSystem(system);
		inputData.clear();
	}

	@Test
	public void testBatch() throws Exception {

		/** The program **/
		ExecutionEnvironment env = new BatchPlanExtractor();
		env.setParallelism(1);

		DataSet<String> input = env.fromCollection(inputData);
		input
				.flatMap(new NotifyingMapper())
				.output(new NotifyingOutputFormat());

		env.execute();

		// Extract job graph and set job id for the task to notify of accumulator changes.
		jobGraph = getOptimizedPlan(((BatchPlanExtractor) env).plan);
		jobID = jobGraph.getJobID();

		verifyResults();
	}

	@Test
	public void testStreaming() throws Exception {

		StreamExecutionEnvironment env = new DummyStreamExecutionEnvironment();
		env.setParallelism(1);

		DataStream<String> input = env.fromCollection(inputData);
		input
				.flatMap(new NotifyingMapper())
				.writeUsingOutputFormat(new NotifyingOutputFormat()).disableChaining();

		jobGraph = env.getStreamGraph().getJobGraph();
		jobID = jobGraph.getJobID();

		verifyResults();
	}

	private static void verifyResults() {
		new JavaTestKit(system) {{

			ActorGateway selfGateway = new AkkaActorGateway(getRef(), jobManagerGateway.leaderSessionID());

			// register for accumulator changes
			jobManagerGateway.tell(new TestingJobManagerMessages.NotifyWhenAccumulatorChange(jobID), selfGateway);
			expectMsgEquals(TIMEOUT, true);

			// submit job

			jobManagerGateway.tell(
					new JobManagerMessages.SubmitJob(
							jobGraph,
							ListeningBehaviour.EXECUTION_RESULT),
					selfGateway);
			expectMsgClass(TIMEOUT, JobManagerMessages.JobSubmitSuccess.class);

			TestingJobManagerMessages.UpdatedAccumulators msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);
			Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = msg.userAccumulators();

			ExecutionAttemptID mapperTaskID = null;

			ExecutionAttemptID sinkTaskID = null;

			/* Check for accumulator values */
			if (checkUserAccumulators(0, userAccumulators)) {
				LOG.info("Passed initial check for map task.");
			} else {
				fail("Wrong accumulator results when map task begins execution.");
			}

			int expectedAccVal = 0;

			/* for mapper task */
			for (int i = 1; i <= NUM_ITERATIONS; i++) {
				expectedAccVal += i;

				// receive message
				msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);
				userAccumulators = msg.userAccumulators();

				LOG.info("{}", userAccumulators);

				if (checkUserAccumulators(expectedAccVal, userAccumulators)) {
					LOG.info("Passed round #" + i);
				} else if (checkUserAccumulators(expectedAccVal, userAccumulators)) {
					// we determined the wrong task id and need to switch the two here
					ExecutionAttemptID temp = mapperTaskID;
					mapperTaskID = sinkTaskID;
					sinkTaskID = temp;
					LOG.info("Passed round #" + i);
				} else {
					fail("Failed in round #" + i);
				}
			}

			msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);
			userAccumulators = msg.userAccumulators();

			if (checkUserAccumulators(expectedAccVal, userAccumulators)) {
				LOG.info("Passed initial check for sink task.");
			} else {
				fail("Wrong accumulator results when sink task begins execution.");
			}

			/* for sink task */
			for (int i = 1; i <= NUM_ITERATIONS; i++) {

				// receive message
				msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);

				userAccumulators = msg.userAccumulators();

				LOG.info("{}", userAccumulators);

				if (checkUserAccumulators(expectedAccVal, userAccumulators)) {
					LOG.info("Passed round #" + i);
				} else {
					fail("Failed in round #" + i);
				}
			}

			expectMsgClass(TIMEOUT, JobManagerMessages.JobResultSuccess.class);

		}};
	}

	private static boolean checkUserAccumulators(int expected, Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap) {
		LOG.info("checking user accumulators");
		return accumulatorMap.containsKey(ACCUMULATOR_NAME) && expected == ((IntCounter) accumulatorMap.get(ACCUMULATOR_NAME).getUnchecked()).getLocalValue();
	}

	/**
	 * UDF that notifies when it changes the accumulator values.
	 */
	private static class NotifyingMapper extends RichFlatMapFunction<String, Integer> {
		private static final long serialVersionUID = 1L;

		private IntCounter counter = new IntCounter();

		private static boolean finished = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, counter);
			notifyTaskManagerOfAccumulatorUpdate();
		}

		@Override
		public void flatMap(String value, Collector<Integer> out) throws Exception {
			int val = Integer.valueOf(value);
			counter.add(val);
			out.collect(val);
			LOG.debug("Emitting value {}.", value);
			notifyTaskManagerOfAccumulatorUpdate();
		}

		@Override
		public void close() throws Exception {
			finished = true;
		}
	}

	/**
	 * Outputs format which notifies of accumulator changes and waits for the previous mapper.
	 */
	private static class NotifyingOutputFormat implements OutputFormat<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			while (!NotifyingMapper.finished) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}
			notifyTaskManagerOfAccumulatorUpdate();
		}

		@Override
		public void writeRecord(Integer record) throws IOException {
			notifyTaskManagerOfAccumulatorUpdate();
		}

		@Override
		public void close() throws IOException {
		}
	}

	/**
	 * Notify task manager of accumulator update and wait until the Heartbeat containing the message
	 * has been reported.
	 */
	public static void notifyTaskManagerOfAccumulatorUpdate() {
		new JavaTestKit(system) {{
			Timeout timeout = new Timeout(TIMEOUT);
			Future<Object> ask = Patterns.ask(taskManager, new TestingTaskManagerMessages.AccumulatorsChanged(jobID), timeout);
			try {
				Await.result(ask, timeout.duration());
			} catch (Exception e) {
				fail("Failed to notify task manager of accumulator update.");
			}
		}};
	}

	/**
	 * Helpers to generate the JobGraph.
	 */
	private static JobGraph getOptimizedPlan(Plan plan) {
		Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
		JobGraphGenerator jgg = new JobGraphGenerator();
		OptimizedPlan op = pc.compile(plan);
		return jgg.compileJobGraph(op);
	}

	private static class BatchPlanExtractor extends LocalEnvironment {

		private Plan plan = null;

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			plan = createProgramPlan();
			return new JobExecutionResult(new JobID(), -1, null);
		}
	}

	/**
	 * This is used to for creating the example topology. {@link #execute} is never called, we
	 * only use this to call {@link #getStreamGraph()}.
	 */
	private static class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

		@Override
		public JobExecutionResult execute() throws Exception {
			return execute("default");
		}

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			throw new RuntimeException("This should not be called.");
		}
	}
}
