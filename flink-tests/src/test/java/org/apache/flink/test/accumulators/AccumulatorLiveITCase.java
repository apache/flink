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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Status;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


/**
 * Tests the availability of accumulator results during runtime. The test case tests a user-defined
 * accumulator and Flink's internal accumulators for two consecutive tasks.
 */
public class AccumulatorLiveITCase {

	private static final Logger LOG = LoggerFactory.getLogger(AccumulatorLiveITCase.class);

	private static ActorSystem system;
	private static ActorGateway jobManagerGateway;
	private static ActorRef taskManager;
	private static JobID jobID;

	// name of user accumulator
	private static String NAME = "test";

	// number of heartbeat intervals to check
	private static final int NUM_ITERATIONS = 5;

	private static List<String> inputData = new ArrayList<String>(NUM_ITERATIONS);

	private static final FiniteDuration TIMEOUT = new FiniteDuration(10, TimeUnit.SECONDS);


	@Before
	public void before() throws Exception {
		system = AkkaUtils.createLocalActorSystem(new Configuration());

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1);
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		TestingCluster testingCluster = new TestingCluster(config, false, true);

		jobManagerGateway = testingCluster.getJobManagerGateway();
		taskManager = testingCluster.getTaskManagersAsJava().get(0);

		// generate test data
		for (int i=0; i < NUM_ITERATIONS; i++) {
			inputData.add(i, String.valueOf(i+1));
		}
	}

	@After
	public void after() throws Exception {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testProgram() throws Exception {

		new JavaTestKit(system) {{

			/** The program **/
			ExecutionEnvironment env = new PlanExtractor();
			DataSet<String> input = env.fromCollection(inputData);
			input
					.flatMap(new WaitingUDF())
					.output(new WaitingOutputFormat());
			env.execute();

			// Extract job graph and set job id for the task to notify of accumulator changes.
			JobGraph jobGraph = getOptimizedPlan(((PlanExtractor) env).plan);
			jobID = jobGraph.getJobID();

			ActorGateway selfGateway = new AkkaActorGateway(getRef(), jobManagerGateway.leaderSessionID());

			// register for accumulator changes
			jobManagerGateway.tell(new TestingJobManagerMessages.NotifyWhenAccumulatorChange(jobID), selfGateway);
			expectMsgEquals(TIMEOUT, true);

			// submit job

			jobManagerGateway.tell(new JobManagerMessages.SubmitJob(jobGraph, false), selfGateway);
			expectMsgClass(TIMEOUT, Status.Success.class);


			ExecutionAttemptID mapperTaskID = null;

			TestingJobManagerMessages.UpdatedAccumulators msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);
			Map<ExecutionAttemptID, Map<AccumulatorRegistry.Metric, Accumulator<?, ?>>> flinkAccumulators = msg.flinkAccumulators();
			Map<String, Accumulator<?, ?>> userAccumulators = msg.userAccumulators();

			// find out the first task's execution attempt id
			for (Map.Entry<ExecutionAttemptID, ?> entry : flinkAccumulators.entrySet()) {
				if (entry.getValue() != null) {
					mapperTaskID = entry.getKey();
					break;
				}
			}

			/* Check for accumulator values */
			if(checkUserAccumulators(0, userAccumulators) && checkFlinkAccumulators(mapperTaskID, 0, 0, 0, 0, flinkAccumulators)) {
				LOG.info("Passed initial check for map task.");
			} else {
				fail("Wrong accumulator results when map task begins execution.");
			}


			int expectedAccVal = 0;
			ExecutionAttemptID sinkTaskID = null;

			/* for mapper task */
			for (int i = 1; i <= NUM_ITERATIONS; i++) {
				expectedAccVal += i;

				// receive message
				msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);
				flinkAccumulators = msg.flinkAccumulators();
				userAccumulators = msg.userAccumulators();

				LOG.info("{}", flinkAccumulators);
				LOG.info("{}", userAccumulators);

				if (checkUserAccumulators(expectedAccVal, userAccumulators) && checkFlinkAccumulators(mapperTaskID, 0, i, 0, i * 4, flinkAccumulators)) {
					LOG.info("Passed round " + i);
				} else {
					fail("Failed in round #" + i);
				}
			}

			msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);
			flinkAccumulators = msg.flinkAccumulators();
			userAccumulators = msg.userAccumulators();

			// find the second's task id
			for (ExecutionAttemptID key : flinkAccumulators.keySet()) {
				if (key != mapperTaskID) {
					sinkTaskID = key;
					break;
				}
			}

			if(checkUserAccumulators(expectedAccVal, userAccumulators) && checkFlinkAccumulators(sinkTaskID, 0, 0, 0, 0, flinkAccumulators)) {
				LOG.info("Passed initial check for sink task.");
			} else {
				fail("Wrong accumulator results when sink task begins execution.");
			}

			/* for sink task */
			for (int i = 1; i <= NUM_ITERATIONS; i++) {

				// receive message
				msg = (TestingJobManagerMessages.UpdatedAccumulators) receiveOne(TIMEOUT);

				flinkAccumulators = msg.flinkAccumulators();
				userAccumulators = msg.userAccumulators();

				LOG.info("{}", flinkAccumulators);
				LOG.info("{}", userAccumulators);

				if (checkUserAccumulators(expectedAccVal, userAccumulators) && checkFlinkAccumulators(sinkTaskID, i, 0, i*4, 0, flinkAccumulators)) {
					LOG.info("Passed round " + i);
				} else {
					fail("Failed in round #" + i);
				}
			}

			expectMsgClass(TIMEOUT, JobManagerMessages.JobResultSuccess.class);

		}};
	}

	private static boolean checkUserAccumulators(int expected, Map<String, Accumulator<?,?>> accumulatorMap) {
		LOG.info("checking user accumulators");
		return accumulatorMap.containsKey(NAME) && expected == ((IntCounter)accumulatorMap.get(NAME)).getLocalValue();
	}

	private static boolean checkFlinkAccumulators(ExecutionAttemptID taskKey, int expectedRecordsIn, int expectedRecordsOut, int expectedBytesIn, int expectedBytesOut,
												  Map<ExecutionAttemptID, Map<AccumulatorRegistry.Metric, Accumulator<?,?>>> accumulatorMap) {
		LOG.info("checking flink accumulators");

		Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> taskMap = accumulatorMap.get(taskKey);
		assertTrue(accumulatorMap.size() > 0);

		for (Map.Entry<AccumulatorRegistry.Metric, Accumulator<?, ?>> entry : taskMap.entrySet()) {
			switch (entry.getKey()) {
				/**
				 * The following two cases are for the DataSource and Map task
				 */
				case NUM_RECORDS_OUT:
					if(((LongCounter) entry.getValue()).getLocalValue() != expectedRecordsOut) {
						return false;
					}
					break;
				case NUM_BYTES_OUT:
					if (((LongCounter) entry.getValue()).getLocalValue() != expectedBytesOut) {
						return false;
					}
					break;
				/**
				 * The following two cases are for the DataSink task
				 */
				case NUM_RECORDS_IN:
					if (((LongCounter) entry.getValue()).getLocalValue() != expectedRecordsIn) {
						return false;
					}
					break;
				case NUM_BYTES_IN:
					if (((LongCounter) entry.getValue()).getLocalValue() != expectedBytesIn) {
						return false;
					}
					break;
				default:
					fail("Unknown accumulator found.");
			}
		}
		return true;
	}


	/**
	 * UDF that waits for at least the heartbeat interval's duration.
	 */
	private static class WaitingUDF extends RichFlatMapFunction<String, Integer> {

		private IntCounter counter = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(NAME, counter);
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

	}

	private static class WaitingOutputFormat implements OutputFormat<Integer> {

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
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
			Timeout timeout = new Timeout(Duration.create(5, "seconds"));
			Future<Object> ask = Patterns.ask(taskManager, new TestingTaskManagerMessages.AccumulatorsChanged(jobID), timeout);
			try {
				Await.result(ask, timeout.duration());
			} catch (Exception e) {
				fail("Failed to notify task manager of accumulator update.");
			}
		}};
	}

	/**
	 * Helpers to generate the JobGraph
	 */
	private static JobGraph getOptimizedPlan(Plan plan) {
		Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
		JobGraphGenerator jgg = new JobGraphGenerator();
		OptimizedPlan op = pc.compile(plan);
		return jgg.compileJobGraph(op);
	}

	private static class PlanExtractor extends LocalEnvironment {

		private Plan plan = null;

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			plan = createProgramPlan();
			return new JobExecutionResult(new JobID(), -1, null);
		}

	}
}
