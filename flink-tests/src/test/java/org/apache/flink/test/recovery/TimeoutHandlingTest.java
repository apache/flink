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

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.scala.util.FailingRespondUpdatePartitionInfoTaskManager;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.forkjoin.ForkJoinPool;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class TimeoutHandlingTest extends TestLogger {

	private static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig());
	}

	@AfterClass
	public static void teardown() {
		if (system != null) {
			JavaTestKit.shutdownActorSystem(system);
			system = null;
		}
	}

	/**
	 * Tests that no illegal state transition occurs when an Execution is in a terminal state
	 * and gets again failed by a timeout of the UpdatePartitionInfo message. In order to provoke
	 * this situation, 3 job vertices (sending -> receiving -> finishing) are created. Additionally,
	 * a special TaskManager is instantiated which loses the third UpdatePartitionInfo message
	 * (2nd UPI message to receiving task). The receiving task will nevertheless switch to FINISHED.
	 * Before the timeout occurs, the finishing task will throw an exception to fail the job. After
	 * the job is restarted, the timeout occurs and the timeout handler will fail the old execution.
	 * Since the Execution is already in state FINISHED, this failing should have no effect.
	 *
	 * @throws Exception
	 */
	@Test
	public void testTerminalStateAndTimeoutBehaviour() throws Exception {
		Configuration config = new Configuration();

		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 3);
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "5000 millis");

		final ErrorReporter reporter = new ErrorReporter();
		final ExecutionContext executionContext = ExecutionContext$.MODULE$.fromExecutor(new ForkJoinPool(), reporter);

		ActorGateway jm = TestingUtils.createJobManager(
			system,
			config,
			executionContext);

		ActorGateway tm = TestingUtils.createTaskManager(
			system,
			jm,
			config,
			true,
			true,
			FailingRespondUpdatePartitionInfoTaskManager.class);

		JobGraph jobGraph = generateFailingJobGraph(1);
		jobGraph.setNumberOfExecutionRetries(1);
		jobGraph.setExecutionRetryDelay(100);

		TestingUtils.submitJobAndWait(
			system,
			jm,
			jobGraph);

		Assert.assertNull(reporter.getError());

		TestingUtils.stopActor(jm);
		TestingUtils.stopActor(tm);
	}

	public static JobGraph generateFailingJobGraph(int parallelism) {
		JobVertex sendingVertex = new JobVertex("SendingVertex");
		sendingVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		sendingVertex.setParallelism(parallelism);

		JobVertex receivingVertex = new JobVertex("ReceivingVertex");
		receivingVertex.setInvokableClass(Tasks.AgnosticReceiver.class);
		receivingVertex.setParallelism(parallelism);
		receivingVertex.connectNewDataSetAsInput(sendingVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED, true);

		JobVertex finishingVertex = new JobVertex("FinishingVertex");
		finishingVertex.setInvokableClass(FailingInvokable.class);
		finishingVertex.setParallelism(parallelism);
		finishingVertex.connectNewDataSetAsInput(receivingVertex, DistributionPattern.POINTWISE);

		JobGraph jobGraph = new JobGraph("Test job", sendingVertex, receivingVertex, finishingVertex);

		return jobGraph;
	}

	public static class FailingInvokable extends AbstractInvokable {

		static int counter = 0;

		@Override
		public void invoke() throws Exception {
			counter += 1;

			if (counter <= 1) {
				throw new Exception("Expected test exception");
			} else {
				Thread.sleep(7000);
			}
		}
	}

	public static class ErrorReporter extends AbstractFunction1<Throwable,BoxedUnit> {
		static Throwable error = null;

		@Override
		public BoxedUnit apply(Throwable v1) {
			error = v1;

			return null;
		}

		public static Throwable getError() {
			return error;
		}
	}
}
