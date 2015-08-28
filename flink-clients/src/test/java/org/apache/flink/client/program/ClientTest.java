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

package org.apache.flink.client.program;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.runtime.util.SerializedThrowable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import scala.Option;
import scala.Some;
import scala.Tuple2;

import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Simple and maybe stupid test to check the {@link Client} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Client.class)
public class ClientTest {

	private PackagedProgram program;
	private Optimizer compilerMock;
	private JobGraphGenerator generatorMock;


	private Configuration config;

	private ActorSystem jobManagerSystem;

	private JobGraph jobGraph = new JobGraph("test graph");

	@Before
	public void setUp() throws Exception {

		final int freePort = NetUtils.getAvailablePort();
		config = new Configuration();
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, freePort);
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);

		program = mock(PackagedProgram.class);
		compilerMock = mock(Optimizer.class);
		generatorMock = mock(JobGraphGenerator.class);

		JobWithJars planWithJarsMock = mock(JobWithJars.class);
		Plan planMock = mock(Plan.class);
		OptimizedPlan optimizedPlanMock = mock(OptimizedPlan.class);

		when(planMock.getJobName()).thenReturn("MockPlan");

		when(program.getPlanWithJars()).thenReturn(planWithJarsMock);
		when(planWithJarsMock.getPlan()).thenReturn(planMock);

		whenNew(Optimizer.class).withArguments(any(DataStatistics.class), any(CostEstimator.class), any(Configuration.class)).thenReturn(this.compilerMock);
		when(compilerMock.compile(planMock)).thenReturn(optimizedPlanMock);

		whenNew(JobGraphGenerator.class).withNoArguments().thenReturn(generatorMock);
		when(generatorMock.compileJobGraph(optimizedPlanMock)).thenReturn(jobGraph);

		try {
			Tuple2<String, Object> address = new Tuple2<String, Object>("localhost", freePort);
			jobManagerSystem = AkkaUtils.createActorSystem(config, new Some<Tuple2<String, Object>>(address));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Setup of test actor system failed.");
		}
	}

	@After
	public void shutDownActorSystem() {
		if (jobManagerSystem != null) {
			try {
				jobManagerSystem.shutdown();
				jobManagerSystem.awaitTermination();
			} catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	/**
	 * This test verifies correct job submission messaging logic and plan translation calls.
	 */
	@Test
	public void shouldSubmitToJobClient() {
		try {
			jobManagerSystem.actorOf(Props.create(SuccessReturningActor.class), JobManager.JOB_MANAGER_NAME());

			Client out = new Client(config, getClass().getClassLoader());
			JobSubmissionResult result = out.run(program.getPlanWithJars(), -1, false);

			assertNotNull(result);

			program.deleteExtractedLibraries();

			verify(this.compilerMock, times(1)).compile(any(Plan.class));
			verify(this.generatorMock, times(1)).compileJobGraph(any(OptimizedPlan.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test verifies correct that the correct exception is thrown when the job submission fails.
	 */
	@Test
	public void shouldSubmitToJobClientFails() {
		try {
			jobManagerSystem.actorOf(Props.create(FailureReturningActor.class), JobManager.JOB_MANAGER_NAME());

			Client out = new Client(config, getClass().getClassLoader());

			try {
				out.run(program.getPlanWithJars(), -1, false);
				fail("This should fail with an exception");
			}
			catch (ProgramInvocationException e) {
				// bam!
			}
			catch (Exception e) {
				fail("wrong exception " + e);
			}

			verify(this.compilerMock, times(1)).compile(any(Plan.class));
			verify(this.generatorMock, times(1)).compileJobGraph(any(OptimizedPlan.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * This test verifies that the local execution environment cannot be created when
	 * the program is submitted through a client.
	 */
	@Test
	public void tryLocalExecution() {
		try {
			PackagedProgram packagedProgramMock = mock(PackagedProgram.class);

			when(packagedProgramMock.isUsingInteractiveMode()).thenReturn(true);

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					ExecutionEnvironment.createLocalEnvironment();
					return null;
				}
			}).when(packagedProgramMock).invokeInteractiveModeForExecution();

			try {
				new Client(config, getClass().getClassLoader()).run(packagedProgramMock, 1, true);
				fail("Creating the local execution environment should not be possible");
			}
			catch (InvalidProgramException e) {
				// that is what we want
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class SuccessReturningActor extends FlinkUntypedActor {

		private final Option<UUID> leaderSessionID = Option.apply(UUID.randomUUID());

		@Override
		public void handleMessage(Object message) {
			if (message instanceof JobManagerMessages.SubmitJob) {
				JobID jid = ((JobManagerMessages.SubmitJob) message).jobGraph().getJobID();
				getSender().tell(
						decorateMessage(new JobManagerMessages.JobSubmitSuccess(jid)),
						getSelf());
			}
			else if (message.getClass() == JobManagerMessages.getRequestLeaderSessionID().getClass()) {
				getSender().tell(
						decorateMessage(new JobManagerMessages.ResponseLeaderSessionID(leaderSessionID)),
						getSelf());
			}
			else {
				getSender().tell(
						decorateMessage(new Status.Failure(new Exception("Unknown message " + message))),
						getSelf());
			}
		}

		@Override
		protected Option<UUID> getLeaderSessionID() {
			return leaderSessionID;
		}
	}

	public static class FailureReturningActor extends FlinkUntypedActor {

		private Option<UUID> leaderSessionID = Option.apply(UUID.randomUUID());

		@Override
		public void handleMessage(Object message) {
			getSender().tell(
					decorateMessage(new JobManagerMessages.JobResultFailure(
							new SerializedThrowable(new Exception("test")))),
					getSelf());
		}

		@Override
		protected Option<UUID> getLeaderSessionID() {
			return leaderSessionID;
		}
	}
}
