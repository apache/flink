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
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.SerializedThrowable;
import org.apache.flink.util.NetUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URL;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.*;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Simple and maybe stupid test to check the {@link Client} class.
 */
public class ClientTest {

	private PackagedProgram program;

	private Configuration config;

	private ActorSystem jobManagerSystem;


	@Before
	public void setUp() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.generateSequence(1, 1000).output(new DiscardingOutputFormat<Long>());
		
		Plan plan = env.createProgramPlan();
		JobWithJars jobWithJars = new JobWithJars(plan, Collections.<URL>emptyList(),  Collections.<URL>emptyList());

		program = mock(PackagedProgram.class);
		when(program.getPlanWithJars()).thenReturn(jobWithJars);
		
		final int freePort = NetUtils.getAvailablePort();
		config = new Configuration();
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, freePort);
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);

		try {
			scala.Tuple2<String, Object> address = new scala.Tuple2<String, Object>("localhost", freePort);
			jobManagerSystem = AkkaUtils.createActorSystem(config, new scala.Some<scala.Tuple2<String, Object>>(address));
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

			Client out = new Client(config);
			JobSubmissionResult result = out.runDetached(program.getPlanWithJars(), 1);

			assertNotNull(result);

			program.deleteExtractedLibraries();
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

			Client out = new Client(config);

			try {
				out.runDetached(program.getPlanWithJars(), 1);
				fail("This should fail with an exception");
			}
			catch (ProgramInvocationException e) {
				// bam!
			}
			catch (Exception e) {
				fail("wrong exception " + e);
			}
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
			jobManagerSystem.actorOf(Props.create(SuccessReturningActor.class), JobManager.JOB_MANAGER_NAME());
			
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
				new Client(config).runBlocking(packagedProgramMock, 1);
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

	@Test
	public void testGetExecutionPlan() {
		try {
			jobManagerSystem.actorOf(Props.create(FailureReturningActor.class), JobManager.JOB_MANAGER_NAME());
			
			PackagedProgram prg = new PackagedProgram(TestOptimizerPlan.class, "/dev/random", "/tmp");
			assertNotNull(prg.getPreviewPlan());
			
			Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), config);
			OptimizedPlan op = (OptimizedPlan) Client.getOptimizedPlan(optimizer, prg, 1);
			assertNotNull(op);

			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			assertNotNull(dumper.getOptimizerPlanAsJSON(op));

			// test HTML escaping
			PlanJSONDumpGenerator dumper2 = new PlanJSONDumpGenerator();
			dumper2.setEncodeForHTML(true);
			String htmlEscaped = dumper2.getOptimizerPlanAsJSON(op);

			assertEquals(-1, htmlEscaped.indexOf('\\'));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class SuccessReturningActor extends FlinkUntypedActor {

		private UUID leaderSessionID = null;

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
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}

	public static class FailureReturningActor extends FlinkUntypedActor {

		private UUID leaderSessionID = null;

		@Override
		public void handleMessage(Object message) {
			getSender().tell(
					decorateMessage(new JobManagerMessages.JobResultFailure(
							new SerializedThrowable(new Exception("test")))),
					getSelf());
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionID;
		}
	}
	
	public static class TestOptimizerPlan implements ProgramDescription {

		@SuppressWarnings("serial")
		public static void main(String[] args) throws Exception {
			if (args.length < 2) {
				System.err.println("Usage: TestOptimizerPlan <input-file-path> <output-file-path>");
				return;
			}

			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> input = env.readCsvFile(args[0])
					.fieldDelimiter("\t").types(Long.class, Long.class);

			DataSet<Tuple2<Long, Long>> result = input.map(
					new MapFunction<Tuple2<Long,Long>, Tuple2<Long,Long>>() {
						public Tuple2<Long, Long> map(Tuple2<Long, Long> value){
							return new Tuple2<Long, Long>(value.f0, value.f1+1);
						}
					});
			result.writeAsCsv(args[1], "\n", "\t");
			env.execute();
		}
		@Override
		public String getDescription() {
			return "TestOptimizerPlan <input-file-path> <output-file-path>";
		}
	}
}
