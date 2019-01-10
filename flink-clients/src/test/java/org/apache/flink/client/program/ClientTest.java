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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.DetachedEnvironment.DetachedJobExecutionResult;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.URL;
import java.util.Collections;
import java.util.UUID;

import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Simple and maybe stupid test to check the {@link ClusterClient} class.
 */
public class ClientTest extends TestLogger {

	private PackagedProgram program;

	private Configuration config;

	private ActorSystem jobManagerSystem;

	private static final String ACCUMULATOR_NAME = "test_accumulator";

	private static final String FAIL_MESSAGE = "Invalid program should have thrown ProgramInvocationException.";

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
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setInteger(JobManagerOptions.PORT, freePort);
		config.setString(AkkaOptions.ASK_TIMEOUT, AkkaOptions.ASK_TIMEOUT.defaultValue());

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
				jobManagerSystem.terminate();
				Await.ready(jobManagerSystem.whenTerminated(), Duration.Inf());
			} catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	/**
	 * Tests that invalid detached mode programs fail.
	 */
	@Test
	public void testDetachedMode() throws Exception{
		jobManagerSystem.actorOf(
			Props.create(SuccessReturningActor.class),
			JobMaster.JOB_MANAGER_NAME);
		StandaloneClusterClient out = new StandaloneClusterClient(config);
		out.setDetached(true);

		try {
			PackagedProgram prg = new PackagedProgram(TestExecuteTwice.class);
			out.run(prg, 1);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.EXECUTE_TWICE_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = new PackagedProgram(TestEager.class);
			out.run(prg, 1);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE + DetachedJobExecutionResult.EAGER_FUNCTION_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = new PackagedProgram(TestGetRuntime.class);
			out.run(prg, 1);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = new PackagedProgram(TestGetJobID.class);
			out.run(prg, 1);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = new PackagedProgram(TestGetAccumulator.class);
			out.run(prg, 1);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE + DetachedJobExecutionResult.EAGER_FUNCTION_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = new PackagedProgram(TestGetAllAccumulator.class);
			out.run(prg, 1);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE,
					e.getCause().getMessage());
		}
	}

	/**
	 * This test verifies correct job submission messaging logic and plan translation calls.
	 */
	@Test
	public void shouldSubmitToJobClient() throws Exception {
		jobManagerSystem.actorOf(
			Props.create(SuccessReturningActor.class),
			JobMaster.JOB_MANAGER_NAME);

		StandaloneClusterClient out = new StandaloneClusterClient(config);
		out.setDetached(true);
		JobSubmissionResult result = out.run(program.getPlanWithJars(), 1);

		assertNotNull(result);

		program.deleteExtractedLibraries();
	}

	/**
	 * This test verifies correct that the correct exception is thrown when the job submission fails.
	 */
	@Test
	public void shouldSubmitToJobClientFails() throws Exception {
			jobManagerSystem.actorOf(
				Props.create(FailureReturningActor.class),
				JobMaster.JOB_MANAGER_NAME);

		StandaloneClusterClient out = new StandaloneClusterClient(config);
		out.setDetached(true);

		try {
			out.run(program.getPlanWithJars(), 1);
			fail("This should fail with an exception");
		}
		catch (ProgramInvocationException e) {
			// bam!
		}
		catch (Exception e) {
			fail("wrong exception " + e);
		}
	}

	/**
	 * This test verifies that the local execution environment cannot be created when
	 * the program is submitted through a client.
	 */
	@Test
	public void tryLocalExecution() {
		try {
			jobManagerSystem.actorOf(
				Props.create(SuccessReturningActor.class),
				JobMaster.JOB_MANAGER_NAME);

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
				StandaloneClusterClient client = new StandaloneClusterClient(config);
				client.setDetached(true);
				client.run(packagedProgramMock, 1);
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
			jobManagerSystem.actorOf(
				Props.create(FailureReturningActor.class),
				JobMaster.JOB_MANAGER_NAME);

			PackagedProgram prg = new PackagedProgram(TestOptimizerPlan.class, "/dev/random", "/tmp");
			assertNotNull(prg.getPreviewPlan());

			Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), config);
			OptimizedPlan op = (OptimizedPlan) ClusterClient.getOptimizedPlan(optimizer, prg, 1);
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

	private static class SuccessReturningActor extends FlinkUntypedActor {

		private UUID leaderSessionID = HighAvailabilityServices.DEFAULT_LEADER_ID;

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
			} else if (message instanceof JobManagerMessages.RequestBlobManagerPort$) {
				getSender().tell(1337, getSelf());
			} else {
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

	private static class FailureReturningActor extends FlinkUntypedActor {

		private UUID leaderSessionID = HighAvailabilityServices.DEFAULT_LEADER_ID;

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

	/**
	 * A test job.
	 */
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
					new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
						public Tuple2<Long, Long> map(Tuple2<Long, Long> value){
							return new Tuple2<Long, Long>(value.f0, value.f1 + 1);
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

	/**
	 * Test job that calls {@link ExecutionEnvironment#execute()} twice.
	 */
	public static final class TestExecuteTwice {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).output(new DiscardingOutputFormat<Integer>());
			env.execute();
			env.fromElements(1, 2).collect();
		}
	}

	/**
	 * Test job that uses an eager sink.
	 */
	public static final class TestEager {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).collect();
		}
	}

	/**
	 * Test job that retrieves the net runtime from the {@link JobExecutionResult}.
	 */
	public static final class TestGetRuntime {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).output(new DiscardingOutputFormat<Integer>());
			env.execute().getNetRuntime();
		}
	}

	/**
	 * Test job that retrieves the job ID from the {@link JobExecutionResult}.
	 */
	public static final class TestGetJobID {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).output(new DiscardingOutputFormat<Integer>());
			env.execute().getJobID();
		}
	}

	/**
	 * Test job that retrieves an accumulator from the {@link JobExecutionResult}.
	 */
	public static final class TestGetAccumulator {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).output(new DiscardingOutputFormat<Integer>());
			env.execute().getAccumulatorResult(ACCUMULATOR_NAME);
		}
	}

	/**
	 * Test job that retrieves all accumulators from the {@link JobExecutionResult}.
	 */
	public static final class TestGetAllAccumulator {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).output(new DiscardingOutputFormat<Integer>());
			env.execute().getAllAccumulatorResults();
		}
	}
}
