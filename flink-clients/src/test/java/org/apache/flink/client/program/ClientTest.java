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
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import java.net.URL;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
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

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE =
		new MiniClusterResource(new MiniClusterResourceConfiguration.Builder().build());

	private Plan plan;

	private Configuration config;

	private static final String TEST_EXECUTOR_NAME = "test_executor";

	private static final String ACCUMULATOR_NAME = "test_accumulator";

	private static final String FAIL_MESSAGE = "Invalid program should have thrown ProgramInvocationException.";

	@Before
	public void setUp() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.generateSequence(1, 1000).output(new DiscardingOutputFormat<>());
		plan = env.createProgramPlan();

		final int freePort = NetUtils.getAvailablePort();
		config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setInteger(JobManagerOptions.PORT, freePort);
		config.setString(AkkaOptions.ASK_TIMEOUT, AkkaOptions.ASK_TIMEOUT.defaultValue());
	}

	private Configuration fromPackagedProgram(final PackagedProgram program, final int parallelism, final boolean detached) {
		final Configuration configuration = new Configuration();
		configuration.setString(DeploymentOptions.TARGET, TEST_EXECUTOR_NAME);
		configuration.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
		configuration.set(DeploymentOptions.ATTACHED, !detached);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, program.getClasspaths(), URL::toString);
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, program.getJobJarAndDependencies(), URL::toString);
		return configuration;
	}

	/**
	 * Tests that invalid detached mode programs fail.
	 */
	@Test
	public void testDetachedMode() throws Exception{
		final ClusterClient<?> clusterClient = new MiniClusterClient(new Configuration(), MINI_CLUSTER_RESOURCE.getMiniCluster());

		try {
			PackagedProgram prg = PackagedProgram.newBuilder().setEntryPointClassName(TestEager.class.getName()).build();
			final Configuration configuration = fromPackagedProgram(prg, 1, true);

			ClientUtils.executeProgram(new TestExecutorServiceLoader(clusterClient, plan), configuration, prg, false, false);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE + DetachedJobExecutionResult.EAGER_FUNCTION_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = PackagedProgram.newBuilder().setEntryPointClassName(TestGetRuntime.class.getName()).build();
			final Configuration configuration = fromPackagedProgram(prg, 1, true);

			ClientUtils.executeProgram(new TestExecutorServiceLoader(clusterClient, plan), configuration, prg, false, false);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = PackagedProgram.newBuilder().setEntryPointClassName(TestGetAccumulator.class.getName()).build();
			final Configuration configuration = fromPackagedProgram(prg, 1, true);

			ClientUtils.executeProgram(new TestExecutorServiceLoader(clusterClient, plan), configuration, prg, false, false);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE + DetachedJobExecutionResult.EAGER_FUNCTION_MESSAGE,
					e.getCause().getMessage());
		}

		try {
			PackagedProgram prg = PackagedProgram.newBuilder().setEntryPointClassName(TestGetAllAccumulator.class.getName()).build();
			final Configuration configuration = fromPackagedProgram(prg, 1, true);

			ClientUtils.executeProgram(new TestExecutorServiceLoader(clusterClient, plan), configuration, prg, false, false);
			fail(FAIL_MESSAGE);
		} catch (ProgramInvocationException e) {
			assertEquals(
					DetachedJobExecutionResult.DETACHED_MESSAGE + DetachedJobExecutionResult.JOB_RESULT_MESSAGE,
					e.getCause().getMessage());
		}
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testMultiExecuteWithEnforcingSingleJobExecution() throws Throwable {
		try {
			launchMultiExecuteJob(true);
		} catch (Exception e) {
			if (e instanceof ProgramInvocationException) {
				throw e.getCause();
			}
		}
		fail("Test should have failed due to multiple execute() calls.");
	}

	@Test
	public void testMultiExecuteWithoutEnforcingSingleJobExecution() throws ProgramInvocationException {
		launchMultiExecuteJob(false);
	}

	private void launchMultiExecuteJob(final boolean enforceSingleJobExecution) throws ProgramInvocationException {
		try (final ClusterClient<?> clusterClient =
					new MiniClusterClient(new Configuration(), MINI_CLUSTER_RESOURCE.getMiniCluster())) {

			final PackagedProgram program = PackagedProgram.newBuilder()
					.setEntryPointClassName(TestMultiExecute.class.getName())
					.build();

			final Configuration configuration = fromPackagedProgram(program, 1, false);

			ClientUtils.executeProgram(
					new TestExecutorServiceLoader(clusterClient, plan),
					configuration,
					program,
					enforceSingleJobExecution,
					false);
		}
	}

	/**
	 * This test verifies correct job submission messaging logic and plan translation calls.
	 */
	@Test
	public void shouldSubmitToJobClient() throws Exception {
		final ClusterClient<?> clusterClient = new MiniClusterClient(new Configuration(), MINI_CLUSTER_RESOURCE.getMiniCluster());
		JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(
				plan,
				new Configuration(),
				1);

		jobGraph.addJars(Collections.emptyList());
		jobGraph.setClasspaths(Collections.emptyList());

		JobSubmissionResult result = ClientUtils.submitJob(clusterClient, jobGraph);
		assertNotNull(result);
	}

	/**
	 * This test verifies that the local execution environment cannot be created when
	 * the program is submitted through a client.
	 */
	@Test
	public void tryLocalExecution() throws ProgramInvocationException, ProgramMissingJobException {
		PackagedProgram packagedProgramMock = mock(PackagedProgram.class);

		when(packagedProgramMock.getUserCodeClassLoader())
				.thenReturn(packagedProgramMock.getClass().getClassLoader());

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ExecutionEnvironment.createLocalEnvironment();
				return null;
			}
		}).when(packagedProgramMock).invokeInteractiveModeForExecution();

		try {
			final ClusterClient<?> client = new MiniClusterClient(new Configuration(), MINI_CLUSTER_RESOURCE.getMiniCluster());
			final Configuration configuration = fromPackagedProgram(packagedProgramMock, 1, true);
			ClientUtils.executeProgram(new TestExecutorServiceLoader(client, plan), configuration, packagedProgramMock, false, false);
			fail("Creating the local execution environment should not be possible");
		}
		catch (InvalidProgramException e) {
			// that is what we want
		}
	}

	@Test
	public void testGetExecutionPlan() throws ProgramInvocationException {
		PackagedProgram prg = PackagedProgram.newBuilder()
			.setEntryPointClassName(TestOptimizerPlan.class.getName())
			.setArguments("/dev/random", "/tmp")
			.build();

		Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), config);
		Plan plan = (Plan) PackagedProgramUtils.getPipelineFromProgram(prg, new Configuration(), 1, true);
		OptimizedPlan op = optimizer.compile(plan);
		assertNotNull(op);

		PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
		assertNotNull(dumper.getOptimizerPlanAsJSON(op));

		// test HTML escaping
		PlanJSONDumpGenerator dumper2 = new PlanJSONDumpGenerator();
		dumper2.setEncodeForHTML(true);
		String htmlEscaped = dumper2.getOptimizerPlanAsJSON(op);

		assertEquals(-1, htmlEscaped.indexOf('\\'));
	}

	// --------------------------------------------------------------------------------------------

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
	 * Test job that uses an eager sink.
	 */
	public static final class TestEager {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.fromElements(1, 2).collect();
		}
	}

	/**
	 * Test job with multiple execute() calls.
	 */
	public static final class TestMultiExecute {

		public static void main(String[] args) throws Exception {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			for (int i = 0; i < 2; i++) {
				env.fromElements(1, 2).output(new DiscardingOutputFormat<>());
				JobClient jc = env.executeAsync();

				jc.getJobExecutionResult(TestMultiExecute.class.getClassLoader());
			}
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

	private static final class TestExecutorServiceLoader implements PipelineExecutorServiceLoader {

		private final ClusterClient<?> clusterClient;

		private final Plan plan;

		TestExecutorServiceLoader(final ClusterClient<?> clusterClient, final Plan plan) {
			this.clusterClient = checkNotNull(clusterClient);
			this.plan = checkNotNull(plan);
		}

		@Override
		public PipelineExecutorFactory getExecutorFactory(@Nonnull Configuration configuration) {
			return new PipelineExecutorFactory() {

				@Override
				public String getName() {
					return "my-name";
				}

				@Override
				public boolean isCompatibleWith(@Nonnull Configuration configuration) {
					return TEST_EXECUTOR_NAME.equalsIgnoreCase(configuration.getString(DeploymentOptions.TARGET));
				}

				@Override
				public PipelineExecutor getExecutor(@Nonnull Configuration configuration) {
					return (pipeline, config) -> {
						final int parallelism = config.getInteger(CoreOptions.DEFAULT_PARALLELISM);
						final JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(plan, config, parallelism);

						final ExecutionConfigAccessor accessor = ExecutionConfigAccessor.fromConfiguration(config);
						jobGraph.addJars(accessor.getJars());
						jobGraph.setClasspaths(accessor.getClasspaths());

						final JobID jobID = ClientUtils.submitJob(clusterClient, jobGraph).getJobID();
						return CompletableFuture.completedFuture(new ClusterClientJobClientAdapter<>(() -> clusterClient, jobID));
					};
				}
			};
		}

		@Override
		public Stream<String> getExecutorNames() {
			throw new UnsupportedOperationException("not implemented");
		}
	}
}
