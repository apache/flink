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

package org.apache.flink.client.deployment.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedThrowable;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ApplicationDispatcherBootstrap}.
 */
public class ApplicationDispatcherBootstrapTest {

	private static final String MULTI_EXECUTE_JOB_CLASS_NAME = "org.apache.flink.client.testjar.MultiExecuteJob";
	private static final int TIMEOUT_SECONDS = 10;

	final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
	final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

	@After
	public void cleanup() {
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
	}

	@Test
	public void testExceptionThrownWhenApplicationContainsNoJobs() throws Throwable {
		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()));

		final CompletableFuture<Void> applicationFuture =
				runApplication(dispatcherBuilder, 0, false);

		assertException(applicationFuture, ApplicationExecutionException.class);
	}

	@Test
	public void testExceptionThrownWithMultiJobApplicationIfOnlyOneJobIsAllowed() throws Throwable {
		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()));

		final CompletableFuture<Void> applicationFuture =
				runApplication(dispatcherBuilder, 3, true);

		assertException(applicationFuture, FlinkRuntimeException.class);
	}

	@Test
	public void testApplicationFailsAsSoonAsOneJobFails() throws Throwable {
		final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();

		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> {
					submittedJobIds.add(jobGraph.getJobID());
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.setRequestJobStatusFunction(jobId -> {
					// we only fail one of the jobs, the first one, the others will "keep" running
					// indefinitely
					if (jobId.equals(submittedJobIds.peek())) {
						return CompletableFuture.completedFuture(JobStatus.FAILED);
					}
					// never finish the other jobs
					return CompletableFuture.completedFuture(JobStatus.RUNNING);
				})
				.setRequestJobResultFunction(jobId -> {
					// we only fail one of the jobs, the first one, the other will "keep" running
					// indefinitely. If we didn't have this the test would hang forever.
					if (jobId.equals(submittedJobIds.peek())) {
						return CompletableFuture.completedFuture(createFailedJobResult(jobId));
					}
					// never finish the other jobs
					return new CompletableFuture<>();
				});

		final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 2, false);

		assertException(applicationFuture, JobExecutionException.class);
	}

	@Test
	public void testApplicationSucceedsWhenAllJobsSucceed() throws Exception {
		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobId -> CompletableFuture.completedFuture(JobStatus.FINISHED))
				.setRequestJobResultFunction(jobId -> CompletableFuture.completedFuture(createSuccessfulJobResult(jobId)));

		final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 3, false);

		// this would block indefinitely if the applications don't finish
		applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	@Test
	public void testApplicationTaskFinishesWhenApplicationFinishes() throws Exception {
		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobId -> CompletableFuture.completedFuture(JobStatus.FINISHED))
				.setRequestJobResultFunction(jobId -> CompletableFuture.completedFuture(createSuccessfulJobResult(jobId)))
				.setClusterShutdownSupplier(() -> CompletableFuture.completedFuture(Acknowledge.get()));

		ApplicationDispatcherBootstrap bootstrap = createApplicationDispatcherBootstrap(3);

		final CompletableFuture<Acknowledge> shutdownFuture =
				bootstrap.runApplicationAndShutdownClusterAsync(dispatcherBuilder.build(), scheduledExecutor);

		ScheduledFuture<?> applicationExecutionFuture = bootstrap.getApplicationExecutionFuture();

		// wait until the bootstrap "thinks" it's done
		shutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		// make sure the task finishes
		applicationExecutionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	@Test
	public void testApplicationIsCancelledWhenStoppingBootstrap() throws Exception {
		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobId -> CompletableFuture.completedFuture(JobStatus.RUNNING))
				.setClusterShutdownSupplier(() -> CompletableFuture.completedFuture(Acknowledge.get()));

		ApplicationDispatcherBootstrap bootstrap = createApplicationDispatcherBootstrap(3);

		final CompletableFuture<Acknowledge> shutdownFuture =
				bootstrap.runApplicationAndShutdownClusterAsync(dispatcherBuilder.build(), scheduledExecutor);

		ScheduledFuture<?> applicationExecutionFuture = bootstrap.getApplicationExecutionFuture();

		bootstrap.stop();

		// wait until the bootstrap "thinks" it's done
		shutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		// verify that the application task is being cancelled
		assertThat(applicationExecutionFuture.isCancelled(), is(true));
	}

	@Test
	public void testClusterShutdownWhenStoppingBootstrap() throws Exception {
		// we're "listening" on this to be completed to verify that the cluster
		// is being shut down from the ApplicationDispatcherBootstrap
		final CompletableFuture<Boolean> externalShutdownFuture = new CompletableFuture<>();

		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobId -> CompletableFuture.completedFuture(JobStatus.RUNNING))
				.setClusterShutdownSupplier(() -> {
					externalShutdownFuture.complete(true);
					return CompletableFuture.completedFuture(Acknowledge.get());
				});

		ApplicationDispatcherBootstrap bootstrap = createApplicationDispatcherBootstrap(2);

		final CompletableFuture<Acknowledge> shutdownFuture =
				bootstrap.runApplicationAndShutdownClusterAsync(dispatcherBuilder.build(), scheduledExecutor);

		bootstrap.stop();

		// wait until the bootstrap "thinks" it's done
		shutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		// verify that the dispatcher is actually being shut down
		assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(true));
	}

	@Test
	public void testClusterShutdownWhenSubmissionFails() throws Exception {
		// we're "listening" on this to be completed to verify that the cluster
		// is being shut down from the ApplicationDispatcherBootstrap
		final CompletableFuture<Boolean> externalShutdownFuture = new CompletableFuture<>();

		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> {
					throw new FlinkRuntimeException("Nope!");
				})
				.setClusterShutdownSupplier(() -> {
					externalShutdownFuture.complete(true);
					return CompletableFuture.completedFuture(Acknowledge.get());
				});

		ApplicationDispatcherBootstrap bootstrap = createApplicationDispatcherBootstrap(3);

		final CompletableFuture<Acknowledge> shutdownFuture =
				bootstrap.runApplicationAndShutdownClusterAsync(dispatcherBuilder.build(), scheduledExecutor);

		// wait until the bootstrap "thinks" it's done
		shutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		// verify that the dispatcher is actually being shut down
		assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(true));
	}

	@Test
	public void testClusterShutdownWhenApplicationSucceeds() throws Exception {
		// we're "listening" on this to be completed to verify that the cluster
		// is being shut down from the ApplicationDispatcherBootstrap
		final CompletableFuture<Boolean> externalShutdownFuture = new CompletableFuture<>();

		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobId -> CompletableFuture.completedFuture(JobStatus.FINISHED))
				.setRequestJobResultFunction(jobId -> CompletableFuture.completedFuture(createSuccessfulJobResult(jobId)))
				.setClusterShutdownSupplier(() -> {
					externalShutdownFuture.complete(true);
					return CompletableFuture.completedFuture(Acknowledge.get());
				});

		ApplicationDispatcherBootstrap bootstrap = createApplicationDispatcherBootstrap(3);

		final CompletableFuture<Acknowledge> shutdownFuture =
				bootstrap.runApplicationAndShutdownClusterAsync(dispatcherBuilder.build(), scheduledExecutor);

		// wait until the bootstrap "thinks" it's done
		shutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		// verify that the dispatcher is actually being shut down
		assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(true));
	}

	@Test
	public void testClusterShutdownWhenApplicationFails() throws Exception {
		// we're "listening" on this to be completed to verify that the cluster
		// is being shut down from the ApplicationDispatcherBootstrap
		final CompletableFuture<Boolean> externalShutdownFuture = new CompletableFuture<>();

		final TestingDispatcherGateway.Builder dispatcherBuilder = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobId -> CompletableFuture.completedFuture(JobStatus.FAILED))
				.setRequestJobResultFunction(jobId -> CompletableFuture.completedFuture(createFailedJobResult(jobId)))
				.setClusterShutdownSupplier(() -> {
					externalShutdownFuture.complete(true);
					return CompletableFuture.completedFuture(Acknowledge.get());
				});

		ApplicationDispatcherBootstrap bootstrap = createApplicationDispatcherBootstrap(3);

		final CompletableFuture<Acknowledge> shutdownFuture =
				bootstrap.runApplicationAndShutdownClusterAsync(dispatcherBuilder.build(), scheduledExecutor);

		// wait until the bootstrap "thinks" it's done
		shutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		// verify that the dispatcher is actually being shut down
		assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(true));
	}

	private CompletableFuture<Void> runApplication(
			TestingDispatcherGateway.Builder dispatcherBuilder,
			int noOfJobs,
			boolean enforceSingleJobExecution) throws FlinkException {

		final PackagedProgram program = getProgram(noOfJobs);

		final ApplicationDispatcherBootstrap bootstrap =
				new ApplicationDispatcherBootstrap(
						program, Collections.emptyList(), getConfiguration());

		return bootstrap.runApplicationAsync(
				dispatcherBuilder.build(),
				scheduledExecutor,
				enforceSingleJobExecution);
	}

	private ApplicationDispatcherBootstrap createApplicationDispatcherBootstrap(int noOfJobs) throws FlinkException {
		return createApplicationDispatcherBootstrap(noOfJobs, Collections.emptyList());
	}

	private ApplicationDispatcherBootstrap createApplicationDispatcherBootstrap(
			int noOfJobs,
			Collection<JobGraph> recoveredJobGraphs) throws FlinkException {
		final PackagedProgram program = getProgram(noOfJobs);
		return new ApplicationDispatcherBootstrap(program, recoveredJobGraphs, getConfiguration());
	}

	private PackagedProgram getProgram(int noOfJobs) throws FlinkException {
		try {
			return PackagedProgram.newBuilder()
					.setUserClassPaths(Collections.singletonList(new File(CliFrontendTestUtils.getTestJarPath()).toURI().toURL()))
					.setEntryPointClassName(MULTI_EXECUTE_JOB_CLASS_NAME)
					.setArguments(String.valueOf(noOfJobs))
					.build();
		} catch (ProgramInvocationException | FileNotFoundException | MalformedURLException e) {
			throw new FlinkException("Could not load the provided entrypoint class.", e);
		}
	}

	private static JobResult createFailedJobResult(final JobID jobId) {
		return new JobResult.Builder()
				.jobId(jobId)
				.netRuntime(2L)
				.applicationStatus(ApplicationStatus.FAILED)
				.serializedThrowable(new SerializedThrowable(new JobExecutionException(jobId, "bla bla bla")))
				.build();
	}

	private static JobResult createSuccessfulJobResult(final JobID jobId) {
		return new JobResult.Builder()
				.jobId(jobId)
				.netRuntime(2L)
				.applicationStatus(ApplicationStatus.SUCCEEDED)
				.build();
	}

	private static <T, E extends Throwable> void assertException(
			CompletableFuture<T> future,
			Class<E> exceptionClass) throws Exception {

		try {
			future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		} catch (Throwable e) {
			Optional<E> expectionException =
					ExceptionUtils.findThrowable(e, exceptionClass);
			if (!expectionException.isPresent()) {
				throw e;
			}
			return;
		}
		fail("Future should have completed exceptionally with " + exceptionClass.getCanonicalName() + ".");
	}

	private Configuration getConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
		return configuration;
	}
}
