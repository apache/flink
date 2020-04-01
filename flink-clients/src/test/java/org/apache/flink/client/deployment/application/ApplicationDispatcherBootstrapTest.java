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
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedThrowable;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ApplicationDispatcherBootstrap}.
 */
public class ApplicationDispatcherBootstrapTest {

	private static final String MULTI_EXECUTE_JOB_CLASS_NAME = "org.apache.flink.client.testjar.MultiExecuteJob";

	/**
	 * Tests that the {@link org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader}
	 * collects the ids of the submitted jobs correctly after submission. These will be used later to check the status
	 * of the individual jobs in order to decide when to shut down the cluster.
	 *
	 * <p>Given that the current API does not allow the user to specify a JobID for each {@code execute()} call, we can
	 * only check that the number of collected jobIDs is equal to the expected one, but we cannot check the actual jobIDs.
	 */
	@Test
	public void testJobIdCollection() throws Exception {
		final int noOfJobs = 3;
		final CompletableFuture<List<JobID>> jobIDs = submitAndGetJobIDs(noOfJobs, false);
		assertThat(jobIDs.get().size(), is(equalTo(noOfJobs)));
	}

	@Test(expected = ApplicationExecutionException.class)
	public void testExceptionThrownWhenApplicationContainsNoJobs() throws Throwable {
		final int noOfJobs = 0;

		final CompletableFuture<List<JobID>> jobIdsFuture = submitAndGetJobIDs(noOfJobs, false);
		assertTrue(jobIdsFuture.isDone() && jobIdsFuture.isCompletedExceptionally());

		try {
			jobIdsFuture.get();
		} catch (Exception e) {
			Optional<ApplicationExecutionException> appException =
					ExceptionUtils.findThrowable(e, ApplicationExecutionException.class);
			if (appException.isPresent()) {
				throw appException.get();
			}
			throw e;
		}
		fail("Test should have failed with ApplicationExecutionException.");
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testExceptionThrownWithMultiJobApplicationIfOnlyOneJobIsAllowed() throws Throwable {
		final int noOfJobs = 3;

		final CompletableFuture<List<JobID>> jobIdsFuture = submitAndGetJobIDs(noOfJobs, true);
		assertTrue(jobIdsFuture.isDone() && jobIdsFuture.isCompletedExceptionally());

		try {
			jobIdsFuture.get();
		} catch (Throwable e) {
			Optional<FlinkRuntimeException> appException =
					ExceptionUtils.findThrowable(e, FlinkRuntimeException.class);
			if (appException.isPresent()) {
				throw appException.get();
			}
			throw e;
		}
		fail("Test should have failed with ApplicationExecutionException.");
	}

	@Test
	public void testApplicationFailsAsSoonAsOneJobFails() throws FlinkException {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

			final Map<JobID, CompletableFuture<JobResult>> simulatedPendingJobsOfApplication =
					createSubmittedApplicationWithNoOfPendingJobs(3);

			final CompletableFuture<Void> applicationTerminationFuture =
					getApplicationTerminationFuture(simulatedPendingJobsOfApplication, scheduledExecutor)
							.whenComplete((e, t) -> completionFuture.complete(null));

			assertFalse(applicationTerminationFuture.isDone());

			successfullyTerminateJob(simulatedPendingJobsOfApplication, 2);
			assertFalse(applicationTerminationFuture.isDone());

			failJob(simulatedPendingJobsOfApplication, 0);

			completionFuture.join();
			assertTrue(applicationTerminationFuture.isCompletedExceptionally());
		} finally {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	@Test
	public void testApplicationSucceedsWhenALLJobsSucceed() throws FlinkException {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

			final Map<JobID, CompletableFuture<JobResult>> simulatedJobsOfApplication =
					createSubmittedApplicationWithNoOfPendingJobs(3);

			final CompletableFuture<Void> applicationTerminationFuture =
					getApplicationTerminationFuture(simulatedJobsOfApplication, scheduledExecutor)
							.whenComplete((e, t) -> completionFuture.complete(null));

			assertFalse(applicationTerminationFuture.isDone());

			successfullyTerminateJob(simulatedJobsOfApplication, 2);
			assertFalse(applicationTerminationFuture.isDone());

			successfullyTerminateJob(simulatedJobsOfApplication, 1);
			assertFalse(applicationTerminationFuture.isDone());

			successfullyTerminateJob(simulatedJobsOfApplication, 0);

			completionFuture.join();
			assertTrue(applicationTerminationFuture.isDone());
		} finally {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	@Test
	public void testClusterShutdownWhenApplicationIsCancelled() throws Exception {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final JobID testJobId = new JobID(0, 2);
			final CompletableFuture<JobStatus> jobTerminationFuture = new CompletableFuture<>();

			final PackagedProgram program = getProgram(1);

			final Configuration configuration = getEmbeddedExecutorConfiguration();
			configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobId.toHexString());

			final DispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
					.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
					.setRequestJobStatusFunction(jobID -> jobTerminationFuture)
					.setClusterShutdownSupplier(() -> CompletableFuture.completedFuture(Acknowledge.get()))
					.build();

			final ApplicationDispatcherBootstrap bootstrap = new ApplicationDispatcherBootstrap(program, Collections.emptyList(), configuration);

			final CompletableFuture<Acknowledge> clusterShutdownFuture =
					bootstrap.runApplicationAndShutdownClusterAsync(testingDispatcherGateway, scheduledExecutor);

			final CompletableFuture<Void> applicationCompletionFuture = bootstrap.getApplicationStatusFuture();
			assertFalse(applicationCompletionFuture.isDone());

			bootstrap.stop();
			clusterShutdownFuture.get();

			assertTrue(applicationCompletionFuture.isDone());
		} finally {
			ExecutorUtils.gracefulShutdown(5L, TimeUnit.SECONDS, executor);
		}
	}

	@Test
	public void testClusterShutdownWhenSubmissionFails() throws Exception {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final JobID testJobId = new JobID(0, 2);
			final CompletableFuture<JobResult> jobTerminationFuture = new CompletableFuture<>();

			final PackagedProgram program = getProgram(1);

			final Configuration configuration = getEmbeddedExecutorConfiguration();
			configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobId.toHexString());

			final DispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
					.setSubmitFunction(jobGraph -> {
						throw new FlinkRuntimeException(new UnsupportedOperationException());
					})
					.setClusterShutdownSupplier(() -> CompletableFuture.completedFuture(Acknowledge.get()))
					.build();

			final ApplicationDispatcherBootstrap bootstrap = new ApplicationDispatcherBootstrap(program, Collections.emptyList(), configuration);

			final CompletableFuture<Acknowledge> clusterShutdownFuture =
					bootstrap.runApplicationAndShutdownClusterAsync(testingDispatcherGateway, scheduledExecutor);

			final CompletableFuture<Void> applicationCompletionFuture = bootstrap.getApplicationStatusFuture();
			assertFalse(applicationCompletionFuture.isDone());

			clusterShutdownFuture.get();

			assertTrue(applicationCompletionFuture.isDone() && applicationCompletionFuture.isCompletedExceptionally());
		} finally {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	@Test
	public void testClusterShutdownWhenApplicationSucceeds() throws Exception {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final JobID testJobId = new JobID(0, 2);
			final CompletableFuture<JobResult> jobTerminationFuture = new CompletableFuture<>();
			final CompletableFuture<Void> clusterTerminationFuture = new CompletableFuture<>();

			final CompletableFuture<Void> applicationCompletionFuture =
					runSingleJobApplication(scheduledExecutor, testJobId, jobTerminationFuture, clusterTerminationFuture);

			assertFalse(applicationCompletionFuture.isDone());

			jobTerminationFuture.complete(createSuccessfulJobResult(testJobId));
			clusterTerminationFuture.get();

			assertTrue(
					applicationCompletionFuture.isDone()
							&& !applicationCompletionFuture.isCompletedExceptionally()
							&& !applicationCompletionFuture.isCancelled());
		} finally {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	@Test
	public void testClusterShutdownWhenApplicationFails() throws Exception {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final JobID testJobId = new JobID(0, 2);
			final CompletableFuture<JobResult> jobTerminationFuture = new CompletableFuture<>();
			final CompletableFuture<Void> clusterTerminationFuture = new CompletableFuture<>();

			final CompletableFuture<Void> applicationCompletionFuture =
					runSingleJobApplication(scheduledExecutor, testJobId, jobTerminationFuture, clusterTerminationFuture);

			assertFalse(applicationCompletionFuture.isDone());

			jobTerminationFuture.complete(createFailedJobResult(testJobId));
			clusterTerminationFuture.get();

			assertTrue(applicationCompletionFuture.isDone() && applicationCompletionFuture.isCompletedExceptionally());
		} finally {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	private CompletableFuture<Void> runSingleJobApplication(
			ScheduledExecutor scheduledExecutor,
			JobID testJobId,
			CompletableFuture<JobResult> jobTerminationFuture,
			CompletableFuture<Void> clusterTerminationFuture) throws FlinkException {

		final PackagedProgram program = getProgram(1);

		final Configuration configuration = getEmbeddedExecutorConfiguration();
		configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobId.toHexString());

		final DispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
				.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
				.setRequestJobStatusFunction(jobID -> CompletableFuture.completedFuture(JobStatus.FINISHED))
				.setRequestJobResultFunction(jobID -> jobTerminationFuture)
				.setClusterShutdownSupplier(() -> {
					clusterTerminationFuture.complete(null);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.build();

		final ApplicationDispatcherBootstrap bootstrap = new ApplicationDispatcherBootstrap(program, Collections.emptyList(), configuration);
		bootstrap.runApplicationAndShutdownClusterAsync(testingDispatcherGateway, scheduledExecutor);
		return bootstrap.getApplicationStatusFuture();
	}

	private CompletableFuture<List<JobID>> submitAndGetJobIDs(int noOfJobs, boolean enforceSingleJobExecution) throws FlinkException {
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		try {
			final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

			final PackagedProgram program = getProgram(noOfJobs);

			final Configuration configuration = getEmbeddedExecutorConfiguration();

			final DispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
					.setSubmitFunction(jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
					.build();

			final ApplicationDispatcherBootstrap bootstrap = new ApplicationDispatcherBootstrap(program, Collections.emptyList(), configuration);
			final CompletableFuture<List<JobID>> jobIdsFuture = new CompletableFuture<>();
			final List<JobID> jobIds = bootstrap.tryRunApplicationAndGetJobIDs(
					jobIdsFuture,
					testingDispatcherGateway,
					scheduledExecutor,
					enforceSingleJobExecution);
			return jobIdsFuture;
		} finally {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	private void successfullyTerminateJob(Map<JobID, CompletableFuture<JobResult>> pendingJobResults, int jobIdx) {
		final JobID jobID = (JobID) pendingJobResults.keySet().toArray()[jobIdx];
		pendingJobResults.get(jobID).complete(createSuccessfulJobResult(jobID));
	}

	private void failJob(Map<JobID, CompletableFuture<JobResult>> pendingJobResults, int jobIdx) {
		final JobID jobID = (JobID) pendingJobResults.keySet().toArray()[jobIdx];
		pendingJobResults.get(jobID).complete(createFailedJobResult(jobID));
	}

	private Map<JobID, CompletableFuture<JobResult>> createSubmittedApplicationWithNoOfPendingJobs(final int noOfJobs)  {
		final Map<JobID, CompletableFuture<JobResult>> jobResults = new HashMap<>();
		for (int i = 0; i < noOfJobs; i++) {
			jobResults.put(new JobID(0, i), new CompletableFuture<>());
		}
		return jobResults;
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

	private CompletableFuture<Void> getApplicationTerminationFuture(
			final Map<JobID, CompletableFuture<JobResult>> jobResults,
			final ScheduledExecutor scheduledExecutor) throws FlinkException {

		final DispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
				.setRequestJobStatusFunction(jobID -> CompletableFuture.completedFuture(JobStatus.FINISHED))
				.setRequestJobResultFunction(jobResults::get)
				.build();

		final ApplicationDispatcherBootstrap bootstrap =
				new ApplicationDispatcherBootstrap(
						getProgram(3), // this is ignored
						Collections.emptyList(),
						getEmbeddedExecutorConfiguration());

		return bootstrap.getApplicationResult(testingDispatcherGateway, jobResults.keySet(), scheduledExecutor);
	}

	private static JobResult createFailedJobResult(final JobID jobId) {
		return new JobResult.Builder()
				.jobId(jobId)
				.netRuntime(2L)
				.applicationStatus(ApplicationStatus.FAILED)
				.serializedThrowable(new SerializedThrowable(new Exception("bla bla bla")))
				.build();
	}

	private static JobResult createSuccessfulJobResult(final JobID jobId) {
		return new JobResult.Builder()
				.jobId(jobId)
				.netRuntime(2L)
				.applicationStatus(ApplicationStatus.SUCCEEDED)
				.build();
	}

	private Configuration getEmbeddedExecutorConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
		return configuration;
	}
}
