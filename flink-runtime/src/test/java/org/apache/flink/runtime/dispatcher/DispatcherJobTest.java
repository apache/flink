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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;

/**
 * Test for the {@link DispatcherJob} class.
 */
public class DispatcherJobTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);
	private static final JobID TEST_JOB_ID = new JobID();

	@Test
	public void testStatusWhenInitializing() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		assertThat(dispatcherJob.isInitialized(), is(false));
		assertThat(dispatcherJob.getResultFuture().isDone(), is(false));
		assertJobStatus(dispatcherJob, JobStatus.INITIALIZING);
	}

	@Test
	public void testStatusWhenRunning() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// finish initialization
		testContext.setRunning();

		assertJobStatus(dispatcherJob, JobStatus.RUNNING);

		// result future not done
		assertThat(dispatcherJob.getResultFuture().isDone(), is(false));

		assertThat(dispatcherJob.isInitialized(), is(true));
	}

	@Test
	public void testStatusWhenJobFinished() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// finish job
		testContext.setRunning();
		testContext.finishJob();

		assertJobStatus(dispatcherJob, JobStatus.FINISHED);

		// assert result future done
		DispatcherJobResult result = dispatcherJob.getResultFuture().get();

		assertThat(result.getArchivedExecutionGraph().getState(), is(JobStatus.FINISHED));
	}

	@Test
	public void testStatusWhenCancellingWhileInitializing() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();
		assertJobStatus(dispatcherJob, JobStatus.INITIALIZING);

		CompletableFuture<Acknowledge> cancelFuture = dispatcherJob.cancel(
			TIMEOUT);

		assertThat(cancelFuture.isDone(), is(false));
		assertThat(dispatcherJob.isInitialized(), is(false));

		assertJobStatus(dispatcherJob, JobStatus.CANCELLING);

		testContext.setRunning();
		testContext.finishCancellation();

		// assert that cancel future completes
		cancelFuture.get();

		assertJobStatus(dispatcherJob, JobStatus.CANCELED);
		assertThat(dispatcherJob.isInitialized(), is(true));
		// assert that the result future completes
		assertThat(dispatcherJob.getResultFuture().get().getArchivedExecutionGraph().getState(), is(JobStatus.CANCELED));
	}

	@Test
	public void testStatusWhenCancellingWhileRunning() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		testContext.setRunning();
		CompletableFuture<Acknowledge> cancelFuture = dispatcherJob.cancel(TIMEOUT);

		assertJobStatus(dispatcherJob, JobStatus.CANCELLING);
		testContext.finishCancellation();

		cancelFuture.get();
		assertJobStatus(dispatcherJob, JobStatus.CANCELED);
		assertThat(dispatcherJob.getResultFuture().get().getArchivedExecutionGraph().getState(), is(JobStatus.CANCELED));
	}

	@Test
	public void testStatusWhenCancellingWhileFailed() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		RuntimeException exception = new RuntimeException("Artificial failure in runner initialization");
		testContext.failInitialization(exception);

		assertJobStatus(dispatcherJob, JobStatus.FAILED);

		CommonTestUtils.assertThrows("Artificial failure", ExecutionException.class, () -> dispatcherJob.cancel(TIMEOUT).get());

		assertJobStatus(dispatcherJob, JobStatus.FAILED);
	}

	@Test
	public void testErrorWhileInitializing() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// now fail
		RuntimeException exception = new RuntimeException("Artificial failure in runner initialization");
		testContext.failInitialization(exception);

		assertThat(dispatcherJob.isInitialized(), is(true));
		assertJobStatus(dispatcherJob, JobStatus.FAILED);

		ArchivedExecutionGraph aeg = dispatcherJob.getResultFuture().get().getArchivedExecutionGraph();
		assertThat(aeg.getFailureInfo().getException().deserializeError(ClassLoader.getSystemClassLoader()), is(exception));
	}

	@Test
	public void testDispatcherJobResult() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();
		testContext.failInitialization(new RuntimeException("Artificial failure in runner initialization"));

		DispatcherJobResult result = dispatcherJob.getResultFuture().get();
		assertThat(result.isInitializationFailure(), is(true));
		assertThat(result.getArchivedExecutionGraph().getState(), is(JobStatus.FAILED));
		assertThat(result.getInitializationFailure().getMessage(), containsString("Artificial failure"));
	}

	@Test
	public void testCloseWhileInitializingSuccessfully() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();
		assertThat(closeFuture.isDone(), is(false));

		// set job running, so that we can cancel it
		testContext.setRunning();

		// assert future completes now
		closeFuture.get();

		// ensure the result future is complete (how it completes is up to the JobManager)
		CompletableFuture<DispatcherJobResult> resultFuture = dispatcherJob.getResultFuture();
		CommonTestUtils.assertThrows("has not been finished", ExecutionException.class,
			resultFuture::get);
	}

	@Test
	public void testCloseWhileInitializingErroneously() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();
		assertThat(closeFuture.isDone(), is(false));

		testContext.failInitialization(new RuntimeException("fail"));

		// assert future completes now
		closeFuture.get();

		// ensure the result future is complete
		dispatcherJob.getResultFuture().get();
	}

	@Test
	public void testCloseWhileRunning() throws Exception {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// complete JobManager runner future to indicate to the DispatcherJob that the Runner has been initialized
		testContext.setRunning();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();

		closeFuture.get();

		// result future should complete exceptionally.
		CompletableFuture<DispatcherJobResult> resultFuture = dispatcherJob.getResultFuture();
		CommonTestUtils.assertThrows("has not been finished", ExecutionException.class,
			resultFuture::get);
	}

	@Test(expected = IllegalStateException.class)
	public void testUnavailableJobMasterGateway() {
		TestContext testContext = createTestContext();
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();
		dispatcherJob.getJobMasterGateway();
	}

	private TestContext createTestContext() {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);

		JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "testJob", testVertex);
		CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture = new CompletableFuture<>();
		DispatcherJob dispatcherJob = DispatcherJob.createFor(jobManagerRunnerCompletableFuture,
			jobGraph.getJobID(), jobGraph.getName(), System.currentTimeMillis());

		return new TestContext(
			jobManagerRunnerCompletableFuture,
			dispatcherJob,
			jobGraph);
	}

	private static class TestContext {
		private final CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture;
		private final DispatcherJob dispatcherJob;
		private final JobGraph jobGraph;
		private final TestingJobMasterGateway mockRunningJobMasterGateway;
		private final CompletableFuture<Acknowledge> cancellationFuture;

		private JobStatus internalJobStatus = JobStatus.INITIALIZING;
		private CompletableFuture<ArchivedExecutionGraph> resultFuture = new CompletableFuture<>();

		public TestContext (
				CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture,
				DispatcherJob dispatcherJob,
				JobGraph jobGraph) {
			this.jobManagerRunnerCompletableFuture = jobManagerRunnerCompletableFuture;
			this.dispatcherJob = dispatcherJob;
			this.jobGraph = jobGraph;

			this.cancellationFuture = new CompletableFuture<>();
			this.mockRunningJobMasterGateway = new TestingJobMasterGatewayBuilder()
				.setRequestJobSupplier(() -> CompletableFuture.completedFuture(ArchivedExecutionGraph.createFromInitializingJob(getJobID(), "test", internalJobStatus, null, 1337)))
				.setRequestJobDetailsSupplier(() -> {
					JobDetails jobDetails = new JobDetails(getJobID(), "", 0, 0, 0, internalJobStatus, 0,
						new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0}, 0);
					return CompletableFuture.completedFuture(jobDetails);
				})
				// once JobManagerRunner is initialized, complete result future with CANCELLED AEG and ack cancellation.
				.setCancelFunction(() -> {
					internalJobStatus = JobStatus.CANCELLING;
					return cancellationFuture;
				})
				.build();
		}

		public JobID getJobID() {
			return jobGraph.getJobID();
		}

		public void failInitialization(Throwable ex) {
			jobManagerRunnerCompletableFuture.completeExceptionally(ex);
		}

		public DispatcherJob getDispatcherJob() {
			return dispatcherJob;
		}

		public void setRunning() {
			internalJobStatus = JobStatus.RUNNING;
			JobManagerRunner jobManagerRunner = new TestingJobManagerRunner.Builder()
				.setJobId(getJobID())
				.setBlockingTermination(false)
				.setJobMasterGatewayFuture(CompletableFuture.completedFuture(mockRunningJobMasterGateway))
				.setResultFuture(resultFuture)
				.build();
			jobManagerRunnerCompletableFuture.complete(jobManagerRunner);
		}

		public void finishJob() {
			internalJobStatus = JobStatus.FINISHED;
			resultFuture.complete(
				ArchivedExecutionGraph.createFromInitializingJob(getJobID(), "test", JobStatus.FINISHED, null, 1337));
		}

		public void finishCancellation() {
			jobManagerRunnerCompletableFuture.thenAccept(runner -> {
				internalJobStatus = JobStatus.CANCELED;
				runner.getResultFuture()
					.complete(ArchivedExecutionGraph.createFromInitializingJob(getJobID(), "test", JobStatus.CANCELED, null, 1337));
				cancellationFuture.complete(Acknowledge.get());
			});
		}
	}

	private void assertJobStatus(DispatcherJob dispatcherJob, JobStatus expectedStatus) throws Exception {
		assertThat(dispatcherJob.requestJobDetails(TIMEOUT).get().getStatus(), is(expectedStatus));
		assertThat(dispatcherJob.requestJob(TIMEOUT).get().getState(), is(expectedStatus));
		assertThat(dispatcherJob.requestJobStatus(TIMEOUT).get(), is(expectedStatus));
	}
}
