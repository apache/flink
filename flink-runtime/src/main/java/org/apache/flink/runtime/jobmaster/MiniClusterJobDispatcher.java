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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The dispatcher that runs in the mini cluster, waits for jobs, and starts job masters
 * upon receiving jobs.
 */
public class MiniClusterJobDispatcher {

	private static final Logger LOG = LoggerFactory.getLogger(MiniClusterJobDispatcher.class);

	// ------------------------------------------------------------------------

	/** lock to ensure that this dispatcher executes only one job at a time */
	private final Object lock = new Object();

	/** the configuration with which the mini cluster was started */
	private final Configuration configuration;

	/** the RPC service to use by the job managers */
	private final RpcService rpcService;

	/** services for discovery, leader election, and recovery */
	private final HighAvailabilityServices haServices;

	/** al the services that the JobManager needs, such as BLOB service, factories, etc */
	private final JobManagerServices jobManagerServices;

	/** The number of JobManagers to launch (more than one simulates a high-availability setup) */
	private final int numJobManagers;

	/** The runner for the job and master. non-null if a job is currently running */
	private volatile JobManagerRunner[] runners;

	/** flag marking the dispatcher as hut down */
	private volatile boolean shutdown;


	/**
	 * Starts a mini cluster job dispatcher.
	 * 
	 * <p>The dispatcher kicks off one JobManager per job, a behavior similar to a
	 * non-highly-available setup.
	 * 
	 * @param config The configuration of the mini cluster
	 * @param haServices Access to the discovery, leader election, and recovery services
	 * 
	 * @throws Exception Thrown, if the services for the JobMaster could not be started.
	 */
	public MiniClusterJobDispatcher(
			Configuration config,
			RpcService rpcService,
			HighAvailabilityServices haServices) throws Exception {
		this(config, rpcService, haServices, 1);
	}

	/**
	 * Starts a mini cluster job dispatcher.
	 *
	 * <p>The dispatcher may kick off more than one JobManager per job, thus simulating
	 * a highly-available setup.
	 * 
	 * @param config The configuration of the mini cluster
	 * @param haServices Access to the discovery, leader election, and recovery services
	 * @param numJobManagers The number of JobMasters to start for each job.
	 * 
	 * @throws Exception Thrown, if the services for the JobMaster could not be started.
	 */
	public MiniClusterJobDispatcher(
			Configuration config,
			RpcService rpcService,
			HighAvailabilityServices haServices,
			int numJobManagers) throws Exception {

		checkArgument(numJobManagers >= 1);
		this.configuration = checkNotNull(config);
		this.rpcService = checkNotNull(rpcService);
		this.haServices = checkNotNull(haServices);
		this.numJobManagers = numJobManagers;

		LOG.info("Creating JobMaster services");
		this.jobManagerServices = JobManagerServices.fromConfiguration(config);
	}

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	/**
	 * Shuts down the mini cluster dispatcher. If a job is currently running, that job will be
	 * terminally failed.
	 */
	public void shutdown() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				LOG.info("Shutting down the dispatcher");

				// in this shutdown code we copy the references to the stack first,
				// to avoid concurrent modification

				JobManagerRunner[] runners = this.runners;
				if (runners != null) {
					this.runners = null;

					Exception shutdownException = new Exception("The MiniCluster is shutting down");
					for (JobManagerRunner runner : runners) {
						runner.shutdown(shutdownException);
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  submitting jobs
	// ------------------------------------------------------------------------

	/**
	 * This method executes a job in detached mode. The method returns immediately after the job
	 * has been added to the
	 *
	 * @param job  The Flink job to execute
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	public void runDetached(JobGraph job) throws JobExecutionException {
		checkNotNull(job);

		LOG.info("Received job for detached execution {} ({})", job.getName(), job.getJobID());

		synchronized (lock) {
			checkState(!shutdown, "mini cluster is shut down");
			checkState(runners == null, "mini cluster can only execute one job at a time");

			OnCompletionActions onJobCompletion = new DetachedFinalizer(numJobManagers);

			this.runners = startJobRunners(job, onJobCompletion);
		}
	}

	/**
	 * This method runs a job in blocking mode. The method returns only after the job
	 * completed successfully, or after it failed terminally.
	 *
	 * @param job  The Flink job to execute 
	 * @return The result of the job execution
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job lauch,
	 *         or if the job terminally failed.
	 */
	public JobExecutionResult runJobBlocking(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job);
		
		LOG.info("Received job for blocking execution {} ({})", job.getName(), job.getJobID());
		final BlockingJobSync onJobCompletion = new BlockingJobSync(job.getJobID(), numJobManagers);

		synchronized (lock) {
			checkState(!shutdown, "mini cluster is shut down");
			checkState(runners == null, "mini cluster can only execute one job at a time");

			this.runners = startJobRunners(job, onJobCompletion);
		}

		try {
			return onJobCompletion.getResult();
		}
		finally {
			// always clear the status for the next job
			runners = null;
		}
	}

	private JobManagerRunner[] startJobRunners(JobGraph job, OnCompletionActions onCompletion) throws JobExecutionException {
		LOG.info("Starting {} JobMaster(s) for job {} ({})", numJobManagers, job.getName(), job.getJobID());

		JobManagerRunner[] runners = new JobManagerRunner[numJobManagers];
		for (int i = 0; i < numJobManagers; i++) {
			try {
				runners[i] = new JobManagerRunner(job, configuration,
						rpcService, haServices, jobManagerServices, onCompletion);
				runners[i].start();
			}
			catch (Throwable t) {
				// shut down all the ones so far
				Exception shutdownCause = new Exception("Failed to properly start all mini cluster JobManagers", t);

				for (int k = 0; k <= i; k++) {
					try {
						if (runners[i] != null) {
							runners[i].shutdown(shutdownCause);
						}
					} catch (Throwable ignored) {
						// silent shutdown
					}
				}

				throw new JobExecutionException(job.getJobID(), "Could not start the JobManager(s) for the job", t);
			}
		}

		return runners;
	}

	// ------------------------------------------------------------------------
	//  test methods to simulate job master failures
	// ------------------------------------------------------------------------

	public void killJobMaster(int which) {
		checkArgument(which >= 0 && which < numJobManagers, "no such job master");
		checkState(!shutdown, "mini cluster is shut down");

		JobManagerRunner[] runners = this.runners;
		checkState(runners != null, "mini cluster it not executing a job right now");

		runners[which].shutdown(new Throwable("kill JobManager"));
	}

	// ------------------------------------------------------------------------
	//  utility classes
	// ------------------------------------------------------------------------

	/**
	 * Simple class that waits for all runners to have reported that they are done.
	 * In the case of a high-availability test setup, there may be multiple runners.
	 * After that, it marks the mini cluster as ready to receive new jobs.
	 */
	private class DetachedFinalizer implements OnCompletionActions {

		private final AtomicInteger numJobManagersToWaitFor;

		private DetachedFinalizer(int numJobManagersToWaitFor) {
			this.numJobManagersToWaitFor = new AtomicInteger(numJobManagersToWaitFor);
		}

		@Override
		public void jobFinished(JobExecutionResult result) {
			decrementCheckAndCleanup();
		}

		@Override
		public void jobFailed(Throwable cause) {
			decrementCheckAndCleanup();
		}

		@Override
		public void jobFinishedByOther() {
			decrementCheckAndCleanup();
		}

		@Override
		public void onFatalError(Throwable exception) {
			decrementCheckAndCleanup();
		}

		private void decrementCheckAndCleanup() {
			if (numJobManagersToWaitFor.decrementAndGet() == 0) {
				MiniClusterJobDispatcher.this.runners = null;
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * This class is used to sync on blocking jobs across multiple runners.
	 * Only after all runners reported back that they are finished, the
	 * result will be released.
	 * 
	 * That way it is guaranteed that after the blocking job submit call returns,
	 * the dispatcher is immediately free to accept another job.
	 */
	private static class BlockingJobSync implements OnCompletionActions {

		private final JobID jobId;

		private final CountDownLatch jobMastersToWaitFor;

		private volatile Throwable jobException;

		private volatile Throwable runnerException;

		private volatile JobExecutionResult result;
		
		BlockingJobSync(JobID jobId, int numJobMastersToWaitFor) {
			this.jobId = jobId;
			this.jobMastersToWaitFor = new CountDownLatch(numJobMastersToWaitFor);
		}

		@Override
		public void jobFinished(JobExecutionResult jobResult) {
			this.result = jobResult;
			jobMastersToWaitFor.countDown();
		}

		@Override
		public void jobFailed(Throwable cause) {
			jobException = cause;
			jobMastersToWaitFor.countDown();
		}

		@Override
		public void jobFinishedByOther() {
			this.jobMastersToWaitFor.countDown();
		}

		@Override
		public void onFatalError(Throwable exception) {
			if (runnerException == null) {
				runnerException = exception;
			}
		}

		public JobExecutionResult getResult() throws JobExecutionException, InterruptedException {
			jobMastersToWaitFor.await();

			final Throwable jobFailureCause = this.jobException;
			final Throwable runnerException = this.runnerException;
			final JobExecutionResult result = this.result;

			// (1) we check if teh job terminated with an exception
			// (2) we check whether the job completed successfully
			// (3) we check if we have exceptions from the JobManagers. the job may still have
			//     completed successfully in that case, if multiple JobMasters were running
			//     and other took over. only if all encounter a fatal error, the job cannot finish

			if (jobFailureCause != null) {
				if (jobFailureCause instanceof JobExecutionException) {
					throw (JobExecutionException) jobFailureCause;
				}
				else {
					throw new JobExecutionException(jobId, "The job execution failed", jobFailureCause);
				}
			}
			else if (result != null) {
				return result;
			}
			else if (runnerException != null) {
				throw new JobExecutionException(jobId,
						"The job execution failed because all JobManagers encountered fatal errors", runnerException);
			}
			else {
				throw new IllegalStateException("Bug: Job finished with neither error nor result.");
			}
		}
	}
}
