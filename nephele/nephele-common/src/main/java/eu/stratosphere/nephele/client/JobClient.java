/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.JobManagementProtocol;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The job client is able to submit, control, and abort jobs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class JobClient {

	/**
	 * The logging object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(JobClient.class);

	/**
	 * The job management server stub.
	 */
	private final JobManagementProtocol jobSubmitClient;

	/**
	 * The job graph assigned with this job client.
	 */
	private final JobGraph jobGraph;

	/**
	 * The configuration assigned with this job client.
	 */
	private final Configuration configuration;

	/**
	 * The shutdown hook which is executed if the user interrupts the job the job execution.
	 */
	private final JobCleanUp jobCleanUp;

	/**
	 * The sequence number of the last processed event received from the job manager.
	 */
	private long lastProcessedEventSequenceNumber = -1;

	/**
	 * Inner class used to perform clean up tasks when the
	 * job client is terminated.
	 * 
	 * @author warneke
	 */
	public static class JobCleanUp extends Thread {

		/**
		 * Stores a reference to the {@link JobClient} object this clean up object has been created for.
		 */
		private final JobClient jobClient;

		/**
		 * Constructs a new clean up object which is used to perform clean up tasks
		 * when the job client is terminated.
		 * 
		 * @param jobClient
		 *        the job client this clean up object belongs to
		 */
		public JobCleanUp(final JobClient jobClient) {

			this.jobClient = jobClient;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {

			try {

				// Terminate the running job if the configuration says so
				if (this.jobClient.getConfiguration().getBoolean(ConfigConstants.JOBCLIENT_SHUTDOWN_TERMINATEJOB_KEY,
					ConfigConstants.DEFAULT_JOBCLIENT_SHUTDOWN_TERMINATEJOB)) {
					System.out.println(AbstractEvent.timestampToString(System.currentTimeMillis())
						+ ":\tJobClient is shutting down, canceling job...");
					this.jobClient.cancelJob();
				}

				// Close the RPC object
				this.jobClient.close();

			} catch (IOException ioe) {
				LOG.warn(StringUtils.stringifyException(ioe));
			}
		}

	}

	/**
	 * Constructs a new job client object and instantiates a local
	 * RPC proxy for the {@link JobSubmissionProtocol}.
	 * 
	 * @param jobGraph
	 *        the job graph to run
	 * @throws IOException
	 *         thrown on error while initializing the RPC connection to the job manager
	 */
	public JobClient(final JobGraph jobGraph) throws IOException {

		this(jobGraph, new Configuration());
	}

	/**
	 * Constructs a new job client object and instantiates a local
	 * RPC proxy for the {@link JobSubmissionProtocol}.
	 * 
	 * @param jobGraph
	 *        the job graph to run
	 * @param configuration
	 *        configuration object which can include special configuration settings for the job client
	 * @throws IOException
	 *         thrown on error while initializing the RPC connection to the job manager
	 */
	public JobClient(final JobGraph jobGraph, final Configuration configuration) throws IOException {

		final String address = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_ADDRESS);
		final int port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		final InetSocketAddress inetaddr = new InetSocketAddress(address, port);
		this.jobSubmitClient = RPC.getProxy(JobManagementProtocol.class, inetaddr, NetUtils.getSocketFactory());
		this.jobGraph = jobGraph;
		this.configuration = configuration;
		this.jobCleanUp = new JobCleanUp(this);
	}

	/**
	 * Constructs a new job client object and instantiates a local
	 * RPC proxy for the {@link JobSubmissionProtocol}.
	 * 
	 * @param jobGraph
	 *        the job graph to run
	 * @param configuration
	 *        configuration object which can include special configuration settings for the job client
	 * @param jobManagerAddress
	 *        IP/Port of the jobmanager (not taken from provided configuration object).
	 * @throws IOException
	 *         thrown on error while initializing the RPC connection to the job manager
	 */
	public JobClient(final JobGraph jobGraph, final Configuration configuration,
			final InetSocketAddress jobManagerAddress)
			throws IOException {

		this.jobSubmitClient = RPC
			.getProxy(JobManagementProtocol.class, jobManagerAddress, NetUtils.getSocketFactory());
		this.jobGraph = jobGraph;
		this.configuration = configuration;
		this.jobCleanUp = new JobCleanUp(this);
	}

	/**
	 * Closes the <code>JobClient</code> by destroying the RPC stub object.
	 */
	public void close() {

		synchronized (this.jobSubmitClient) {
			RPC.stopProxy(this.jobSubmitClient);
		}
	}

	/**
	 * Returns the {@link Configuration} object which can include special configuration settings for the job client.
	 * 
	 * @return the {@link Configuration} object which can include special configuration settings for the job client
	 */
	public Configuration getConfiguration() {

		return this.configuration;
	}

	/**
	 * Submits the job assigned to this job client to the job manager.
	 * 
	 * @return a <code>JobSubmissionResult</code> object encapsulating the results of the job submission
	 * @throws IOException
	 *         thrown in case of submission errors while transmitting the data to the job manager
	 */
	public JobSubmissionResult submitJob() throws IOException {

		synchronized (this.jobSubmitClient) {

			return this.jobSubmitClient.submitJob(this.jobGraph);
		}
	}

	/**
	 * Cancels the job assigned to this job client.
	 * 
	 * @return a <code>JobCancelResult</code> object encapsulating the result of the job cancel request
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request to the job manager
	 */
	public JobCancelResult cancelJob() throws IOException {

		synchronized (this.jobSubmitClient) {
			return this.jobSubmitClient.cancelJob(this.jobGraph.getJobID());
		}
	}

	/**
	 * Retrieves the current status of the job assigned to this job client.
	 * 
	 * @return a <code>JobProgressResult</code> object including the current job progress
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 */
	public JobProgressResult getJobProgress() throws IOException {

		synchronized (this.jobSubmitClient) {
			return this.jobSubmitClient.getJobProgress(this.jobGraph.getJobID());
		}
	}

	/**
	 * Submits the job assigned to this job client to the job manager and queries the job manager
	 * about the progress of the job until it is either finished or aborted.
	 * 
	 * @return the duration of the job execution in milliseconds
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 * @throws JobExecutionException
	 *         thrown if the job has been aborted either by the user or as a result of an error
	 */
	public long submitJobAndWait() throws IOException, JobExecutionException {

		synchronized (this.jobSubmitClient) {

			final JobSubmissionResult submissionResult = this.jobSubmitClient.submitJob(this.jobGraph);
			if (submissionResult.getReturnCode() == AbstractJobResult.ReturnCode.ERROR) {
				LOG.error("ERROR: " + submissionResult.getDescription());
				throw new JobExecutionException(submissionResult.getDescription(), false);
			}

			// Make sure the job is properly terminated when the user shut's down the client
			Runtime.getRuntime().addShutdownHook(this.jobCleanUp);
		}

		long sleep = 0;
		try {
			final IntegerRecord interval = this.jobSubmitClient.getRecommendedPollingInterval();
			sleep = interval.getValue() * 1000;
		} catch (IOException ioe) {
			Runtime.getRuntime().removeShutdownHook(this.jobCleanUp);
			// Rethrow error
			throw ioe;
		}

		try {
			Thread.sleep(sleep / 2);
		} catch (InterruptedException e) {
			Runtime.getRuntime().removeShutdownHook(this.jobCleanUp);
			logErrorAndRethrow(StringUtils.stringifyException(e));
		}

		long startTimestamp = -1;

		while (true) {

			if (Thread.interrupted()) {
				logErrorAndRethrow("Job client has been interrupted");
			}

			JobProgressResult jobProgressResult = null;
			try {
				jobProgressResult = getJobProgress();
			} catch (IOException ioe) {
				Runtime.getRuntime().removeShutdownHook(this.jobCleanUp);
				// Rethrow error
				throw ioe;
			}

			if (jobProgressResult == null) {
				logErrorAndRethrow("Returned job progress is unexpectedly null!");
			}

			if (jobProgressResult.getReturnCode() == AbstractJobResult.ReturnCode.ERROR) {
				logErrorAndRethrow("Could not retrieve job progress: " + jobProgressResult.getDescription());
			}

			final Iterator<AbstractEvent> it = jobProgressResult.getEvents();
			while (it.hasNext()) {

				final AbstractEvent event = it.next();

				// Did we already process that event?
				if (this.lastProcessedEventSequenceNumber >= event.getSequenceNumber()) {
					continue;
				}

				System.out.println(event.toString());

				this.lastProcessedEventSequenceNumber = event.getSequenceNumber();

				// Check if we can exit the loop
				if (event instanceof JobEvent) {
					final JobEvent jobEvent = (JobEvent) event;
					final JobStatus jobStatus = jobEvent.getCurrentJobStatus();
					if (jobStatus == JobStatus.SCHEDULED) {
						startTimestamp = jobEvent.getTimestamp();
					}
					if (jobStatus == JobStatus.FINISHED) {
						Runtime.getRuntime().removeShutdownHook(this.jobCleanUp);
						final long jobDuration = jobEvent.getTimestamp() - startTimestamp;
						System.out.println("Job duration (in ms): " + jobDuration);
						return jobDuration;
					} else if (jobStatus == JobStatus.CANCELED || jobStatus == JobStatus.FAILED) {
						Runtime.getRuntime().removeShutdownHook(this.jobCleanUp);
						LOG.info(jobEvent.getOptionalMessage());
						if (jobStatus == JobStatus.CANCELED) {
							throw new JobExecutionException(jobEvent.getOptionalMessage(), true);
						} else {
							throw new JobExecutionException(jobEvent.getOptionalMessage(), false);
						}
					}
				}
			}

			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				logErrorAndRethrow(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Returns the recommended interval in seconds in which a client
	 * is supposed to poll for progress information.
	 * 
	 * @return the interval in seconds
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 */
	public int getRecommendedPollingInterval() throws IOException {

		synchronized (this.jobSubmitClient) {
			return this.jobSubmitClient.getRecommendedPollingInterval().getValue();
		}
	}

	/**
	 * Writes the given error message to the log and throws it in an {@link IOException}.
	 * 
	 * @param errorMessage
	 *        the error message to write to the log
	 * @throws IOException
	 *         thrown after the error message is written to the log
	 */
	private void logErrorAndRethrow(final String errorMessage) throws IOException {

		LOG.error(errorMessage);
		throw new IOException(errorMessage);
	}

}
