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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.messages.ShutdownClusterAfterJob;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Java representation of a running Flink cluster within YARN.
 */
public class YarnClusterClient extends ClusterClient {

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClient.class);

	private static final int POLLING_THREAD_INTERVAL_MS = 1000;

	private YarnClient yarnClient;

	private Thread clientShutdownHook = new ClientShutdownHook();
	private PollingThread pollingRunner;

	//---------- Class internal fields -------------------

	private final AbstractYarnClusterDescriptor clusterDescriptor;
	private final LazApplicationClientLoader applicationClient;
	private final FiniteDuration akkaDuration;
	private final ApplicationReport appReport;
	private final ApplicationId appId;
	private final String trackingURL;

	private boolean isConnected = true;

	/** Indicator whether this cluster has just been created. */
	private final boolean newlyCreatedCluster;

	/**
	 * Create a new Flink on YARN cluster.
	 *
	 * @param clusterDescriptor The descriptor used at cluster creation
	 * @param yarnClient Client to talk to YARN
	 * @param appReport the YARN application ID
	 * @param flinkConfig Flink configuration
	 * @param newlyCreatedCluster Indicator whether this cluster has just been created
	 * @throws IOException
	 * @throws YarnException
	 */
	public YarnClusterClient(
		final AbstractYarnClusterDescriptor clusterDescriptor,
		final YarnClient yarnClient,
		final ApplicationReport appReport,
		Configuration flinkConfig,
		boolean newlyCreatedCluster) throws Exception {

		super(flinkConfig);

		this.akkaDuration = AkkaUtils.getTimeout(flinkConfig);
		this.clusterDescriptor = clusterDescriptor;
		this.yarnClient = yarnClient;
		this.appReport = appReport;
		this.appId = appReport.getApplicationId();
		this.trackingURL = appReport.getTrackingUrl();
		this.newlyCreatedCluster = newlyCreatedCluster;

		this.applicationClient = new LazApplicationClientLoader(
			flinkConfig,
			actorSystemLoader,
			highAvailabilityServices);

		this.pollingRunner = new PollingThread(yarnClient, appId);
		this.pollingRunner.setDaemon(true);
		this.pollingRunner.start();

		Runtime.getRuntime().addShutdownHook(clientShutdownHook);
	}

	/**
	 * Disconnect from the Yarn cluster.
	 */
	public void disconnect() {

		if (hasBeenShutDown.getAndSet(true)) {
			return;
		}

		if (!isConnected) {
			throw new IllegalStateException("Can not disconnect from an unconnected cluster.");
		}

		LOG.info("Disconnecting YarnClusterClient from ApplicationMaster");

		try {
			Runtime.getRuntime().removeShutdownHook(clientShutdownHook);
		} catch (IllegalStateException e) {
			// we are already in the shutdown hook
		}

		try {
			pollingRunner.stopRunner();
			pollingRunner.join(1000);
		} catch (InterruptedException e) {
			LOG.warn("Shutdown of the polling runner was interrupted", e);
			Thread.currentThread().interrupt();
		}

		isConnected = false;
	}

	// -------------------------- Interaction with the cluster ------------------------

	/*
	 * Tells the Cluster to monitor the status of JobId and stop itself once the specified job has finished.
	 */
	private void stopAfterJob(JobID jobID) {
		Preconditions.checkNotNull(jobID, "The job id must not be null");
		try {
			Future<Object> replyFuture =
				getJobManagerGateway().ask(
					new ShutdownClusterAfterJob(jobID),
					akkaDuration);
			Await.ready(replyFuture, akkaDuration);
		} catch (Exception e) {
			throw new RuntimeException("Unable to tell application master to stop once the specified job has been finised", e);
		}
	}

	@Override
	public org.apache.flink.configuration.Configuration getFlinkConfiguration() {
		return flinkConfig;
	}

	@Override
	public int getMaxSlots() {
		int maxSlots = clusterDescriptor.getTaskManagerCount() * clusterDescriptor.getTaskManagerSlots();
		return maxSlots > 0 ? maxSlots : -1;
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return clusterDescriptor.hasUserJarFiles(userJarFiles);
	}

	@Override
	protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		if (isDetached()) {
			if (newlyCreatedCluster) {
				stopAfterJob(jobGraph.getJobID());
			}
			return super.runDetached(jobGraph, classLoader);
		} else {
			return super.run(jobGraph, classLoader);
		}
	}

	@Override
	public String getWebInterfaceURL() {
		// there seems to be a difference between HD 2.2.0 and 2.6.0
		if (!trackingURL.startsWith("http://")) {
			return "http://" + trackingURL;
		} else {
			return trackingURL;
		}
	}

	@Override
	public String getClusterIdentifier() {
		return "Yarn cluster with application id " + appReport.getApplicationId();
	}

	/**
	 * This method is only available if the cluster hasn't been started in detached mode.
	 */
	@Override
	public GetClusterStatusResponse getClusterStatus() {
		if (!isConnected) {
			throw new IllegalStateException("The cluster is not connected to the cluster.");
		}
		if (hasBeenShutdown()) {
			throw new IllegalStateException("The cluster has already been shutdown.");
		}

		try {
			final Future<Object> clusterStatusOption =
				getJobManagerGateway().ask(
					GetClusterStatus.getInstance(),
					akkaDuration);
			return (GetClusterStatusResponse) Await.result(clusterStatusOption, akkaDuration);
		} catch (Exception e) {
			throw new RuntimeException("Unable to get ClusterClient status from Application Client", e);
		}
	}

	public ApplicationStatus getApplicationStatus() {
		if (!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}
		ApplicationReport lastReport = null;
		if (pollingRunner == null) {
			LOG.warn("YarnClusterClient.getApplicationStatus() has been called on an uninitialized cluster." +
					"The system might be in an erroneous state");
		} else {
			lastReport = pollingRunner.getLastReport();
		}
		if (lastReport == null) {
			LOG.warn("YarnClusterClient.getApplicationStatus() has been called on a cluster that didn't receive a status so far." +
					"The system might be in an erroneous state");
			return ApplicationStatus.UNKNOWN;
		} else {
			YarnApplicationState appState = lastReport.getYarnApplicationState();
			ApplicationStatus status =
				(appState == YarnApplicationState.FAILED || appState == YarnApplicationState.KILLED) ?
					ApplicationStatus.FAILED : ApplicationStatus.SUCCEEDED;
			if (status != ApplicationStatus.SUCCEEDED) {
				LOG.warn("YARN reported application state {}", appState);
				LOG.warn("Diagnostics: {}", lastReport.getDiagnostics());
			}
			return status;
		}
	}

	@Override
	public List<String> getNewMessages() {

		if (hasBeenShutdown()) {
			throw new RuntimeException("The YarnClusterClient has already been stopped");
		}

		if (!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}

		List<String> ret = new ArrayList<String>();
		// get messages from ApplicationClient (locally)
		while (true) {
			Object result;
			try {
				Future<Object> response =
					Patterns.ask(
						applicationClient.get(),
						YarnMessages.getLocalGetYarnMessage(),
						new Timeout(akkaDuration));
				result = Await.result(response, akkaDuration);
			} catch (Exception ioe) {
				LOG.warn("Error retrieving the YARN messages locally", ioe);
				break;
			}

			if (!(result instanceof Option)) {
				throw new RuntimeException("LocalGetYarnMessage requires a response of type " +
						"Option. Instead the response is of type " + result.getClass() + ".");
			} else {
				Option messageOption = (Option) result;
				LOG.debug("Received message option {}", messageOption);
				if (messageOption.isEmpty()) {
					break;
				} else {
					Object obj = messageOption.get();

					if (obj instanceof InfoMessage) {
						InfoMessage msg = (InfoMessage) obj;
						ret.add("[" + msg.date() + "] " + msg.message());
					} else {
						LOG.warn("LocalGetYarnMessage returned unexpected type: " + messageOption);
					}
				}
			}
		}
		return ret;
	}

	// -------------------------- Shutdown handling ------------------------

	private AtomicBoolean hasBeenShutDown = new AtomicBoolean(false);

	/**
	 * Shuts down or disconnects from the YARN cluster.
	 */
	@Override
	public void finalizeCluster() {
		if (isDetached() || !newlyCreatedCluster) {
			disconnect();
		} else {
			shutdownCluster();
		}
	}

	/**
	 * Shuts down the Yarn application.
	 */
	public void shutdownCluster() {

		if (hasBeenShutDown.getAndSet(true)) {
			return;
		}

		if (!isConnected) {
			throw new IllegalStateException("The cluster has been not been connected to the ApplicationMaster.");
		}

		try {
			Runtime.getRuntime().removeShutdownHook(clientShutdownHook);
		} catch (IllegalStateException e) {
			// we are already in the shutdown hook
		}

		LOG.info("Sending shutdown request to the Application Master");
		try {
			Future<Object> response =
				Patterns.ask(applicationClient.get(),
					new YarnMessages.LocalStopYarnSession(getApplicationStatus(),
							"Flink YARN Client requested shutdown"),
					new Timeout(akkaDuration));
			Await.ready(response, akkaDuration);
		} catch (Exception e) {
			LOG.warn("Error while stopping YARN cluster.", e);
		}

		try {
			File propertiesFile = FlinkYarnSessionCli.getYarnPropertiesLocation(flinkConfig);
			if (propertiesFile.isFile()) {
				if (propertiesFile.delete()) {
					LOG.info("Deleted Yarn properties file at {}", propertiesFile.getAbsoluteFile().toString());
				} else {
					LOG.warn("Couldn't delete Yarn properties file at {}", propertiesFile.getAbsoluteFile().toString());
				}
			}
		} catch (Exception e) {
			LOG.warn("Exception while deleting the JobManager address file", e);
		}

		try {
			pollingRunner.stopRunner();
			pollingRunner.join(1000);
		} catch (InterruptedException e) {
			LOG.warn("Shutdown of the polling runner was interrupted", e);
			Thread.currentThread().interrupt();
		}

		try {
			ApplicationReport appReport = yarnClient.getApplicationReport(appId);

			LOG.info("Application " + appId + " finished with state " + appReport
				.getYarnApplicationState() + " and final state " + appReport
				.getFinalApplicationStatus() + " at " + appReport.getFinishTime());

			if (appReport.getYarnApplicationState() == YarnApplicationState.FAILED || appReport.getYarnApplicationState()
				== YarnApplicationState.KILLED) {
				LOG.warn("Application failed. Diagnostics " + appReport.getDiagnostics());
				LOG.warn("If log aggregation is activated in the Hadoop cluster, we recommend to retrieve "
					+ "the full application log using this command:"
					+ System.lineSeparator()
					+ "\tyarn logs -applicationId " + appReport.getApplicationId()
					+ System.lineSeparator()
					+ "(It sometimes takes a few seconds until the logs are aggregated)");
			}
		} catch (Exception e) {
			LOG.warn("Couldn't get final report", e);
		}

		LOG.info("YARN Client is shutting down");
		yarnClient.stop(); // actorRunner is using the yarnClient.
		yarnClient = null; // set null to clearly see if somebody wants to access it afterwards.
	}

	public boolean hasBeenShutdown() {
		return hasBeenShutDown.get();
	}

	private class ClientShutdownHook extends Thread {
		@Override
		public void run() {
			LOG.info("Shutting down YarnClusterClient from the client shutdown hook");

			try {
				shutdown();
			} catch (Throwable t) {
				LOG.warn("Could not properly shut down the yarn cluster client.", t);
			}
		}
	}

	// -------------------------- Polling ------------------------

	private static class PollingThread extends Thread {

		AtomicBoolean running = new AtomicBoolean(true);
		private YarnClient yarnClient;
		private ApplicationId appId;

		// ------- status information stored in the polling thread
		private final Object lock = new Object();
		private ApplicationReport lastReport;

		public PollingThread(YarnClient yarnClient, ApplicationId appId) {
			this.yarnClient = yarnClient;
			this.appId = appId;
		}

		public void stopRunner() {
			if (!running.get()) {
				LOG.warn("Polling thread was already stopped");
			}
			running.set(false);
		}

		public ApplicationReport getLastReport() {
			synchronized (lock) {
				return lastReport;
			}
		}

		@Override
		public void run() {
			while (running.get() && yarnClient.isInState(Service.STATE.STARTED)) {
				try {
					ApplicationReport report = yarnClient.getApplicationReport(appId);
					synchronized (lock) {
						lastReport = report;
					}
				} catch (Exception e) {
					LOG.warn("Error while getting application report", e);
				}
				try {
					Thread.sleep(YarnClusterClient.POLLING_THREAD_INTERVAL_MS);
				} catch (InterruptedException e) {
					LOG.error("Polling thread got interrupted", e);
					Thread.currentThread().interrupt(); // pass interrupt.
					stopRunner();
				}
			}
			if (running.get() && !yarnClient.isInState(Service.STATE.STARTED)) {
				// == if the polling thread is still running but the yarn client is stopped.
				LOG.warn("YARN client is unexpected in state " + yarnClient.getServiceState());
			}
		}
	}

	@Override
	public boolean isDetached() {
		return super.isDetached() || clusterDescriptor.isDetachedMode();
	}

	/**
	 * Blocks until all TaskManagers are connected to the JobManager.
	 */
	@Override
	public void waitForClusterToBeReady() {
		logAndSysout("Waiting until all TaskManagers have connected");

		for (GetClusterStatusResponse currentStatus, lastStatus = null;; lastStatus = currentStatus) {
			currentStatus = getClusterStatus();
			if (currentStatus != null && !currentStatus.equals(lastStatus)) {
				logAndSysout("TaskManager status (" + currentStatus.numRegisteredTaskManagers() + "/"
					+ clusterDescriptor.getTaskManagerCount() + ")");
				if (currentStatus.numRegisteredTaskManagers() >= clusterDescriptor.getTaskManagerCount()) {
					logAndSysout("All TaskManagers are connected");
					break;
				}
			} else if (lastStatus == null) {
				logAndSysout("No status updates from the YARN cluster received so far. Waiting ...");
			}

			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted while waiting for TaskManagers", e);
			}
		}
	}

	public ApplicationId getApplicationId() {
		return appId;
	}

	private static class LazApplicationClientLoader {

		private final org.apache.flink.configuration.Configuration flinkConfig;
		private final LazyActorSystemLoader actorSystemLoader;
		private final HighAvailabilityServices highAvailabilityServices;

		private ActorRef applicationClient;

		private LazApplicationClientLoader(
				org.apache.flink.configuration.Configuration flinkConfig,
				LazyActorSystemLoader actorSystemLoader,
				HighAvailabilityServices highAvailabilityServices) {
			this.flinkConfig = Preconditions.checkNotNull(flinkConfig, "flinkConfig");
			this.actorSystemLoader = Preconditions.checkNotNull(actorSystemLoader, "actorSystemLoader");
			this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices, "highAvailabilityServices");
		}

		/**
		 * Creates a new ApplicationClient actor or returns an existing one. May start an ActorSystem.
		 * @return ActorSystem
		 */
		public ActorRef get() throws FlinkException {
			if (applicationClient == null) {
				// start application client
				LOG.info("Start application client.");

				final ActorSystem actorSystem;

				try {
					actorSystem = actorSystemLoader.get();
				} catch (FlinkException fle) {
					throw new FlinkException("Could not start the ClusterClient's ActorSystem.", fle);
				}

				try {
					applicationClient = actorSystem.actorOf(
						Props.create(
							ApplicationClient.class,
							flinkConfig,
							highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID)),
						"applicationClient");
				} catch (Exception e) {
					throw new FlinkException("Could not start the ApplicationClient.", e);
				}
			}

			return applicationClient;
		}
	}
}
