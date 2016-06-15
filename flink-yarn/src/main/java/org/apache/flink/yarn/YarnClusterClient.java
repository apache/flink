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

import akka.actor.ActorRef;

import static akka.pattern.Patterns.ask;

import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None$;
import scala.Option;
import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java representation of a running Flink cluster within YARN.
 */
public class YarnClusterClient extends ClusterClient {

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClient.class);

	private static final int POLLING_THREAD_INTERVAL_MS = 1000;

	private YarnClient yarnClient;
	private Thread actorRunner;
	private Thread clientShutdownHook = new ClientShutdownHook();
	private PollingThread pollingRunner;
	private final Configuration hadoopConfig;
	// (HDFS) location of the files required to run on YARN. Needed here to delete them on shutdown.
	private final Path sessionFilesDir;

	/** The leader retrieval service for connecting to the cluster and finding the active leader. */
	private final LeaderRetrievalService leaderRetrievalService;

	//---------- Class internal fields -------------------

	private final AbstractYarnClusterDescriptor clusterDescriptor;
	private final ActorRef applicationClient;
	private final FiniteDuration akkaDuration;
	private final Timeout akkaTimeout;
	private final ApplicationReport applicationId;
	private final ApplicationId appId;
	private final String trackingURL;

	private boolean isConnected = false;


	/**
	 * Create a new Flink on YARN cluster.
	 *
	 * @param clusterDescriptor The descriptor used at cluster creation
	 * @param yarnClient Client to talk to YARN
	 * @param appReport the YARN application ID
	 * @param flinkConfig Flink configuration
	 * @param sessionFilesDir Location of files required for YARN session
	 * @throws IOException
	 * @throws YarnException
	 */
	public YarnClusterClient(
		final AbstractYarnClusterDescriptor clusterDescriptor,
		final YarnClient yarnClient,
		final ApplicationReport appReport,
		org.apache.flink.configuration.Configuration flinkConfig,
		Path sessionFilesDir) throws IOException, YarnException {

		super(flinkConfig);

		this.akkaDuration = AkkaUtils.getTimeout(flinkConfig);
		this.akkaTimeout = Timeout.durationToTimeout(akkaDuration);
		this.clusterDescriptor = clusterDescriptor;
		this.yarnClient = yarnClient;
		this.hadoopConfig = yarnClient.getConfig();
		this.sessionFilesDir = sessionFilesDir;
		this.applicationId = appReport;
		this.appId = appReport.getApplicationId();
		this.trackingURL = appReport.getTrackingUrl();

		try {
			leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(flinkConfig);
		} catch (Exception e) {
			throw new IOException("Could not create the leader retrieval service.", e);
		}


		if (isConnected) {
			throw new IllegalStateException("Already connected to the cluster.");
		}

		// start application client
		LOG.info("Start application client.");

		applicationClient = actorSystem.actorOf(
			Props.create(
				ApplicationClient.class,
				flinkConfig,
				leaderRetrievalService),
			"applicationClient");

		actorRunner = new Thread(new Runnable() {
			@Override
			public void run() {
				// blocks until ApplicationClient has been stopped
				actorSystem.awaitTermination();

				// get final application report
				try {
					ApplicationReport appReport = yarnClient.getApplicationReport(appId);

					LOG.info("Application " + appId + " finished with state " + appReport
						.getYarnApplicationState() + " and final state " + appReport
						.getFinalApplicationStatus() + " at " + appReport.getFinishTime());

					if (appReport.getYarnApplicationState() == YarnApplicationState.FAILED || appReport.getYarnApplicationState()
						== YarnApplicationState.KILLED) {
						LOG.warn("Application failed. Diagnostics " + appReport.getDiagnostics());
						LOG.warn("If log aggregation is activated in the Hadoop cluster, we recommend to retrieve "
							+ "the full application log using this command:\n"
							+ "\tyarn logs -applicationId " + appReport.getApplicationId() + "\n"
							+ "(It sometimes takes a few seconds until the logs are aggregated)");
					}
				} catch (Exception e) {
					LOG.warn("Error while getting final application report", e);
				}
			}
		});
		actorRunner.setDaemon(true);
		actorRunner.start();

		pollingRunner = new PollingThread(yarnClient, appId);
		pollingRunner.setDaemon(true);
		pollingRunner.start();

		Runtime.getRuntime().addShutdownHook(clientShutdownHook);

		isConnected = true;

		logAndSysout("Waiting until all TaskManagers have connected");

		while(true) {
			GetClusterStatusResponse status = getClusterStatus();
			if (status != null) {
				if (status.numRegisteredTaskManagers() < clusterDescriptor.getTaskManagerCount()) {
					logAndSysout("TaskManager status (" + status.numRegisteredTaskManagers() + "/"
						+ clusterDescriptor.getTaskManagerCount() + ")");
				} else {
					logAndSysout("All TaskManagers are connected");
					break;
				}
			} else {
				logAndSysout("No status updates from the YARN cluster received so far. Waiting ...");
			}

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				LOG.error("Interrupted while waiting for TaskManagers");
				System.err.println("Thread is interrupted");
				throw new IOException("Interrupted while waiting for TaskManagers", e);
			}
		}
	}

	public void disconnect() {
		if(!isConnected) {
			throw new IllegalStateException("Can not disconnect from an unconnected cluster.");
		}
		LOG.info("Disconnecting YarnClusterClient from ApplicationMaster");

		if(!Runtime.getRuntime().removeShutdownHook(clientShutdownHook)) {
			LOG.warn("Error while removing the shutdown hook. The YARN session might be killed unintentionally");
		}
		// tell the actor to shut down.
		applicationClient.tell(PoisonPill.getInstance(), applicationClient);

		try {
			actorRunner.join(1000); // wait for 1 second
		} catch (InterruptedException e) {
			LOG.warn("Shutdown of the actor runner was interrupted", e);
			Thread.currentThread().interrupt();
		}
		try {
			pollingRunner.stopRunner();
			pollingRunner.join(1000);
		} catch(InterruptedException e) {
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
		Future<Object> messageReceived = ask(applicationClient, new YarnMessages.LocalStopAMAfterJob(jobID), akkaTimeout);
		try {
			Await.result(messageReceived, akkaDuration);
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
	protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		if (isDetached()) {
			JobSubmissionResult result = super.runDetached(jobGraph, classLoader);
			stopAfterJob(jobGraph.getJobID());
			return result;
		} else {
			return super.run(jobGraph, classLoader);
		}
	}

	@Override
	public String getWebInterfaceURL() {
		// there seems to be a difference between HD 2.2.0 and 2.6.0
		if(!trackingURL.startsWith("http://")) {
			return "http://" + trackingURL;
		} else {
			return trackingURL;
		}
	}

	@Override
	public String getClusterIdentifier() {
		return applicationId.getApplicationId().toString();
	}

	/**
	 * This method is only available if the cluster hasn't been started in detached mode.
	 */
	@Override
	public GetClusterStatusResponse getClusterStatus() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster is not connected to the ApplicationMaster.");
		}
		if(hasBeenShutdown()) {
			throw new RuntimeException("The YarnClusterClient has already been stopped");
		}
		Future<Object> clusterStatusOption = ask(applicationClient, YarnMessages.getLocalGetyarnClusterStatus(), akkaTimeout);
		Object clusterStatus;
		try {
			clusterStatus = Await.result(clusterStatusOption, akkaDuration);
		} catch (Exception e) {
			throw new RuntimeException("Unable to get ClusterClient status from Application Client", e);
		}
		if(clusterStatus instanceof None$) {
			return null;
		} else if(clusterStatus instanceof Some) {
			return (GetClusterStatusResponse) (((Some) clusterStatus).get());
		} else {
			throw new RuntimeException("Unexpected type: " + clusterStatus.getClass().getCanonicalName());
		}
	}

	public ApplicationStatus getApplicationStatus() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}
		if(pollingRunner == null) {
			LOG.warn("YarnClusterClient.getApplicationStatus() has been called on an uninitialized cluster." +
					"The system might be in an erroneous state");
		}
		ApplicationReport lastReport = pollingRunner.getLastReport();
		if(lastReport == null) {
			LOG.warn("YarnClusterClient.getApplicationStatus() has been called on a cluster that didn't receive a status so far." +
					"The system might be in an erroneous state");
			return ApplicationStatus.UNKNOWN;
		} else {
			YarnApplicationState appState = lastReport.getYarnApplicationState();
			ApplicationStatus status =
				(appState == YarnApplicationState.FAILED || appState == YarnApplicationState.KILLED) ?
					ApplicationStatus.FAILED : ApplicationStatus.SUCCEEDED;
			if(status != ApplicationStatus.SUCCEEDED) {
				LOG.warn("YARN reported application state {}", appState);
				LOG.warn("Diagnostics: {}", lastReport.getDiagnostics());
			}
			return status;
		}
	}


	private String getDiagnostics() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}

		if (getApplicationStatus() == ApplicationStatus.SUCCEEDED) {
			LOG.warn("getDiagnostics() called for cluster which is not in failed state");
		}
		ApplicationReport lastReport = pollingRunner.getLastReport();
		if (lastReport == null) {
			LOG.warn("Last report is null");
			return null;
		} else {
			return lastReport.getDiagnostics();
		}
	}

	@Override
	public List<String> getNewMessages() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}

		if(hasBeenShutdown()) {
			throw new RuntimeException("The YarnClusterClient has already been stopped");
		}
		List<String> ret = new ArrayList<String>();

		// get messages from ApplicationClient (locally)
		while(true) {
			Object result;
			try {
				Future<Object> response = Patterns.ask(applicationClient,
						YarnMessages.getLocalGetYarnMessage(), new Timeout(akkaDuration));

				result = Await.result(response, akkaDuration);
			} catch(Exception ioe) {
				LOG.warn("Error retrieving the YARN messages locally", ioe);
				break;
			}

			if(!(result instanceof Option)) {
				throw new RuntimeException("LocalGetYarnMessage requires a response of type " +
						"Option. Instead the response is of type " + result.getClass() + ".");
			} else {
				Option messageOption = (Option) result;
				LOG.debug("Received message option {}", messageOption);
				if(messageOption.isEmpty()) {
					break;
				} else {
					Object obj = messageOption.get();

					if(obj instanceof InfoMessage) {
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

		if (!isConnected) {
			throw new IllegalStateException("The cluster has been not been connected to the ApplicationMaster.");
		}

		if (isDetached()) {
			// only disconnect if we are running detached
			disconnect();
			return;
		}

		// show cluster status

		List<String> msgs = getNewMessages();
		if (msgs != null && msgs.size() > 1) {

			logAndSysout("The following messages were created by the YARN cluster while running the Job:");
			for (String msg : msgs) {
				logAndSysout(msg);
			}
		}
		if (getApplicationStatus() != ApplicationStatus.SUCCEEDED) {
			logAndSysout("YARN cluster is in non-successful state " + getApplicationStatus());
			logAndSysout("YARN Diagnostics: " + getDiagnostics());
		}


		if(hasBeenShutDown.getAndSet(true)) {
			return;
		}

		try {
			Runtime.getRuntime().removeShutdownHook(clientShutdownHook);
		} catch (IllegalStateException e) {
			// we are already in the shutdown hook
		}

		if(actorSystem != null){
			LOG.info("Sending shutdown request to the Application Master");
			if(applicationClient != ActorRef.noSender()) {
				try {
					Future<Object> response = Patterns.ask(applicationClient,
							new YarnMessages.LocalStopYarnSession(getApplicationStatus(),
									"Flink YARN Client requested shutdown"),
							new Timeout(akkaDuration));
					Await.ready(response, akkaDuration);
				} catch(Exception e) {
					LOG.warn("Error while stopping YARN Application Client", e);
				}
			}

			actorSystem.shutdown();
			actorSystem.awaitTermination();
		}

		LOG.info("Deleting files in " + sessionFilesDir);
		try {
			FileSystem shutFS = FileSystem.get(hadoopConfig);
			shutFS.delete(sessionFilesDir, true); // delete conf and jar file.
			shutFS.close();
		}catch(IOException e){
			LOG.error("Could not delete the Flink jar and configuration files in HDFS..", e);
		}

		try {
			actorRunner.join(1000); // wait for 1 second
		} catch (InterruptedException e) {
			LOG.warn("Shutdown of the actor runner was interrupted", e);
			Thread.currentThread().interrupt();
		}
		try {
			pollingRunner.stopRunner();
			pollingRunner.join(1000);
		} catch(InterruptedException e) {
			LOG.warn("Shutdown of the polling runner was interrupted", e);
			Thread.currentThread().interrupt();
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
			shutdown();
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
			if(!running.get()) {
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
			if(running.get() && !yarnClient.isInState(Service.STATE.STARTED)) {
				// == if the polling thread is still running but the yarn client is stopped.
				LOG.warn("YARN client is unexpected in state " + yarnClient.getServiceState());
			}
		}
	}

	@Override
	public boolean isDetached() {
		// either we have set detached mode using the general '-d' flag or using the Yarn CLI flag 'yd'
		return super.isDetached() || clusterDescriptor.isDetachedMode();
	}
}
