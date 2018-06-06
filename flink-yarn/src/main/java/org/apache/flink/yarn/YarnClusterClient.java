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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Java representation of a running Flink cluster within YARN.
 */
public class YarnClusterClient extends ClusterClient<ApplicationId> {

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClient.class);

	//---------- Class internal fields -------------------

	private final AbstractYarnClusterDescriptor clusterDescriptor;
	private final int numberTaskManagers;
	private final int slotsPerTaskManager;
	private final LazApplicationClientLoader applicationClient;
	private final FiniteDuration akkaDuration;
	private final ApplicationId appId;
	private final String trackingURL;

	/** Indicator whether this cluster has just been created. */
	private final boolean newlyCreatedCluster;

	/**
	 * Create a new Flink on YARN cluster.
	 *
	 * @param clusterDescriptor The descriptor used at cluster creation
	 * @param numberTaskManagers The number of task managers, -1 if unknown
	 * @param slotsPerTaskManager Slots per task manager, -1 if unknown
	 * @param appReport the YARN application ID
	 * @param flinkConfig Flink configuration
	 * @param newlyCreatedCluster Indicator whether this cluster has just been created
	 * @throws IOException
	 * @throws YarnException
	 */
	public YarnClusterClient(
		final AbstractYarnClusterDescriptor clusterDescriptor,
		final int numberTaskManagers,
		final int slotsPerTaskManager,
		final ApplicationReport appReport,
		Configuration flinkConfig,
		boolean newlyCreatedCluster) throws Exception {

		super(flinkConfig);

		this.akkaDuration = AkkaUtils.getTimeout(flinkConfig);
		this.clusterDescriptor = clusterDescriptor;
		this.numberTaskManagers = numberTaskManagers;
		this.slotsPerTaskManager = slotsPerTaskManager;
		this.appId = appReport.getApplicationId();
		this.trackingURL = appReport.getTrackingUrl();
		this.newlyCreatedCluster = newlyCreatedCluster;

		this.applicationClient = new LazApplicationClientLoader(
			flinkConfig,
			actorSystemLoader,
			highAvailabilityServices);
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
		// TODO: this should be retrieved from the running Flink cluster
		int maxSlots = numberTaskManagers * slotsPerTaskManager;
		return maxSlots > 0 ? maxSlots : MAX_SLOTS_UNKNOWN;
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return clusterDescriptor.hasUserJarFiles(userJarFiles);
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
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

	/**
	 * This method is only available if the cluster hasn't been started in detached mode.
	 */
	@Override
	public GetClusterStatusResponse getClusterStatus() {
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

	@Override
	public List<String> getNewMessages() {

		List<String> ret = new ArrayList<>();
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

	@Override
	public ApplicationId getClusterId() {
		return appId;
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

		for (GetClusterStatusResponse currentStatus, lastStatus = null; true; lastStatus = currentStatus) {
			currentStatus = getClusterStatus();
			if (currentStatus != null && !currentStatus.equals(lastStatus)) {
				logAndSysout("TaskManager status (" + currentStatus.numRegisteredTaskManagers() + "/"
					+ numberTaskManagers + ")");
				if (currentStatus.numRegisteredTaskManagers() >= numberTaskManagers) {
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

	@Override
	public void shutDownCluster() {
		LOG.info("Sending shutdown request to the Application Master");
		try {
			final Future<Object> response = Patterns.ask(applicationClient.get(),
				new YarnMessages.LocalStopYarnSession(ApplicationStatus.SUCCEEDED,
					"Flink YARN Client requested shutdown"),
				new Timeout(akkaDuration));
			Await.ready(response, akkaDuration);
		} catch (final Exception e) {
			LOG.warn("Error while stopping YARN cluster.", e);
		}
	}

	public ApplicationId getApplicationId() {
		return appId;
	}

	private static class LazApplicationClientLoader {

		private final Configuration flinkConfig;
		private final LazyActorSystemLoader actorSystemLoader;
		private final HighAvailabilityServices highAvailabilityServices;

		private ActorRef applicationClient;

		private LazApplicationClientLoader(
				Configuration flinkConfig,
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
