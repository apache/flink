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
import akka.actor.ActorSystem;

import static akka.pattern.Patterns.ask;

import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None$;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java representation of a running Flink cluster within YARN.
 */
public class FlinkYarnCluster extends AbstractFlinkYarnCluster {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnCluster.class);

	private static final int POLLING_THREAD_INTERVAL_MS = 1000;

	private YarnClient yarnClient;
	private Thread actorRunner;
	private Thread clientShutdownHook = new ClientShutdownHook();
	private PollingThread pollingRunner;
	private final Configuration hadoopConfig;
	// (HDFS) location of the files required to run on YARN. Needed here to delete them on shutdown.
	private final Path sessionFilesDir;
	private final InetSocketAddress jobManagerAddress;

	//---------- Class internal fields -------------------

	private ActorSystem actorSystem;
	private ActorRef applicationClient;
	private ApplicationReport intialAppReport;
	private final FiniteDuration akkaDuration;
	private final Timeout akkaTimeout;
	private final ApplicationId applicationId;
	private final boolean detached;
	private final org.apache.flink.configuration.Configuration flinkConfig;
	private final ApplicationId appId;

	private boolean isConnected = false;


	/**
	 * Create a new Flink on YARN cluster.
	 *
	 * @param yarnClient
	 * @param appId the YARN application ID
	 * @param hadoopConfig
	 * @param flinkConfig
	 * @param sessionFilesDir
	 * @param detached Set to true if no actor system or RPC communication with the cluster should be established
	 * @throws IOException
	 * @throws YarnException
	 */
	public FlinkYarnCluster(
			final YarnClient yarnClient,
			final ApplicationId appId,
			Configuration hadoopConfig,
			org.apache.flink.configuration.Configuration flinkConfig,
			Path sessionFilesDir,
			boolean detached) throws IOException, YarnException {
		this.akkaDuration = AkkaUtils.getTimeout(flinkConfig);
		this.akkaTimeout = Timeout.durationToTimeout(akkaDuration);
		this.yarnClient = yarnClient;
		this.hadoopConfig = hadoopConfig;
		this.sessionFilesDir = sessionFilesDir;
		this.applicationId = appId;
		this.detached = detached;
		this.flinkConfig = flinkConfig;
		this.appId = appId;

		// get one application report manually
		intialAppReport = yarnClient.getApplicationReport(appId);
		String jobManagerHost = intialAppReport.getHost();
		int jobManagerPort = intialAppReport.getRpcPort();
		this.jobManagerAddress = new InetSocketAddress(jobManagerHost, jobManagerPort);
	}

	/**
	 * Connect the FlinkYarnCluster to the ApplicationMaster.
	 *
	 * Detached YARN sessions don't need to connect to the ApplicationMaster.
	 * Detached per job YARN sessions need to connect until the required number of TaskManagers have been started.
	 * 
	 * @throws IOException
	 */
	public void connectToCluster() throws IOException {
		if(isConnected) {
			throw new IllegalStateException("Can not connect to the cluster again");
		}

		// start actor system
		LOG.info("Start actor system.");
		// find name of own public interface, able to connect to the JM
		// try to find address for 2 seconds. log after 400 ms.
		InetAddress ownHostname = ConnectionUtils.findConnectingAddress(jobManagerAddress, 2000, 400);
		actorSystem = AkkaUtils.createActorSystem(flinkConfig,
				new Some(new Tuple2<String, Integer>(ownHostname.getCanonicalHostName(), 0)));

		// Create the leader election service
		flinkConfig.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, this.jobManagerAddress.getHostName());
		flinkConfig.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, this.jobManagerAddress.getPort());

		LeaderRetrievalService leaderRetrievalService;

		try {
			leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(flinkConfig);
		} catch (Exception e) {
			throw new IOException("Could not create the leader retrieval service.", e);
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
	}

	@Override
	public void disconnect() {
		if(!isConnected) {
			throw new IllegalStateException("Can not disconnect from an unconnected cluster.");
		}
		LOG.info("Disconnecting FlinkYarnCluster from ApplicationMaster");

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
	 * This call blocks until the message has been recevied.
	 */
	@Override
	public void stopAfterJob(JobID jobID) {
		Preconditions.checkNotNull("The job id must not be null", jobID);
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
	public InetSocketAddress getJobManagerAddress() {
		return jobManagerAddress;
	}

	@Override
	public String getWebInterfaceURL() {
		String url = this.intialAppReport.getTrackingUrl();
		// there seems to be a difference between HD 2.2.0 and 2.6.0
		if(!url.startsWith("http://")) {
			url = "http://" + url;
		}
		return url;
	}

	@Override
	public String getApplicationId() {
		return applicationId.toString();
	}

	@Override
	public boolean isDetached() {
		return this.detached;
	}

	/**
	 * This method is only available if the cluster hasn't been started in detached mode.
	 */
	@Override
	public FlinkYarnClusterStatus getClusterStatus() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster is not connected to the ApplicationMaster.");
		}
		if(hasBeenStopped()) {
			throw new RuntimeException("The FlinkYarnCluster has already been stopped");
		}
		Future<Object> clusterStatusOption = ask(applicationClient, YarnMessages.getLocalGetyarnClusterStatus(), akkaTimeout);
		Object clusterStatus;
		try {
			clusterStatus = Await.result(clusterStatusOption, akkaDuration);
		} catch (Exception e) {
			throw new RuntimeException("Unable to get Cluster status from Application Client", e);
		}
		if(clusterStatus instanceof None$) {
			return null;
		} else if(clusterStatus instanceof Some) {
			return (FlinkYarnClusterStatus) (((Some) clusterStatus).get());
		} else {
			throw new RuntimeException("Unexpected type: " + clusterStatus.getClass().getCanonicalName());
		}
	}

	@Override
	public boolean hasFailed() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}
		if(pollingRunner == null) {
			LOG.warn("FlinkYarnCluster.hasFailed() has been called on an uninitialized cluster." +
					"The system might be in an erroneous state");
		}
		ApplicationReport lastReport = pollingRunner.getLastReport();
		if(lastReport == null) {
			LOG.warn("FlinkYarnCluster.hasFailed() has been called on a cluster that didn't receive a status so far." +
					"The system might be in an erroneous state");
			return false;
		} else {
			YarnApplicationState appState = lastReport.getYarnApplicationState();
			boolean status = (appState == YarnApplicationState.FAILED ||
					appState == YarnApplicationState.KILLED);
			if(status) {
				LOG.warn("YARN reported application state {}", appState);
				LOG.warn("Diagnostics: {}", lastReport.getDiagnostics());
			}
			return status;
		}
	}


	@Override
	public String getDiagnostics() {
		if(!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
		}

		if (!hasFailed()) {
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

		if(hasBeenStopped()) {
			throw new RuntimeException("The FlinkYarnCluster has already been stopped");
		}
		List<String> ret = new ArrayList<String>();

		// get messages from ApplicationClient (locally)
		while(true) {
			Object result = null;
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

					if(obj instanceof YarnMessages.YarnMessage) {
						YarnMessages.YarnMessage msg = (YarnMessages.YarnMessage) obj;
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
	 * Shutdown the YARN cluster.
	 * @param failApplication whether we should fail the YARN application (in case of errors in Flink)
	 */
	@Override
	public void shutdown(boolean failApplication) {
		if(!isConnected) {
			throw new IllegalStateException("The cluster has been connected to the ApplicationMaster.");
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
					FinalApplicationStatus finalStatus;
					if (failApplication) {
						finalStatus = FinalApplicationStatus.FAILED;
					} else {
						finalStatus = FinalApplicationStatus.SUCCEEDED;
					}
					Future<Object> response = Patterns.ask(applicationClient,
							new YarnMessages.LocalStopYarnSession(finalStatus,
									"Flink YARN Client requested shutdown"),
							new Timeout(akkaDuration));

					Await.ready(response, akkaDuration);
				} catch(Exception e) {
					LOG.warn("Error while stopping YARN Application Client", e);
				}
			}

			actorSystem.shutdown();
			actorSystem.awaitTermination();

			actorSystem = null;
		}

		LOG.info("Deleting files in " + sessionFilesDir );
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

	@Override
	public boolean hasBeenStopped() {
		return hasBeenShutDown.get();
	}


	public class ClientShutdownHook extends Thread {
		@Override
		public void run() {
			LOG.info("Shutting down FlinkYarnCluster from the client shutdown hook");
			shutdown(true);
		}
	}

	// -------------------------- Polling ------------------------

	public static class PollingThread extends Thread {

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
					Thread.sleep(FlinkYarnCluster.POLLING_THREAD_INTERVAL_MS);
				} catch (InterruptedException e) {
					LOG.error("Polling thread got interrupted", e);
					Thread.currentThread().interrupt(); // pass interrupt.
				}
			}
			if(running.get() && !yarnClient.isInState(Service.STATE.STARTED)) {
				// == if the polling thread is still running but the yarn client is stopped.
				LOG.warn("YARN client is unexpected in state " + yarnClient.getServiceState());
			}
		}
	}

}
