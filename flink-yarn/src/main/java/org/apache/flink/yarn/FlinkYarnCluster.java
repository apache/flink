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

import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.net.NetUtils;
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

	public FlinkYarnCluster(final YarnClient yarnClient, final ApplicationId appId, Configuration hadoopConfig,
							org.apache.flink.configuration.Configuration flinkConfig,
							Path sessionFilesDir) throws IOException, YarnException {
		this.akkaDuration = AkkaUtils.getTimeout(flinkConfig);
		this.akkaTimeout = Timeout.durationToTimeout(akkaDuration);
		this.yarnClient = yarnClient;
		this.hadoopConfig = hadoopConfig;
		this.sessionFilesDir = sessionFilesDir;

		// get one application report manually
		intialAppReport = yarnClient.getApplicationReport(appId);
		String jobManagerHost = intialAppReport.getHost();
		int jobManagerPort = intialAppReport.getRpcPort();
		this.jobManagerAddress = new InetSocketAddress(jobManagerHost, jobManagerPort);

		// start actor system
		LOG.info("Start actor system.");
		InetAddress ownHostname = NetUtils.resolveAddress(jobManagerAddress); // find name of own public interface, able to connect to the JM
		actorSystem = AkkaUtils.createActorSystem(flinkConfig,
				new Some(new Tuple2<String, Integer>(ownHostname.getCanonicalHostName(), 0)));

		// start application client
		LOG.info("Start application client.");

		applicationClient = actorSystem.actorOf(Props.create(ApplicationClient.class), "applicationClient");

		// instruct ApplicationClient to start a periodical status polling
		applicationClient.tell(new Messages.LocalRegisterClient(this.jobManagerAddress), applicationClient);


		// add hook to ensure proper shutdown
		Runtime.getRuntime().addShutdownHook(clientShutdownHook);

		actorRunner = new Thread(new Runnable() {
			@Override
			public void run() {
				// blocks until ApplicationMaster has been stopped
				actorSystem.awaitTermination();

				// get final application report
				try {
					ApplicationReport appReport = yarnClient.getApplicationReport(appId);

					LOG.info("Application " + appId + " finished with state " + appReport
							.getYarnApplicationState() + " and final state " + appReport
							.getFinalApplicationStatus() + " at " + appReport.getFinishTime());

					if(appReport.getYarnApplicationState() == YarnApplicationState.FAILED || appReport.getYarnApplicationState()
							== YarnApplicationState.KILLED	) {
						LOG.warn("Application failed. Diagnostics "+appReport.getDiagnostics());
						LOG.warn("If log aggregation is activated in the Hadoop cluster, we recommend to retrieve "
								+ "the full application log using this command:\n"
								+ "\tyarn logs -applicationId "+appReport.getApplicationId()+"\n"
								+ "(It sometimes takes a few seconds until the logs are aggregated)");
					}
				} catch(Exception e) {
					LOG.warn("Error while getting final application report", e);
				}
			}
		});
		actorRunner.setDaemon(true);
		actorRunner.start();

		pollingRunner = new PollingThread(yarnClient, appId);
		pollingRunner.setDaemon(true);
		pollingRunner.start();
	}

	// -------------------------- Interaction with the cluster ------------------------

	@Override
	public InetSocketAddress getJobManagerAddress() {
		return jobManagerAddress;
	}

	@Override
	public String getWebInterfaceURL() {
		return this.intialAppReport.getTrackingUrl();
	}


	@Override
	public FlinkYarnClusterStatus getClusterStatus() {
		if(hasBeenStopped()) {
			throw new RuntimeException("The FlinkYarnCluster has alread been stopped");
		}
		Future<Object> clusterStatusOption = ask(applicationClient, Messages.LocalGetYarnClusterStatus$.MODULE$, akkaTimeout);
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
			throw new RuntimeException("Unexpected type: "+clusterStatus.getClass().getCanonicalName());
		}
	}

	@Override
	public boolean hasFailed() {
		if(pollingRunner == null) {
			LOG.warn("FlinkYarnCluster.hasFailed() has been called on an uninitialized cluster." +
					"The system might be in an erroneous state");
		}
		ApplicationReport lastReport = pollingRunner.getLastReport();
		if(lastReport == null) {
			LOG.warn("FlinkYarnCluster.hasFailed() has been called on a cluster. that didn't receive a status so far." +
					"The system might be in an erroneous state");
			return false;
		} else {
			return (lastReport.getYarnApplicationState() == YarnApplicationState.FAILED ||
					lastReport.getYarnApplicationState() == YarnApplicationState.KILLED);
		}
	}

	@Override
	public String getDiagnostics() {
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
		if(hasBeenStopped()) {
			throw new RuntimeException("The FlinkYarnCluster has alread been stopped");
		}
		List<String> ret = new ArrayList<String>();
		// get messages from ApplicationClient (locally)

		while(true) {
			Object result = null;
			try {
				Future<Object> response = Patterns.ask(applicationClient,
						Messages.getLocalGetYarnMessage(), new Timeout(akkaDuration));

				result = Await.result(response, akkaDuration);
			} catch(Exception ioe) {
				LOG.warn("Error retrieving the yarn messages locally", ioe);
			}

			if(!(result instanceof Option)) {
				throw new RuntimeException("LocalGetYarnMessage requires a response of type " +
						"Option. Instead the response is of type " + result.getClass() + ".");
			} else {
				Option messageOption = (Option) result;

				if(messageOption.isEmpty()) {
					break;
				} else {
					Object obj = messageOption.get();

					if(obj instanceof Messages.YarnMessage) {
						Messages.YarnMessage msg = (Messages.YarnMessage) obj;
						ret.add("["+msg.date()+"] "+msg.message());
					} else {
						LOG.warn("LocalGetYarnMessage returned unexpected type: "+messageOption);
					}
				}
			}
		}
		return ret;
	}

	// -------------------------- Shutdown handling ------------------------

	private AtomicBoolean hasBeenShutDown = new AtomicBoolean(false);
	@Override
	public void shutdown() {
		shutdownInternal(true);
	}

	private void shutdownInternal(boolean removeShutdownHook) {
		if(hasBeenShutDown.getAndSet(true)) {
			return;
		}
		// the session is being stopped explicitly.
		if(removeShutdownHook) {
			Runtime.getRuntime().removeShutdownHook(clientShutdownHook);
		}
		if(actorSystem != null){
			LOG.info("Sending shutdown request to the Application Master");
			if(applicationClient != ActorRef.noSender()) {
				try {
					Future<Object> response = Patterns.ask(applicationClient,
							new Messages.StopYarnSession(FinalApplicationStatus.SUCCEEDED),
							new Timeout(akkaDuration));

					Await.ready(response, akkaDuration);
				} catch(Exception e) {
					throw new RuntimeException("Error while stopping YARN Application Client", e);
				}
			}

			actorSystem.shutdown();
			actorSystem.awaitTermination();

			actorSystem = null;
		}

		LOG.info("Deleting files in "+sessionFilesDir );
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
			shutdownInternal(false);
		}
	}

	// -------------------------- Polling ------------------------

	public static class PollingThread extends Thread {

		AtomicBoolean running = new AtomicBoolean(true);
		private YarnClient yarnClient;
		private ApplicationId appId;

		// ------- status information stored in the polling thread
		private Object lock = new Object();
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
					// TODO: do more here.
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
				LOG.warn("YARN client is unexpected in state "+yarnClient.getServiceState());
			}
		}
	}

}
