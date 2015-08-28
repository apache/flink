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

package org.apache.flink.runtime.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Status;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobClientMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

/**
 * The JobClient bridges between the JobManager's asynchronous actor messages and
 * the synchronous method calls to trigger.
 */
public class JobClient {

	private static final Logger LOG = LoggerFactory.getLogger(JobClient.class);


	public static ActorSystem startJobClientActorSystem(Configuration config)
			throws IOException {
		LOG.info("Starting JobClient actor system");
		Option<Tuple2<String, Object>> remoting =
				new Some<Tuple2<String, Object>>(new Tuple2<String, Object>("", 0));

		// start a remote actor system to listen on an arbitrary port
		ActorSystem system = AkkaUtils.createActorSystem(config, remoting);
		Address address = system.provider().getDefaultAddress();

		String host = address.host().isDefined() ? address.host().get() : "(unknown)";
		int port = address.port().isDefined() ? ((Integer) address.port().get()) : -1;
		LOG.info("Started JobClient actor system at " + host + ':' + port);

		return system;
	}

	/**
	 * Extracts the JobManager's Akka URL from the configuration. If localActorSystem is true, then
	 * the JobClient is executed in the same actor system as the JobManager. Thus, they can
	 * communicate locally.
	 *
	 * @param config Configuration object containing all user provided configuration values
	 * @return The socket address of the JobManager actor system
	 */
	public static InetSocketAddress getJobManagerAddress(Configuration config) throws IOException {

		String jobManagerAddress = config.getString(
				ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);

		int jobManagerRPCPort = config.getInteger(
				ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		if (jobManagerAddress == null) {
			throw new RuntimeException(
					"JobManager address has not been specified in the configuration.");
		}

		try {
			return new InetSocketAddress(
					InetAddress.getByName(jobManagerAddress), jobManagerRPCPort);
		}
		catch (UnknownHostException e) {
			throw new IOException("Cannot resolve JobManager hostname " + jobManagerAddress, e);
		}
	}

	/**
	 * Sends a [[JobGraph]] to the JobClient actor specified by jobClient which submits it then to
	 * the JobManager. The method blocks until the job has finished or the JobManager is no longer
	 * alive. In the former case, the [[SerializedJobExecutionResult]] is returned and in the latter
	 * case a [[JobExecutionException]] is thrown.
	 *
	 * @param actorSystem The actor system that performs the communication.
	 * @param jobManagerGateway  Gateway to the JobManager that should execute the job.
	 * @param jobGraph    JobGraph describing the Flink job
	 * @param timeout     Timeout for futures
	 * @return The job execution result
	 * @throws org.apache.flink.runtime.client.JobExecutionException Thrown if the job
	 *                                                               execution fails.
	 */
	public static SerializedJobExecutionResult submitJobAndWait(
			ActorSystem actorSystem,
			ActorGateway jobManagerGateway,
			JobGraph jobGraph,
			FiniteDuration timeout,
			boolean sysoutLogUpdates) throws JobExecutionException {

		Preconditions.checkNotNull(actorSystem, "The actorSystem must not be null.");
		Preconditions.checkNotNull(jobManagerGateway, "The jobManagerGateway must not be null.");
		Preconditions.checkNotNull(jobGraph, "The jobGraph must not be null.");
		Preconditions.checkNotNull(timeout, "The timeout must not be null.");

		// for this job, we create a proxy JobClientActor that deals with all communication with
		// the JobManager. It forwards the job submission, checks the success/failure responses, logs
		// update messages, watches for disconnect between client and JobManager, ...

		Props jobClientActorProps = Props.create(
				JobClientActor.class,
				jobManagerGateway.actor(),
				LOG,
				sysoutLogUpdates,
				jobManagerGateway.leaderSessionID());

		ActorRef jobClientActor = actorSystem.actorOf(jobClientActorProps);

		try {
			Future<Object> future = Patterns.ask(jobClientActor,
					new JobClientMessages.SubmitJobAndWait(jobGraph),
					new Timeout(AkkaUtils.INF_TIMEOUT()));

			Object answer = Await.result(future, AkkaUtils.INF_TIMEOUT());

			if (answer instanceof JobManagerMessages.JobResultSuccess) {
				LOG.info("Job execution complete");

				SerializedJobExecutionResult result = ((JobManagerMessages.JobResultSuccess) answer).result();
				if (result != null) {
					return result;
				} else {
					throw new Exception("Job was successfully executed but result contained a null JobExecutionResult.");
				}
			} else if (answer instanceof Status.Failure) {
				throw ((Status.Failure) answer).cause();
			} else {
				throw new Exception("Unknown answer after submitting the job: " + answer);
			}
		}
		catch (JobExecutionException e) {
			throw e;
		}
		catch (TimeoutException e) {
			throw new JobTimeoutException(jobGraph.getJobID(), "Timeout while waiting for JobManager answer. " +
					"Job time exceeded " + AkkaUtils.INF_TIMEOUT(), e);
		}
		catch (Throwable t) {
			throw new JobExecutionException(jobGraph.getJobID(),
					"Communication with JobManager failed: " + t.getMessage(), t);
		}
		finally {
			// failsafe shutdown of the client actor
			jobClientActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}

	/**
	 * Submits a job in detached mode. The method sends the JobGraph to the
	 * JobManager and waits for the answer whether teh job could be started or not.
	 *
	 * @param jobManagerGateway Gateway to the JobManager which will execute the jobs
	 * @param jobGraph The job
	 * @param timeout  Timeout in which the JobManager must have responded.
	 */
	public static void submitJobDetached(
			ActorGateway jobManagerGateway,
			JobGraph jobGraph,
			FiniteDuration timeout) throws JobExecutionException {

		Preconditions.checkNotNull(jobManagerGateway, "The jobManagerGateway must not be null.");
		Preconditions.checkNotNull(jobGraph, "The jobGraph must not be null.");
		Preconditions.checkNotNull(timeout, "The timeout must not be null.");

		Future<Object> future = jobManagerGateway.ask(
				new JobManagerMessages.SubmitJob(jobGraph, false),
				timeout);

		try {
			Object result = Await.result(future, timeout);
			if (result instanceof JobID) {
				JobID respondedID = (JobID) result;
				if (!respondedID.equals(jobGraph.getJobID())) {
					throw new Exception("JobManager responded for wrong Job. This Job: " +
							jobGraph.getJobID() + ", response: " + respondedID);
				}
			}
			else {
				throw new Exception("Unexpected response: " + result);
			}
		}
		catch (JobExecutionException e) {
			throw e;
		}
		catch (TimeoutException e) {
			throw new JobTimeoutException(jobGraph.getJobID(),
					"JobManager did not respond within " + timeout.toString(), e);
		}
		catch (Throwable t) {
			throw new JobExecutionException(jobGraph.getJobID(),
					"Failed to send job to JobManager: " + t.getMessage(), t.getCause());
		}
	}

	/**
	 * Uploads the specified jar files of the [[JobGraph]] jobGraph to the BlobServer of the
	 * JobManager. The respective port is retrieved from the JobManager. This function issues a
	 * blocking call.
	 *
	 * @param jobGraph   Flink job containing the information about the required jars
	 * @param jobManagerGateway Gateway to the JobManager.
	 * @param timeout    Timeout for futures
	 * @throws IOException Thrown, if the file upload to the JobManager failed.
	 */
	public static void uploadJarFiles(
			JobGraph jobGraph,
			ActorGateway jobManagerGateway,
			FiniteDuration timeout)
			throws IOException {
		if (jobGraph.hasUsercodeJarFiles()) {

			Future<Object> futureBlobPort = jobManagerGateway.ask(
					JobManagerMessages.getRequestBlobManagerPort(),
					timeout);

			int port;
			try {
				Object result = Await.result(futureBlobPort, timeout);
				if (result instanceof Integer) {
					port = (Integer) result;
				} else {
					throw new Exception("Expected port number (int) as answer, received " + result);
				}
			}
			catch (Exception e) {
				throw new IOException("Could not retrieve the JobManager's blob port.", e);
			}

			Option<String> jmHost = jobManagerGateway.actor().path().address().host();
			String jmHostname = jmHost.isDefined() ? jmHost.get() : "localhost";
			InetSocketAddress serverAddress = new InetSocketAddress(jmHostname, port);

			jobGraph.uploadRequiredJarFiles(serverAddress);
		}
	}
}
