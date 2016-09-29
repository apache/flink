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

package org.apache.flink.runtime.jobClient;

import akka.actor.ActorSystem;
import akka.actor.Address;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobRetrievalException;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.Tuple2;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobClientUtils is a utility for client.
 * It offers the following methods:
 * <ul>
 * <li>{@link #startJobClientRpcService(Configuration)} Starts a rpc service for client</li>
 * <li>{@link #retrieveRunningJobResult(JobID, JobMasterGateway, RpcService, LeaderRetrievalService, boolean, Time, Configuration)}
 * Attaches to a running Job using the JobID, and wait for its job result</li>
 * <li>{@link #awaitJobResult(JobInfoTracker, ClassLoader, Time)} Awaits the result of the job execution which jobInfoTracker listen for</li>
 * <li>{@link #retrieveClassLoader(JobID, JobMasterGateway, Configuration)} Reconstructs the class loader by first requesting information about it at the JobMaster
 * and then downloading missing jar files</li>
 * </ul>
 */
public class JobClientUtils {

	private static final Logger LOG = LoggerFactory.getLogger(JobClientUtils.class);


	/**
	 * Starts a rpc service for client
	 *
	 * @param config the flink configuration
	 * @return
	 * @throws IOException
	 */
	public static RpcService startJobClientRpcService(Configuration config)
		throws Exception
	{
		LOG.info("Starting JobClientUtils rpc service");
		Option<Tuple2<String, Object>> remoting = new Some<>(new Tuple2<String, Object>("", 0));

		// start a remote actor system to listen on an arbitrary port
		ActorSystem system = AkkaUtils.createActorSystem(config, remoting);
		Address address = system.provider().getDefaultAddress();

		String hostAddress = address.host().isDefined() ?
			NetUtils.ipAddressToUrlString(InetAddress.getByName(address.host().get())) :
			"(unknown)";
		int port = address.port().isDefined() ? ((Integer) address.port().get()) : -1;
		LOG.info("Started JobClientUtils actor system at " + hostAddress + ':' + port);
		return RpcServiceUtils.createRpcService(hostAddress, port, config);
	}

	/**
	 * Attaches to a running Job using the JobID, and wait for its job result
	 *
	 * @param jobID                  id of job
	 * @param jobMasterGateway       gateway to the JobMaster
	 * @param rpcService
	 * @param leaderRetrievalService leader retriever service of jobMaster
	 * @param sysoutLogUpdates       whether status messages shall be printed to sysout
	 * @param timeout                register timeout
	 * @param configuration          the flink configuration
	 * @return
	 * @throws JobExecutionException
	 */
	public static JobExecutionResult retrieveRunningJobResult(
		JobID jobID,
		JobMasterGateway jobMasterGateway,
		RpcService rpcService,
		LeaderRetrievalService leaderRetrievalService,
		boolean sysoutLogUpdates,
		Time timeout,
		Configuration configuration) throws JobExecutionException
	{

		checkNotNull(jobID, "The jobID must not be null.");
		checkNotNull(jobMasterGateway, "The jobMasterGateway must not be null.");
		checkNotNull(rpcService, "The rpcService must not be null.");
		checkNotNull(leaderRetrievalService, "The leaderRetrievalService must not be null.");
		checkNotNull(timeout, "The timeout must not be null");
		checkNotNull(configuration, "The configuration must not be null");

		JobInfoTracker jobInfoTracker = null;
		try {
			jobInfoTracker = new JobInfoTracker(rpcService, leaderRetrievalService, jobID, sysoutLogUpdates);
			jobInfoTracker.start();
			registerClientAtJobMaster(jobID, jobInfoTracker.getAddress(), jobMasterGateway, timeout);
			ClassLoader classLoader = retrieveClassLoader(jobID, jobMasterGateway, configuration);
			return awaitJobResult(jobInfoTracker, classLoader, timeout);
		} finally {
			if (jobInfoTracker != null) {
				jobInfoTracker.shutDown();
			}
		}
	}

	/**
	 * Awaits the result of the job execution which jobInfoTracker listen for
	 *
	 * @param jobInfoTracker job info tracker
	 * @param classLoader    classloader to parse the job result
	 * @return
	 * @throws JobExecutionException
	 */
	public static JobExecutionResult awaitJobResult(JobInfoTracker jobInfoTracker,
		ClassLoader classLoader, Time timeout) throws JobExecutionException
	{
		try {
			long timeoutInMillis = timeout.toMilliseconds();
			JobID jobID = jobInfoTracker.getJobID();
			Future<JobExecutionResult> jobExecutionResultFuture = jobInfoTracker.getJobExecutionResult(classLoader);
			while (true) {
				try {
					JobExecutionResult jobExecutionResult = jobExecutionResultFuture.get(timeoutInMillis, TimeUnit.MILLISECONDS);
					return jobExecutionResult;
				} catch (InterruptedException e) {
					throw new JobExecutionException(
						jobID,
						"Interrupted while waiting for job completion.");
				} catch (TimeoutException e) {
					try {
						// retry when timeout exception happened util jobInfoTracker is dead
						Future<Boolean> isAliveFuture = jobInfoTracker.getRpcService().isReachable(jobInfoTracker.getAddress());
						boolean isAlive = isAliveFuture.get(timeoutInMillis, TimeUnit.MILLISECONDS);
						checkState(isAlive, "JobInfoTracker has been dead!");
					} catch (Exception eInner) {
						if (!jobExecutionResultFuture.isDone()) {
							throw new JobExecutionException(
								jobID,
								"JobInfoTracker seems to have died before the JobExecutionResult could be retrieved.",
								eInner);
						}
					}
				} catch (ExecutionException e) {
					if (e.getCause() instanceof JobExecutionException) {
						throw (JobExecutionException) e.getCause();
					} else {
						throw new JobExecutionException(jobID,
							"Couldn't retrieve the JobExecutionResult from the JobMaster.", e.getCause());
					}

				}
			}
		} finally {
			jobInfoTracker.shutDown();
		}
	}

	/**
	 * Reconstructs the class loader by first requesting information about it at the JobMaster
	 * and then downloading missing jar files.
	 *
	 * @param jobID            id of job
	 * @param jobMasterGateway gateway to the JobMaster
	 * @param config           the flink configuration
	 * @return A classloader that should behave like the original classloader
	 * @throws JobRetrievalException if anything goes wrong
	 */
	public static ClassLoader retrieveClassLoader(
		JobID jobID,
		JobMasterGateway jobMasterGateway,
		Configuration config)
		throws JobRetrievalException
	{

		final JobManagerMessages.ClassloadingProps props;
		try {
			long defaultTimeoutInMillis = AkkaUtils.getDefaultTimeout().toMillis();
			props = jobMasterGateway.requestClassloadingProps(Time.milliseconds(defaultTimeoutInMillis)).get(defaultTimeoutInMillis, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Couldn't retrieve class loading properties from JobMaster.", e);
		}
		String jmHostname = null;
		try {
			InetSocketAddress jobMasterInetSocketAddress = AkkaUtils.getInetSockeAddressFromAkkaURL(jobMasterGateway.getAddress());
			jmHostname = jobMasterInetSocketAddress.getHostName();
		} catch (Exception e) {
			throw new RuntimeException("Failed to retrieve JobMaster address", e);
		}
		InetSocketAddress serverAddress = new InetSocketAddress(jmHostname, props.blobManagerPort());
		final BlobCache blobClient = new BlobCache(serverAddress, config);

		final List<BlobKey> requiredJarFiles = props.requiredJarFiles();
		final List<URL> requiredClasspaths = props.requiredClasspaths();

		final URL[] allURLs = new URL[requiredJarFiles.size() + requiredClasspaths.size()];

		int pos = 0;
		for (BlobKey blobKey : props.requiredJarFiles()) {
			try {
				allURLs[pos++] = blobClient.getURL(blobKey);
			} catch (Exception e) {
				blobClient.shutdown();
				throw new JobRetrievalException(jobID, "Failed to download BlobKey " + blobKey);
			}
		}

		for (URL url : requiredClasspaths) {
			allURLs[pos++] = url;
		}

		return new URLClassLoader(allURLs, org.apache.flink.runtime.client.JobClient.class.getClassLoader());
	}

	/**
	 * Registers client at job master
	 *
	 * @param jobID            id of job
	 * @param clientAddress    client address
	 * @param jobMasterGateway gateway to the JobMaster
	 * @param timeout          register timeout
	 * @throws JobExecutionException
	 */
	private static JobManagerMessages.RegisterJobClientSuccess registerClientAtJobMaster(JobID jobID,
		String clientAddress, JobMasterGateway jobMasterGateway,
		Time timeout) throws JobExecutionException
	{
		try {
			Future<JobManagerMessages.RegisterJobClientSuccess> registerJobClientSuccessFuture =
				jobMasterGateway.registerJobInfoTracker(clientAddress, ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES, timeout);
			JobManagerMessages.RegisterJobClientSuccess registerJobClientSuccess = registerJobClientSuccessFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			return registerJobClientSuccess;
		} catch (Throwable e) {
			throw new JobExecutionException(jobID, "Registration for Job at the JobMaster " +
				"timed out. " + "You may increase '" + ConfigConstants.AKKA_CLIENT_TIMEOUT +
				"' in case the JobMaster needs more time to confirm the job client registration.", e);
		}
	}
}
