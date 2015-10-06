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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.ResponseWebMonitorPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Retrieves and stores the actor gateway to the current leading JobManager and its archive. In
 * case of an error, the {@link WebRuntimeMonitor} to which this instance is associated will be
 * stopped.
 *
 * <p>The job manager gateway only works if the web monitor and the job manager run in the same
 * actor system, because many execution graph structures are not serializable. This breaks the nice
 * leader retrieval abstraction and we have a special code path in case that another job manager is
 * leader. In such a case, we get the address of the web monitor of the leading job manager and
 * redirect to it (instead of directly communicating with it).
 */
public class JobManagerArchiveRetriever implements LeaderRetrievalListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerArchiveRetriever.class);

	private final ActorSystem actorSystem;
	private final FiniteDuration lookupTimeout;
	private final FiniteDuration timeout;
	private final WebMonitor webMonitor;

	/** Pattern to extract the host from an remote Akka URL */
	private final Pattern hostFromLeaderAddressPattern = Pattern.compile("^.+@(.+):([0-9]+)/user/.+$");

	/** The JobManager Akka URL associated with this JobManager */
	private volatile String jobManagerAkkaUrl;

	/** will be written and read concurrently */
	private volatile ActorGateway jobManagerGateway;
	private volatile ActorGateway archiveGateway;
	private volatile String redirectWebMonitorAddress;

	public JobManagerArchiveRetriever(
			WebMonitor webMonitor,
			ActorSystem actorSystem,
			FiniteDuration lookupTimeout,
			FiniteDuration timeout) {

		this.webMonitor = checkNotNull(webMonitor);
		this.actorSystem = checkNotNull(actorSystem);
		this.lookupTimeout = checkNotNull(lookupTimeout);
		this.timeout = checkNotNull(timeout);
	}

	/**
	 * Associates this instance with the job manager identified by the given URL.
	 *
	 * <p>This has to match the URL retrieved by the leader retrieval service. In tests setups you
	 * have to make sure to use the correct type of URLs.
	 */
	public void setJobManagerAkkaUrlAndRetrieveGateway(String jobManagerAkkaUrl) throws Exception {
		this.jobManagerAkkaUrl = checkNotNull(jobManagerAkkaUrl, "JobManager Akka URL");

		ActorRef jobManagerRef = AkkaUtils.getActorRef(
				jobManagerAkkaUrl, actorSystem, lookupTimeout);
		jobManagerGateway = new AkkaActorGateway(jobManagerRef, null);

		Future<Object> archiveFuture = jobManagerGateway.ask(
				JobManagerMessages.getRequestArchive(), timeout);

		ActorRef archiveRef = ((JobManagerMessages.ResponseArchive)
				Await.result(archiveFuture, timeout)).actor();
		archiveGateway = new AkkaActorGateway(archiveRef, null);
	}

	/**
	 * Returns the gateway to the job manager associated with this web monitor. Before working with
	 * the returned gateway, make sure to check {@link #getRedirectAddress()} for a redirect. This
	 * is necessary, because non-serializability breaks the leader retrieval abstraction (you cannot
	 * just work with any leader).
	 */
	public ActorGateway getJobManagerGateway() {
		// Sanity check
		if (jobManagerGateway == null) {
			throw new IllegalStateException("Job manager gateway has not been retrieved yet. " +
					"Did you call start?");
		}

		return jobManagerGateway;
	}

	public ActorGateway getArchiveGateway() {
		// Sanity check
		if (archiveGateway == null) {
			throw new IllegalStateException("Archive gateway has not been retrieved yet. " +
					"Did you call start?");
		}

		return archiveGateway;
	}

	/**
	 * Returns the current redirect address or <code>null</code> if the job manager associated with
	 * this web monitor is leading. In that case, work with the gateway directly.
	 */
	public String getRedirectAddress() {
		return redirectWebMonitorAddress;
	}

	@Override
	public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
		if (leaderAddress != null && !leaderAddress.equals("")) {
			try {
				if (jobManagerAkkaUrl == null) {
					throw new IllegalStateException("Unspecified Akka URL for the job manager " +
							"associated with this web monitor.");
				}

				boolean isLeader = jobManagerAkkaUrl.equals(leaderAddress);

				redirectWebMonitorAddress = null;

				// Our JobManager is leader and our work is done :)
				if (isLeader) {
					LOG.info("Retrieved leader notification. Associated local job manager {} is leader.", jobManagerAkkaUrl);
				}
				else {
					// We need to redirect to the leader -.-
					//
					// This is necessary currently, because many execution graph structures are not
					// serializable. The proper solution here is to have these serializable and
					// transparently work with the leading job manager instead of redirecting.
					LOG.info("Retrieved leader notification. Remote job  manager {} is leader.", leaderAddress);

					// Leader actor ref
					Future<ActorRef> newLeaderFuture = AkkaUtils
							.getActorRefFuture(leaderAddress, actorSystem, timeout);

					// Leader web monitor port request future
					Future<Object> portFuture = newLeaderFuture.flatMap(
							new Mapper<ActorRef, Future<Object>>() {
								@Override
								public Future<Object> apply(ActorRef jobManagerRef) {
									ActorGateway newLeaderGateway = new AkkaActorGateway(
											jobManagerRef, leaderSessionID);

									return newLeaderGateway.ask(JobManagerMessages
											.getRequestWebMonitorPort(), timeout);
								}
							}, actorSystem.dispatcher());

					// Set the redirect address
					portFuture.onComplete(new OnComplete<Object>() {
						@Override
						public void onComplete(Throwable failure, Object success) throws Throwable {
							if (failure == null) {
								ResponseWebMonitorPort portResponse = (ResponseWebMonitorPort) success;
								int webMonitorPort = portResponse.port();

								LOG.info("Leading job manager web monitor port is {}.", webMonitorPort);

								if (webMonitorPort != 1) {
									Matcher matcher = hostFromLeaderAddressPattern.matcher(leaderAddress);
									if (matcher.matches()) {
										redirectWebMonitorAddress = String.format("%s:%d",
												matcher.group(1), webMonitorPort);

										LOG.info("New redirect address is {}.", redirectWebMonitorAddress);
									}
									else {
										LOG.warn("Unexpected leader address pattern. Cannot extract host.");
									}
								}
								else {
									LOG.warn("Leading job manager has not web monitor port configured.");
								}
							}
							else if (!actorSystem.isTerminated()) {
								LOG.warn("Leading job manager port request future failed.", failure);
							}
						}
					}, actorSystem.dispatcher());
				}
			}
			catch (Exception e) {
				handleError(e);
			}
		}
	}

	@Override
	public void handleError(Exception exception) {
		LOG.error("Received error from LeaderRetrievalService.", exception);

		try {
			// stop associated webMonitor
			webMonitor.stop();
		}
		catch (Exception e) {
			LOG.error("Error while stopping the web server due to a LeaderRetrievalService error.", e);
		}
	}
}
