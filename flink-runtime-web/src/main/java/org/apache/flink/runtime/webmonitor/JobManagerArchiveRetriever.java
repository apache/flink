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
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;

/**
 * Retrieves and stores the actor gateway to the current leading JobManager and its archive. In
 * case of an error, the {@link WebRuntimeMonitor} to which this instance is associated will be
 * stopped.
 */
public class JobManagerArchiveRetriever implements LeaderRetrievalListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobManagerArchiveRetriever.class);

	private final ActorSystem actorSystem;
	private final FiniteDuration lookupTimeout;
	private final FiniteDuration timeout;
	private final WebMonitor webMonitor;

	/** will be written and read concurrently */
	private volatile ActorGateway jobManagerGateway;
	private volatile ActorGateway archiveGateway;

	public JobManagerArchiveRetriever(
			WebMonitor webMonitor,
			ActorSystem actorSystem,
			FiniteDuration lookupTimeout,
			FiniteDuration timeout) {
		this.webMonitor = webMonitor;
		this.actorSystem = actorSystem;
		this.lookupTimeout = lookupTimeout;
		this.timeout = timeout;
	}

	public ActorGateway getJobManagerGateway() {
		return jobManagerGateway;
	}

	public ActorGateway getArchiveGateway() {
		return archiveGateway;
	}


	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
		if (leaderAddress != null && !leaderAddress.equals("")) {
			try {
				ActorRef jobManager = AkkaUtils.getActorRef(
						leaderAddress,
						actorSystem,
						lookupTimeout);
				jobManagerGateway = new AkkaActorGateway(jobManager, leaderSessionID);

				Future<Object> archiveFuture = jobManagerGateway.ask(
						JobManagerMessages.getRequestArchive(),
						timeout);

				ActorRef archive = ((JobManagerMessages.ResponseArchive) Await.result(
						archiveFuture,
						timeout)
				).actor();

				archiveGateway = new AkkaActorGateway(archive, leaderSessionID);
			} catch (Exception e) {
				handleError(e);
			}
		}
	}

	@Override
	public void handleError(Exception exception) {
		LOG.error("Received error from LeaderRetrievalService.", exception);

		try{
			// stop associated webMonitor
			webMonitor.stop();
		} catch (Exception e) {
			LOG.error("Error while stopping the web server due to a LeaderRetrievalService error.", e);
		}
	}
}
