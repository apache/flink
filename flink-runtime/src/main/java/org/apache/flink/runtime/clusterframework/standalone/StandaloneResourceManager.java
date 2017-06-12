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

package org.apache.flink.runtime.clusterframework.standalone;

import akka.actor.ActorSelection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.util.Collection;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN or Mesos.
 */
public class StandaloneResourceManager extends FlinkResourceManager<ResourceID> {
	

	public StandaloneResourceManager(Configuration flinkConfig, LeaderRetrievalService leaderRetriever) {
		super(0, flinkConfig, leaderRetriever);
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	@Override
	protected void triggerConnectingToJobManager(String leaderAddress) {
		ActorSelection jobManagerSel = context().actorSelection(leaderAddress);
		// check if we are at the leading JobManager.
		if (jobManagerSel.anchorPath().root().equals(self().path().root())) {
			super.triggerConnectingToJobManager(leaderAddress);
		} else {
			LOG.info("Received leader address but not running in leader ActorSystem. Cancelling registration.");
		}
	}

	@Override
	protected void initialize() throws Exception {
		// nothing to initialize
	}

	@Override
	protected void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {
	}

	@Override
	protected void fatalError(String message, Throwable error) {
		LOG.error("FATAL ERROR IN RESOURCE MANAGER: " + message, error);

		// kill this process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	@Override
	protected Collection<ResourceID> reacceptRegisteredWorkers(Collection<ResourceID> toConsolidate)
	{
		// we accept everything
		return toConsolidate;
	}

	@Override
	protected void requestNewWorkers(int numWorkers) {
		// cannot request new workers
	}

	@Override
	protected ResourceID workerStarted(ResourceID resourceID) {
		// we accept everything
		return resourceID;
	}

	@Override
	protected void releaseStartedWorker(ResourceID resourceID) {
		// cannot release any workers, they simply stay
	}

	@Override
	protected void releasePendingWorker(ResourceID resourceID) {
		// no pending workers
	}

	@Override
	protected int getNumWorkerRequestsPending() {
		// this one never has pending requests and containers in launch
		return 0;
	}

	@Override
	protected int getNumWorkersPendingRegistration() {
		// this one never has pending requests and containers in launch
		return 0;
	}

	// ------------------------------------------------------------------------
	//  Actor messages
	// ------------------------------------------------------------------------

	@Override
	protected void handleMessage(Object message) {
		super.handleMessage(message);
	}

}
