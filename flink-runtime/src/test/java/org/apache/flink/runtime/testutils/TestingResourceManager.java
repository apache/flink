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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testingUtils.TestingMessages;

import akka.actor.ActorRef;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A testing resource manager which may alter the default standalone resource master's behavior.
 */
public class TestingResourceManager extends StandaloneResourceManager {

	/** Set of Actors which want to be informed of a connection to the job manager */
	private Set<ActorRef> waitForResourceManagerConnected = new HashSet<>();

	/** Set of Actors which want to be informed of a shutdown */
	private Set<ActorRef> waitForShutdown = new HashSet<>();

	/** Flag to signal a connection to the JobManager */
	private boolean isConnected = false;

	public TestingResourceManager(Configuration flinkConfig, LeaderRetrievalService leaderRetriever) {
		super(flinkConfig, leaderRetriever);
	}

	/**
	 * Overwrite messages here if desired
	 */
	@Override
	protected void handleMessage(Object message) {

		if (message instanceof GetRegisteredResources) {
			sender().tell(new GetRegisteredResourcesReply(getStartedTaskManagers()), self());
		} else if (message instanceof FailResource) {
			ResourceID resourceID = ((FailResource) message).resourceID;
			notifyWorkerFailed(resourceID, "Failed for test case.");

		} else if (message instanceof NotifyWhenResourceManagerConnected) {
			if (isConnected) {
				sender().tell(
					Acknowledge.get(),
					self());
			} else {
				waitForResourceManagerConnected.add(sender());
			}
		} else if (message instanceof RegisterResourceManagerSuccessful) {
			super.handleMessage(message);

			isConnected = true;

			for (ActorRef ref : waitForResourceManagerConnected) {
				ref.tell(
					Acknowledge.get(),
					self());
			}
			waitForResourceManagerConnected.clear();

		} else if (message instanceof TestingMessages.NotifyOfComponentShutdown$) {
			waitForShutdown.add(sender());
		} else if (message instanceof TestingMessages.Alive$) {
			sender().tell(Acknowledge.get(), self());
		} else {
			super.handleMessage(message);
		}
	}

	/**
	 * Testing messages
	 */
	public static class GetRegisteredResources {}

	public static class GetRegisteredResourcesReply {

		public Collection<ResourceID> resources;

		public GetRegisteredResourcesReply(Collection<ResourceID> resources) {
			this.resources = resources;
		}

	}

	/**
	 * Fails all resources that the resource manager has registered
	 */
	public static class FailResource {

		public ResourceID resourceID;

		public FailResource(ResourceID resourceID) {
			this.resourceID = resourceID;
		}
	}

	/**
	 * The sender of this message will be informed of a connection to the Job Manager
	 */
	public static class NotifyWhenResourceManagerConnected {}

	/**
	 * Inform registered listeners about a shutdown of the application.
     */
	@Override
	protected void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {
		for (ActorRef listener : waitForShutdown) {
			listener.tell(new TestingMessages.ComponentShutdown(self()), self());
		}
		waitForShutdown.clear();
	}
}
