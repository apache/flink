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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.yarn.messages.NotifyWhenResourcesRegistered;
import org.apache.flink.yarn.messages.RequestNumberOfRegisteredResources;

import akka.actor.ActorRef;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A test extension to the {@link YarnFlinkResourceManager} that can handle additional test messages.
 */
public class TestingYarnFlinkResourceManager extends YarnFlinkResourceManager {

	private final PriorityQueue<Tuple2<Integer, ActorRef>> waitingQueue = new PriorityQueue<>(32, new Comparator<Tuple2<Integer, ActorRef>>() {
		@Override
		public int compare(Tuple2<Integer, ActorRef> o1, Tuple2<Integer, ActorRef> o2) {
			return o1.f0 - o2.f0;
		}
	});

	public TestingYarnFlinkResourceManager(
		Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		LeaderRetrievalService leaderRetrievalService,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers) {

		super(flinkConfig,
			yarnConfig,
			leaderRetrievalService,
			applicationMasterHostName,
			webInterfaceURL,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			numInitialTaskManagers);
	}

	public TestingYarnFlinkResourceManager(
		Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		LeaderRetrievalService leaderRetrievalService,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers,
		YarnResourceManagerCallbackHandler callbackHandler,
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient,
		NMClient nodeManagerClient) {
		super(flinkConfig, yarnConfig, leaderRetrievalService, applicationMasterHostName, webInterfaceURL, taskManagerParameters, taskManagerLaunchContext, yarnHeartbeatIntervalMillis, maxFailedContainers, numInitialTaskManagers, callbackHandler, resourceManagerClient, nodeManagerClient);
	}

	@Override
	protected void handleMessage(Object message) {
		if (message instanceof RequestNumberOfRegisteredResources) {
			getSender().tell(getNumberOfStartedTaskManagers(), getSelf());
		} else if (message instanceof NotifyWhenResourcesRegistered) {
			NotifyWhenResourcesRegistered notifyMessage = (NotifyWhenResourcesRegistered) message;

			if (getNumberOfStartedTaskManagers() >= notifyMessage.getNumberResources()) {
				getSender().tell(true, getSelf());
			} else {
				waitingQueue.offer(Tuple2.of(notifyMessage.getNumberResources(), getSender()));
			}
		} else if (message instanceof NotifyResourceStarted) {
			super.handleMessage(message);

			while (!waitingQueue.isEmpty() && waitingQueue.peek().f0 <= getNumberOfStartedTaskManagers()) {
				ActorRef receiver = waitingQueue.poll().f1;
				receiver.tell(true, getSelf());
			}
		} else {
			super.handleMessage(message);
		}
	}
}
