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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.messages.NotifyResourceStarted;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.messages.NotifyWhenResourcesRegistered;
import org.apache.flink.yarn.messages.RequestNumberOfRegisteredResources;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link YarnFlinkResourceManager}.
 */
public class YarnFlinkResourceManagerTest extends TestLogger {

	private static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testYarnFlinkResourceManagerJobManagerLostLeadership() throws Exception {
		new JavaTestKit(system) {{

			final Deadline deadline = new FiniteDuration(3, TimeUnit.MINUTES).fromNow();

			Configuration flinkConfig = new Configuration();
			YarnConfiguration yarnConfig = new YarnConfiguration();
			SettableLeaderRetrievalService leaderRetrievalService = new SettableLeaderRetrievalService(
				null,
				null);
			String applicationMasterHostName = "localhost";
			String webInterfaceURL = "foobar";
			ContaineredTaskManagerParameters taskManagerParameters = new ContaineredTaskManagerParameters(
				1L, 1L, 1L, 1, new HashMap<String, String>());
			ContainerLaunchContext taskManagerLaunchContext = mock(ContainerLaunchContext.class);
			int yarnHeartbeatIntervalMillis = 1000;
			int maxFailedContainers = 10;
			int numInitialTaskManagers = 5;
			final YarnResourceManagerCallbackHandler callbackHandler = new YarnResourceManagerCallbackHandler();
			AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient = mock(AMRMClientAsync.class);
			NMClient nodeManagerClient = mock(NMClient.class);
			UUID leaderSessionID = UUID.randomUUID();

			final List<Container> containerList = new ArrayList<>();

			for (int i = 0; i < numInitialTaskManagers; i++) {
				Container mockContainer = mock(Container.class);
				when(mockContainer.getId()).thenReturn(
					ContainerId.newInstance(
						ApplicationAttemptId.newInstance(
							ApplicationId.newInstance(System.currentTimeMillis(), 1),
							1),
						i));
				when(mockContainer.getNodeId()).thenReturn(NodeId.newInstance("container", 1234));
				containerList.add(mockContainer);
			}

			final CompletableFuture<AkkaActorGateway> resourceManagerFuture = new CompletableFuture<>();
			final CompletableFuture<AkkaActorGateway> leaderGatewayFuture = new CompletableFuture<>();

			doAnswer(
				(InvocationOnMock invocation) -> {
					Container container = (Container) invocation.getArguments()[0];
					resourceManagerFuture.thenCombine(leaderGatewayFuture,
						(resourceManagerGateway, leaderGateway) -> {
						resourceManagerGateway.tell(
							new NotifyResourceStarted(YarnFlinkResourceManager.extractResourceID(container)),
							leaderGateway);
						return null;
						});
					return null;
				})
				.when(nodeManagerClient)
				.startContainer(
					Matchers.any(Container.class),
					Matchers.any(ContainerLaunchContext.class));

			ActorRef resourceManager = null;
			ActorRef leader1;

			try {
				leader1 = system.actorOf(
					Props.create(
						TestingUtils.ForwardingActor.class,
						getRef(),
						Option.apply(leaderSessionID)
					));

				resourceManager = system.actorOf(
					Props.create(
						TestingYarnFlinkResourceManager.class,
						flinkConfig,
						yarnConfig,
						leaderRetrievalService,
						applicationMasterHostName,
						webInterfaceURL,
						taskManagerParameters,
						taskManagerLaunchContext,
						yarnHeartbeatIntervalMillis,
						maxFailedContainers,
						numInitialTaskManagers,
						callbackHandler,
						resourceManagerClient,
						nodeManagerClient
					));

				doReturn(Collections.singletonList(Collections.nCopies(numInitialTaskManagers, new AMRMClient.ContainerRequest(Resource.newInstance(1024 * 1024, 1), null, null, Priority.newInstance(0)))))
					.when(resourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));

				leaderRetrievalService.notifyListener(leader1.path().toString(), leaderSessionID);

				final AkkaActorGateway leader1Gateway = new AkkaActorGateway(leader1, leaderSessionID);
				final AkkaActorGateway resourceManagerGateway = new AkkaActorGateway(resourceManager, leaderSessionID);

				leaderGatewayFuture.complete(leader1Gateway);
				resourceManagerFuture.complete(resourceManagerGateway);

				expectMsgClass(deadline.timeLeft(), RegisterResourceManager.class);

				resourceManagerGateway.tell(new RegisterResourceManagerSuccessful(leader1, Collections.emptyList()));

				callbackHandler.onContainersAllocated(containerList);

				for (int i = 0; i < containerList.size(); i++) {
					expectMsgClass(deadline.timeLeft(), Acknowledge.class);
				}

				Future<Object> taskManagerRegisteredFuture = resourceManagerGateway.ask(new NotifyWhenResourcesRegistered(numInitialTaskManagers), deadline.timeLeft());

				Await.ready(taskManagerRegisteredFuture, deadline.timeLeft());

				leaderRetrievalService.notifyListener(null, null);

				leaderRetrievalService.notifyListener(leader1.path().toString(), leaderSessionID);

				expectMsgClass(deadline.timeLeft(), RegisterResourceManager.class);

				resourceManagerGateway.tell(new RegisterResourceManagerSuccessful(leader1, Collections.emptyList()));

				for (Container container: containerList) {
					resourceManagerGateway.tell(
						new NotifyResourceStarted(YarnFlinkResourceManager.extractResourceID(container)),
						leader1Gateway);
				}

				for (int i = 0; i < containerList.size(); i++) {
					expectMsgClass(deadline.timeLeft(), Acknowledge.class);
				}

				Future<Object> numberOfRegisteredResourcesFuture = resourceManagerGateway.ask(RequestNumberOfRegisteredResources.INSTANCE, deadline.timeLeft());

				int numberOfRegisteredResources = (Integer) Await.result(numberOfRegisteredResourcesFuture, deadline.timeLeft());

				assertEquals(numInitialTaskManagers, numberOfRegisteredResources);
			} finally {
				if (resourceManager != null) {
					resourceManager.tell(PoisonPill.getInstance(), ActorRef.noSender());
				}
			}
		}};
	}
}
