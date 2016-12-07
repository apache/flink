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
package org.apache.flink.runtime.metrics;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.taskmanager.TaskManagerConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.junit.Assert;
import org.junit.Test;
import scala.Option;
import scala.Tuple7;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

public class TaskManagerMetricsTest {

	/**
	 * Tests the metric registry life cycle on JobManager re-connects.
	 */
	@Test
	public void testMetricRegistryLifeCycle() {
		ActorSystem actorSystem = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

			// ================================================================
			// Start JobManager
			// ================================================================
			final ActorRef jobManager = JobManager.startJobManagerActors(
				new Configuration(),
				actorSystem,
				actorSystem.dispatcher(),
				actorSystem.dispatcher(),
				JobManager.class,
				MemoryArchivist.class)._1();

			LeaderRetrievalService leaderRetrievalService = new StandaloneLeaderRetrievalService(jobManager.path().toString());

			// ================================================================
			// Start TaskManager
			// ================================================================
			ResourceID tmResourceID = ResourceID.generate();

			Tuple7<TaskManagerConfiguration, TaskManagerLocation, MemoryManager, IOManager, NetworkEnvironment, LeaderRetrievalService, MetricRegistry> components =
				TaskManager.createTaskManagerComponents(
					new Configuration(),
					tmResourceID,
					"localhost",
					true,
					Option.apply(leaderRetrievalService)
				);

			// create the task manager
			final Props tmProps = TaskManager.getTaskManagerProps(
				TaskManager.class,
				components._1(),
				tmResourceID,
				components._2(),
				components._3(),
				components._4(),
				components._5(),
				components._6(),
				components._7());

			final ActorRef taskManager = actorSystem.actorOf(tmProps);

			new JavaTestKit(actorSystem) {{
				new Within(new FiniteDuration(5000, TimeUnit.SECONDS)) {
					@Override
					protected void run() {
						taskManager.tell(TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
							getTestActor());

						// wait for the TM to be registered
						expectMsgEquals(TaskManagerMessages.getRegisteredAtJobManagerMessage());

						// trigger re-registration of TM; this should include a disconnect from the current JM
						taskManager.tell(new TaskManagerMessages.JobManagerLeaderAddress(jobManager.path().toString(), null), jobManager);

						// wait for re-registration to be completed
						taskManager.tell(TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
							getTestActor());

						expectMsgEquals(TaskManagerMessages.getRegisteredAtJobManagerMessage());
					}
				};
			}};

			// verify that the registry was not shutdown due to the disconnect
			MetricRegistry tmRegistry = components._7();
			Assert.assertFalse(tmRegistry.isShutdown());

			// shut down the actors and the actor system
			actorSystem.shutdown();
			actorSystem.awaitTermination();
		} finally {
			if (actorSystem != null) {
				actorSystem.shutdown();
			}
		}
	}
}
