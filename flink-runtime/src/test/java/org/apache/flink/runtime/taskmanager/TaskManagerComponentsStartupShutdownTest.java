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

package org.apache.flink.runtime.taskmanager;

import static org.junit.Assert.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;

import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class TaskManagerComponentsStartupShutdownTest {

	/**
	 * Makes sure that all components are shut down when the TaskManager
	 * actor is shut down.
	 */
	@Test
	public void testComponentsStartupShutdown() {

		final String[] TMP_DIR = new String[] { ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH };
		final FiniteDuration timeout = new FiniteDuration(100, TimeUnit.SECONDS);
		final int BUFFER_SIZE = 32 * 1024;

		Configuration config = new Configuration();
		config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, "200 ms");
		config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, "1 s");
		config.setInteger(ConfigConstants.AKKA_WATCH_THRESHOLD, 1);

		ActorSystem actorSystem = null;
		try {
			actorSystem = AkkaUtils.createLocalActorSystem(config);

			final ActorRef jobManager = JobManager.startJobManagerActors(
				config,
				actorSystem,
				JobManager.class,
				MemoryArchivist.class)._1();

			// create the components for the TaskManager manually
			final TaskManagerConfiguration tmConfig = new TaskManagerConfiguration(
					TMP_DIR,
					1000000,
					timeout,
					Option.<FiniteDuration>empty(),
					1,
					config);

			final NetworkEnvironmentConfiguration netConf = new NetworkEnvironmentConfiguration(
					32, BUFFER_SIZE, MemoryType.HEAP, IOManager.IOMode.SYNC, Option.<NettyConfig>empty(),
					new Tuple2<Integer, Integer>(0, 0));

			final InstanceConnectionInfo connectionInfo = new InstanceConnectionInfo(InetAddress.getLocalHost(), 10000);

			final MemoryManager memManager = new MemoryManager(32 * BUFFER_SIZE, 1, BUFFER_SIZE, MemoryType.HEAP, false);
			final IOManager ioManager = new IOManagerAsync(TMP_DIR);
			final NetworkEnvironment network = new NetworkEnvironment(
				TestingUtils.defaultExecutionContext(),
				timeout,
				netConf);
			final int numberOfSlots = 1;

			LeaderRetrievalService leaderRetrievalService = new StandaloneLeaderRetrievalService(jobManager.path().toString());

			// create the task manager
			final Props tmProps = Props.create(
					TaskManager.class,
					tmConfig,
					connectionInfo,
					memManager,
					ioManager,
					network,
					numberOfSlots,
					leaderRetrievalService);

			final ActorRef taskManager = actorSystem.actorOf(tmProps);

			new JavaTestKit(actorSystem) {{

				// wait for the TaskManager to be registered
				new Within(new FiniteDuration(5000, TimeUnit.SECONDS)) {
					@Override
					protected void run() {
						taskManager.tell(TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
								getTestActor());

						expectMsgEquals(TaskManagerMessages.getRegisteredAtJobManagerMessage());
					}
				};
			}};

			// the components should now all be initialized
			assertTrue(network.isAssociated());

			// shut down all actors and the actor system
			// Kill the Task down the JobManager
			taskManager.tell(Kill.getInstance(), ActorRef.noSender());
			jobManager.tell(Kill.getInstance(), ActorRef.noSender());

			// shut down the actors and the actor system
			actorSystem.shutdown();
			actorSystem.awaitTermination();
			actorSystem = null;

			// now that the TaskManager is shut down, the components should be shut down as well
			assertFalse(network.isAssociated());
			assertTrue(network.isShutdown());
			assertTrue(ioManager.isProperlyShutDown());
			assertTrue(memManager.isShutdown());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (actorSystem != null) {
				actorSystem.shutdown();
			}
		}
	}
}
