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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertTrue;

public class TaskManagerComponentsStartupShutdownTest extends TestLogger {

	/**
	 * Makes sure that all components are shut down when the TaskManager
	 * actor is shut down.
	 */
	@Test
	public void testComponentsStartupShutdown() throws Exception {

		final Time timeout = Time.seconds(100);
		final int BUFFER_SIZE = 32 * 1024;

		Configuration config = new Configuration();
		config.setString(AkkaOptions.WATCH_HEARTBEAT_INTERVAL, "200 ms");
		config.setString(AkkaOptions.WATCH_HEARTBEAT_PAUSE, "1 s");
		config.setInteger(AkkaOptions.WATCH_THRESHOLD, 1);

		final String[] TMP_DIR = ConfigurationUtils.parseTempDirectories(config);

		ActorSystem actorSystem = null;

		HighAvailabilityServices highAvailabilityServices = new EmbeddedHaServices(TestingUtils.defaultExecutor());

		ActorRef jobManager = null;
		ActorRef taskManager = null;

		try {
			actorSystem = AkkaUtils.createLocalActorSystem(config);

			jobManager = JobManager.startJobManagerActors(
				config,
				actorSystem,
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				highAvailabilityServices,
				NoOpMetricRegistry.INSTANCE,
				Option.empty(),
				JobManager.class,
				MemoryArchivist.class)._1();

			FlinkResourceManager.startResourceManagerActors(
				config,
				actorSystem,
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				StandaloneResourceManager.class);

			final int numberOfSlots = 1;

			// create the components for the TaskManager manually
			final TaskManagerConfiguration tmConfig = new TaskManagerConfiguration(
				numberOfSlots,
				TMP_DIR,
				timeout,
				null,
				Time.milliseconds(500),
				Time.seconds(30),
				Time.seconds(10),
				config,
				false, // exit-jvm-on-fatal-error
				FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
				new String[0],
				null,
				null);

			final int networkBufNum = 32;
			// note: the network buffer memory configured here is not actually used below but set
			// accordingly to be consistent
			final NetworkEnvironmentConfiguration netConf = new NetworkEnvironmentConfiguration(
					0.1f, networkBufNum * BUFFER_SIZE, networkBufNum * BUFFER_SIZE, BUFFER_SIZE, IOManager.IOMode.SYNC,
					0, 0, 2, 8, null);

			ResourceID taskManagerId = ResourceID.generate();
			
			final TaskManagerLocation connectionInfo = new TaskManagerLocation(taskManagerId, InetAddress.getLocalHost(), 10000);

			final MemoryManager memManager = new MemoryManager(networkBufNum * BUFFER_SIZE, 1, BUFFER_SIZE, MemoryType.HEAP, false);
			final IOManager ioManager = new IOManagerAsync(TMP_DIR);
			final NetworkEnvironment network = new NetworkEnvironment(
				new NetworkBufferPool(32, netConf.networkBufferSize()),
				new LocalConnectionManager(),
				new ResultPartitionManager(),
				new TaskEventDispatcher(),
				new KvStateRegistry(),
				null,
				null,
				netConf.ioMode(),
				netConf.partitionRequestInitialBackoff(),
				netConf.partitionRequestMaxBackoff(),
				netConf.networkBuffersPerChannel(),
				netConf.floatingNetworkBuffersPerGate(),
				true);

			network.start();

			TaskExecutorLocalStateStoresManager storesManager = new TaskExecutorLocalStateStoresManager(
				LocalRecoveryConfig.LocalRecoveryMode.DISABLED,
				ioManager.getSpillingDirectories(),
				Executors.directExecutor());

			MetricRegistryConfiguration metricRegistryConfiguration = MetricRegistryConfiguration.fromConfiguration(config);

			// create the task manager
			final Props tmProps = Props.create(
				TaskManager.class,
				tmConfig,
				taskManagerId,
				connectionInfo,
				memManager,
				ioManager,
				network,
				storesManager,
				numberOfSlots,
				highAvailabilityServices,
				new TaskManagerMetricGroup(NoOpMetricRegistry.INSTANCE, connectionInfo.getHostname(), connectionInfo.getResourceID().getResourceIdString()));

			taskManager = actorSystem.actorOf(tmProps);

			final ActorRef finalTaskManager = taskManager;

			new JavaTestKit(actorSystem) {{

				// wait for the TaskManager to be registered
				new Within(new FiniteDuration(5000L, TimeUnit.SECONDS)) {
					@Override
					protected void run() {
						finalTaskManager.tell(TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage(),
								getTestActor());

						expectMsgClass(TaskManagerMessages.RegisteredAtJobManager.class);
					}
				};
			}};

			// shut down all actors and the actor system
			// Kill the Task down the JobManager
			taskManager.tell(Kill.getInstance(), ActorRef.noSender());
			jobManager.tell(Kill.getInstance(), ActorRef.noSender());

			// shut down the actors and the actor system
			actorSystem.shutdown();
			actorSystem.awaitTermination();
			actorSystem = null;

			// now that the TaskManager is shut down, the components should be shut down as well
			assertTrue(network.isShutdown());
			assertTrue(ioManager.isProperlyShutDown());
			assertTrue(memManager.isShutdown());
		} finally {
			if (actorSystem != null) {
				actorSystem.shutdown();

				actorSystem.awaitTermination(TestingUtils.TESTING_TIMEOUT());
			}

			highAvailabilityServices.closeAndCleanupAllData();
		}
	}
}
