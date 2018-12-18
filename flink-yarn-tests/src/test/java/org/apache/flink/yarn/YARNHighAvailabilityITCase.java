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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.JavaTestKit;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assume.assumeTrue;

/**
 * Tests that verify correct HA behavior.
 */
public class YARNHighAvailabilityITCase extends YarnTestBase {

	private static TestingServer zkServer;

	private static ActorSystem actorSystem;

	private static final int numberApplicationAttempts = 3;

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@BeforeClass
	public static void setup() {
		actorSystem = AkkaUtils.createDefaultActorSystem();

		try {
			zkServer = new TestingServer();
			zkServer.start();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Could not start ZooKeeper testing cluster.");
		}

		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-ha");
		YARN_CONFIGURATION.set(YarnConfiguration.RM_AM_MAX_ATTEMPTS, "" + numberApplicationAttempts);

		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (zkServer != null) {
			zkServer.stop();
		}

		JavaTestKit.shutdownActorSystem(actorSystem);
		actorSystem = null;
	}

	/**
	 * Tests that the application master can be killed multiple times and that the surviving
	 * TaskManager successfully reconnects to the newly started JobManager.
	 * @throws Exception
	 */
	@Test
	public void testMultipleAMKill() throws Exception {
		assumeTrue("This test only works with the old actor based code.", !isNewMode);
		final int numberKillingAttempts = numberApplicationAttempts - 1;
		String confDirPath = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		final Configuration configuration = GlobalConfiguration.loadConfiguration();
		TestingYarnClusterDescriptor flinkYarnClient = new TestingYarnClusterDescriptor(
			configuration,
			getYarnConfiguration(),
			confDirPath,
			getYarnClient(),
			true);

		Assert.assertNotNull("unable to get yarn client", flinkYarnClient);
		flinkYarnClient.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
		flinkYarnClient.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));

		String fsStateHandlePath = temp.getRoot().getPath();

		// load the configuration
		File configDirectory = new File(confDirPath);
		GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

		flinkYarnClient.setDynamicPropertiesEncoded("recovery.mode=zookeeper@@recovery.zookeeper.quorum=" +
			zkServer.getConnectString() + "@@yarn.application-attempts=" + numberApplicationAttempts +
			"@@" + CheckpointingOptions.STATE_BACKEND.key() + "=FILESYSTEM" +
			"@@" + CheckpointingOptions.CHECKPOINTS_DIRECTORY + "=" + fsStateHandlePath + "/checkpoints" +
			"@@" + HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + fsStateHandlePath + "/recovery");

		ClusterClient<ApplicationId> yarnClusterClient = null;

		final FiniteDuration timeout = new FiniteDuration(2, TimeUnit.MINUTES);

		HighAvailabilityServices highAvailabilityServices = null;

		final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(768)
			.setTaskManagerMemoryMB(1024)
			.setNumberTaskManagers(1)
			.setSlotsPerTaskManager(1)
			.createClusterSpecification();

		try {
			yarnClusterClient = flinkYarnClient.deploySessionCluster(clusterSpecification);

			highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
				yarnClusterClient.getFlinkConfiguration(),
				Executors.directExecutor(),
				HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

			final HighAvailabilityServices finalHighAvailabilityServices = highAvailabilityServices;

			new JavaTestKit(actorSystem) {{
				for (int attempt = 0; attempt < numberKillingAttempts; attempt++) {
					new Within(timeout) {
						@Override
						protected void run() {
							try {
								ActorGateway gateway = LeaderRetrievalUtils.retrieveLeaderGateway(
									finalHighAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
									actorSystem,
									timeout);
								ActorGateway selfGateway = new AkkaActorGateway(getRef(), gateway.leaderSessionID());

								gateway.tell(new TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered(1), selfGateway);

								expectMsgEquals(Acknowledge.get());

								gateway.tell(PoisonPill.getInstance());
							} catch (Exception e) {
								throw new AssertionError("Could not complete test.", e);
							}
						}
					};
				}

				new Within(timeout) {
					@Override
					protected void run() {
						try {
							ActorGateway gateway = LeaderRetrievalUtils.retrieveLeaderGateway(
								finalHighAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
								actorSystem,
								timeout);
							ActorGateway selfGateway = new AkkaActorGateway(getRef(), gateway.leaderSessionID());

							gateway.tell(new TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered(1), selfGateway);

							expectMsgEquals(Acknowledge.get());
						} catch (Exception e) {
							throw new AssertionError("Could not complete test.", e);
						}
					}
				};

			}};
		} finally {
			if (yarnClusterClient != null) {
				log.info("Shutting down the Flink Yarn application.");
				yarnClusterClient.shutDownCluster();
				yarnClusterClient.shutdown();
			}

			if (highAvailabilityServices != null) {
				highAvailabilityServices.closeAndCleanupAllData();
			}
		}
	}
}
