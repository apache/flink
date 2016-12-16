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

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.testkit.JavaTestKit;
import org.apache.curator.test.TestingServer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class YARNHighAvailabilityITCase extends YarnTestBase {

	protected static TestingServer zkServer;

	protected static ActorSystem actorSystem;

	protected static final int numberApplicationAttempts = 10;

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

		yarnConfiguration.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-ha");
		yarnConfiguration.set(YarnConfiguration.RM_AM_MAX_ATTEMPTS, "" + numberApplicationAttempts);

		startYARNWithConfig(yarnConfiguration);
	}

	@AfterClass
	public static void teardown() throws Exception {
		if(zkServer != null) {
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
		final int numberKillingAttempts = numberApplicationAttempts - 1;

		TestingYarnClusterDescriptor flinkYarnClient = new TestingYarnClusterDescriptor();

		Assert.assertNotNull("unable to get yarn client", flinkYarnClient);
		flinkYarnClient.setTaskManagerCount(1);
		flinkYarnClient.setJobManagerMemory(768);
		flinkYarnClient.setTaskManagerMemory(1024);
		flinkYarnClient.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
		flinkYarnClient.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));

		String confDirPath = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		flinkYarnClient.setConfigurationDirectory(confDirPath);

		String fsStateHandlePath = temp.getRoot().getPath();

		// load the configuration
		File configDirectory = new File(confDirPath);
		GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

		flinkYarnClient.setFlinkConfiguration(GlobalConfiguration.loadConfiguration());
		flinkYarnClient.setDynamicPropertiesEncoded("recovery.mode=zookeeper@@recovery.zookeeper.quorum=" +
			zkServer.getConnectString() + "@@yarn.application-attempts=" + numberApplicationAttempts +
			"@@" + ConfigConstants.STATE_BACKEND + "=FILESYSTEM" +
			"@@" + FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY + "=" + fsStateHandlePath + "/checkpoints" +
			"@@" + HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + fsStateHandlePath + "/recovery");
		flinkYarnClient.setConfigurationFilePath(new Path(confDirPath + File.separator + "flink-conf.yaml"));

		ClusterClient yarnCluster = null;

		final FiniteDuration timeout = new FiniteDuration(2, TimeUnit.MINUTES);

		try {
			yarnCluster = flinkYarnClient.deploy();
			final Configuration config = yarnCluster.getFlinkConfiguration();

			new JavaTestKit(actorSystem) {{
				for (int attempt = 0; attempt < numberKillingAttempts; attempt++) {
					new Within(timeout) {
						@Override
						protected void run() {
							try {
								LeaderRetrievalService lrs = LeaderRetrievalUtils.createLeaderRetrievalService(config);
								ActorGateway gateway = LeaderRetrievalUtils.retrieveLeaderGateway(lrs, actorSystem, timeout);
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
							LeaderRetrievalService lrs = LeaderRetrievalUtils.createLeaderRetrievalService(config);
							ActorGateway gateway2 = LeaderRetrievalUtils.retrieveLeaderGateway(lrs, actorSystem, timeout);
							ActorGateway selfGateway = new AkkaActorGateway(getRef(), gateway2.leaderSessionID());
							gateway2.tell(new TestingJobManagerMessages.NotifyWhenAtLeastNumTaskManagerAreRegistered(1), selfGateway);

							expectMsgEquals(Acknowledge.get());
						} catch (Exception e) {
							throw new AssertionError("Could not complete test.", e);
						}
					}
				};

			}};
		} finally {
			if (yarnCluster != null) {
				yarnCluster.shutdown();
			}
		}
	}
}
