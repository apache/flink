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
import org.apache.curator.test.TestingCluster;
import org.apache.flink.client.FlinkYarnSessionCli;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class YarnHighAvailability extends YarnTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(YarnHighAvailability.class);

	private static TestingCluster zkCluster;

	@BeforeClass
	public static void setup() {
		zkCluster = new TestingCluster(3);

		try {
			zkCluster.start();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Could not start ZooKeeper testing cluster.");
		}

		yarnConfiguration.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-ha");

		startYARNWithConfig(yarnConfiguration);
	}

	@AfterClass
	public static void teardown() throws IOException {
		if(zkCluster != null) {
			zkCluster.stop();
		}
	}

	@Test
	public void testAMKill() throws Exception {
		AbstractFlinkYarnClient flinkYarnClient = FlinkYarnSessionCli.getFlinkYarnClient();

		Assert.assertNotNull("unable to get yarn client", flinkYarnClient);
		flinkYarnClient.setTaskManagerCount(1);
		flinkYarnClient.setJobManagerMemory(768);
		flinkYarnClient.setTaskManagerMemory(768);
		flinkYarnClient.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));

		String confDirPath = System.getenv("FLINK_CONF_DIR");
		flinkYarnClient.setConfigurationDirectory(confDirPath);

		flinkYarnClient.setFlinkConfigurationObject(GlobalConfiguration.getConfiguration());
		flinkYarnClient.setDynamicPropertiesEncoded("recovery.mode=zookeeper@@ha.zookeeper.quorum=" + zkCluster.getConnectString());
		flinkYarnClient.setConfigurationFilePath(new Path(confDirPath + File.separator + "flink-conf.yaml"));

		AbstractFlinkYarnCluster yarnCluster = null;
		ActorSystem system = null;

		final FiniteDuration timeout = new FiniteDuration(2, TimeUnit.MINUTES);

		try {
			system = AkkaUtils.createDefaultActorSystem();

			yarnCluster = flinkYarnClient.deploy();
			yarnCluster.connectToCluster();

			Thread.sleep(10000);

			Configuration config = yarnCluster.getFlinkConfiguration();

			LeaderRetrievalService lrs = LeaderRetrievalUtils.createLeaderRetrievalService(config);
			ActorGateway gateway = LeaderRetrievalUtils.retrieveLeaderGateway(lrs, system, timeout);
			LOG.info("Kill current leader " + gateway.actor().toString());
			gateway.tell(PoisonPill.getInstance());

			Thread.sleep(60000);

			LeaderRetrievalService lrs2 = LeaderRetrievalUtils.createLeaderRetrievalService(config);
			LOG.info("Created leader retrieval service.");
			ActorGateway gateway2 = LeaderRetrievalUtils.retrieveLeaderGateway(lrs2, system, timeout);

			LOG.info("Current leader " + gateway2.actor().toString());
		} finally {
			if (yarnCluster != null) {
				yarnCluster.shutdown(false);
			}

			if (system != null) {
				system.shutdown();
				system.awaitTermination();
			}
		}
	}
}
