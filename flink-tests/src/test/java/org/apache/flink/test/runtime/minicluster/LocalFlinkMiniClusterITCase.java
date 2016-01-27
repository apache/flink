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

package org.apache.flink.test.runtime.minicluster;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LocalFlinkMiniClusterITCase {

	static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create("Testkit", AkkaUtils.getDefaultAkkaConfig());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	@Test
	public void testLocalFlinkMiniClusterWithMultipleTaskManagers() {
		LocalFlinkMiniCluster miniCluster = null;

		final int numTMs = 3;
		final int numSlots = 14;

		try{
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots);
			miniCluster = new LocalFlinkMiniCluster(config, true);

			miniCluster.start();

			final ActorGateway jmGateway = miniCluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			new JavaTestKit(system) {{
				final ActorGateway selfGateway = new AkkaActorGateway(getRef(), null);

				new Within(TestingUtils.TESTING_DURATION()) {

					@Override
					protected void run() {
						jmGateway.tell(
								JobManagerMessages.getRequestNumberRegisteredTaskManager(),
								selfGateway);

						expectMsgEquals(TestingUtils.TESTING_DURATION(), numTMs);

						jmGateway.tell(
								JobManagerMessages.getRequestTotalNumberOfSlots(),
								selfGateway);

						expectMsgEquals(TestingUtils.TESTING_DURATION(), numTMs*numSlots);
					}
				};
			}};


		} finally {
			if (miniCluster != null) {
				miniCluster.stop();
			}
		}
	}
}
