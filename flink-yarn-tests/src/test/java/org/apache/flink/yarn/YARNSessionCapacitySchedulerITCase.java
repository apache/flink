/**
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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.yarn.YARNSessionFIFOITCase.addTestAppender;
import static org.apache.flink.yarn.YARNSessionFIFOITCase.checkForLogString;


/**
 * This test starts a MiniYARNCluster with a CapacityScheduler.
 * Is has, by default a queue called "default". The configuration here adds another queue: "qa-team".
 */
public class YARNSessionCapacitySchedulerITCase extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YARNSessionCapacitySchedulerITCase.class);

	@BeforeClass
	public static void setup() {
		yarnConfiguration.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		yarnConfiguration.set("yarn.scheduler.capacity.root.queues", "default,qa-team");
		yarnConfiguration.setInt("yarn.scheduler.capacity.root.default.capacity", 40);
		yarnConfiguration.setInt("yarn.scheduler.capacity.root.qa-team.capacity", 60);
		startYARNWithConfig(yarnConfiguration);
	}

	/**
	 * Test regular operation, including command line parameter parsing.
	 */
	@Test
	public void testClientStartup() {
		runWithArgs(new String[] {"-j", flinkUberjar.getAbsolutePath(),
						"-n", "1",
						"-jm", "512",
						"-tm", "1024", "-qu", "qa-team"},
				"Number of connected TaskManagers changed to 1. Slots available: 1", RunTypes.YARN_SESSION);
	}


	/**
	 * Test deployment to non-existing queue. (user-reported error)
	 * Deployment to the queue is possible because there are no queues, so we don't check.
	 */
	@Test
	public void testNonexistingQueue() {
		addTestAppender();
		runWithArgs(new String[] {"-j", flinkUberjar.getAbsolutePath(),
				"-n", "1",
				"-jm", "512",
				"-tm", "1024",
				"-qu", "doesntExist"}, "to unknown queue: doesntExist", RunTypes.YARN_SESSION);
		checkForLogString("The specified queue 'doesntExist' does not exist. Available queues: default, qa-team");
	}
}
