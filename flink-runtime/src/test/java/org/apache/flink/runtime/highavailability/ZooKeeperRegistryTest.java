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

package org.apache.flink.runtime.highavailability;

import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class ZooKeeperRegistryTest extends TestLogger {
	private TestingServer testingServer;

	private static Logger LOG = LoggerFactory.getLogger(ZooKeeperRegistryTest.class);

	@Before
	public void before() throws Exception {
		testingServer = new TestingServer();
	}

	@After
	public void after() throws Exception {
		testingServer.stop();
		testingServer = null;
	}

	/**
	 * Tests that the function of ZookeeperRegistry, setJobRunning(), setJobFinished(), isJobRunning()
	 */
	@Test
	public void testZooKeeperRegistry() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		HighAvailabilityServices zkHaService = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(configuration);
		RunningJobsRegistry zkRegistry = zkHaService.getRunningJobsRegistry();

		try {
			JobID jobID = JobID.generate();
			assertTrue(!zkRegistry.isJobRunning(jobID));

			zkRegistry.setJobRunning(jobID);
			assertTrue(zkRegistry.isJobRunning(jobID));

			zkRegistry.setJobFinished(jobID);
			assertTrue(!zkRegistry.isJobRunning(jobID));

		} finally {
			if (zkHaService != null) {
				zkHaService.close();
			}
		}
	}
}
