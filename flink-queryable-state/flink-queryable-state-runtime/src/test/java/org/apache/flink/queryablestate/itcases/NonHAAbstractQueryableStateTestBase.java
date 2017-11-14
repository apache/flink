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

package org.apache.flink.queryablestate.itcases;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.testingUtils.TestingCluster;

import org.junit.AfterClass;
import org.junit.Assert;

import static org.junit.Assert.fail;

/**
 * Base class with the cluster configuration for the tests on the HA mode.
 */
public abstract class NonHAAbstractQueryableStateTestBase extends AbstractQueryableStateTestBase {

	private static final int NUM_TMS = 2;
	private static final int NUM_SLOTS_PER_TM = 4;

	public static void setup(int proxyPortRangeStart, int serverPortRangeStart) {
		try {
			Configuration config = new Configuration();
			config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 4L);
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
			config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
			config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
			config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);
			config.setString(QueryableStateOptions.PROXY_PORT_RANGE, proxyPortRangeStart + "-" + (proxyPortRangeStart + NUM_TMS));
			config.setString(QueryableStateOptions.SERVER_PORT_RANGE, serverPortRangeStart + "-" + (serverPortRangeStart + NUM_TMS));

			cluster = new TestingCluster(config, false);
			cluster.start(true);

			client = new QueryableStateClient("localhost", proxyPortRangeStart);

			// verify that we are not in HA mode
			Assert.assertTrue(cluster.haMode() == HighAvailabilityMode.NONE);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@AfterClass
	public static void tearDown() {
		try {
			cluster.stop();
			client.shutdownAndWait();
		} catch (Throwable e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
