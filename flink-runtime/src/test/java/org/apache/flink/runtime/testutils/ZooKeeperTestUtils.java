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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ZooKeeper test utilities.
 */
public class ZooKeeperTestUtils {

	/**
	 * Creates a configuration to operate in {@link HighAvailabilityMode#ZOOKEEPER}.
	 *
	 * @param zooKeeperQuorum   ZooKeeper quorum to connect to
	 * @param fsStateHandlePath Base path for file system state backend (for checkpoints and
	 *                          recovery)
	 * @return A new configuration to operate in {@link HighAvailabilityMode#ZOOKEEPER}.
	 */
	public static Configuration createZooKeeperHAConfig(
			String zooKeeperQuorum, String fsStateHandlePath) {

		return configureZooKeeperHA(new Configuration(), zooKeeperQuorum, fsStateHandlePath);
	}

	/**
	 * Sets all necessary configuration keys to operate in {@link HighAvailabilityMode#ZOOKEEPER}.
	 *
	 * @param config            Configuration to use
	 * @param zooKeeperQuorum   ZooKeeper quorum to connect to
	 * @param fsStateHandlePath Base path for file system state backend (for checkpoints and
	 *                          recovery)
	 * @return The modified configuration to operate in {@link HighAvailabilityMode#ZOOKEEPER}.
	 */
	public static Configuration configureZooKeeperHA(
			Configuration config,
			String zooKeeperQuorum,
			String fsStateHandlePath) {

		checkNotNull(config, "Configuration");
		checkNotNull(zooKeeperQuorum, "ZooKeeper quorum");
		checkNotNull(fsStateHandlePath, "File state handle backend path");

		// ZooKeeper recovery mode
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperQuorum);

		int connTimeout = 5000;
		if (System.getenv().containsKey("CI")) {
			// The regular timeout is to aggressive for Travis and connections are often lost.
			connTimeout = 30000;
		}

		config.setInteger(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT, connTimeout);
		config.setInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, connTimeout);

		// File system state backend
		config.setString(CheckpointingOptions.STATE_BACKEND, "FILESYSTEM");
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, fsStateHandlePath + "/checkpoints");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, fsStateHandlePath + "/recovery");

		config.setString(AkkaOptions.ASK_TIMEOUT, "100 s");

		return config;
	}

}
