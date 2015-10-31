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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ZooKeeper test utilities.
 */
public class ZooKeeperTestUtils {

	/**
	 * Creates a configuration to operate in {@link RecoveryMode#ZOOKEEPER}.
	 *
	 * @param zooKeeperQuorum   ZooKeeper quorum to connect to
	 * @param fsStateHandlePath Base path for file system state backend (for checkpoints and
	 *                          recovery)
	 * @return A new configuration to operate in {@link RecoveryMode#ZOOKEEPER}.
	 */
	public static Configuration createZooKeeperRecoveryModeConfig(
			String zooKeeperQuorum, String fsStateHandlePath) {

		return setZooKeeperRecoveryMode(new Configuration(), zooKeeperQuorum, fsStateHandlePath);
	}

	/**
	 * Sets all necessary configuration keys to operate in {@link RecoveryMode#ZOOKEEPER}.
	 *
	 * @param config            Configuration to use
	 * @param zooKeeperQuorum   ZooKeeper quorum to connect to
	 * @param fsStateHandlePath Base path for file system state backend (for checkpoints and
	 *                          recovery)
	 * @return The modified configuration to operate in {@link RecoveryMode#ZOOKEEPER}.
	 */
	public static Configuration setZooKeeperRecoveryMode(
			Configuration config,
			String zooKeeperQuorum,
			String fsStateHandlePath) {

		checkNotNull(config, "Configuration");
		checkNotNull(zooKeeperQuorum, "ZooKeeper quorum");
		checkNotNull(fsStateHandlePath, "File state handle backend path");

		// Web frontend, you have been dismissed. Sorry.
		config.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, -1);

		// ZooKeeper recovery mode
		config.setString(ConfigConstants.RECOVERY_MODE, "ZOOKEEPER");
		config.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, zooKeeperQuorum);

		int connTimeout = 5000;
		if (System.getenv().get("CI") != null) {
			// The regular timeout is to aggressive for Travis and connections are often lost.
			connTimeout = 20000;
		}

		config.setInteger(ConfigConstants.ZOOKEEPER_CONNECTION_TIMEOUT, connTimeout);
		config.setInteger(ConfigConstants.ZOOKEEPER_SESSION_TIMEOUT, connTimeout);

		// File system state backend
		config.setString(ConfigConstants.STATE_BACKEND, "FILESYSTEM");
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, fsStateHandlePath + "/checkpoints");
		config.setString(ConfigConstants.ZOOKEEPER_RECOVERY_PATH, fsStateHandlePath + "/recovery");

		// Akka failure detection and execution retries
		config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, "1000 ms");
		config.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, "6 s");
		config.setInteger(ConfigConstants.AKKA_WATCH_THRESHOLD, 9);
		config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "10 s");

		return config;
	}

}
