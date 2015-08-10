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

package org.apache.flink.runtime.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper utilities.
 */
public class ZooKeeperUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

	public static CuratorFramework createCuratorFramework(Configuration configuration) throws Exception {
		String zkQuorum = ZooKeeperUtil.getZooKeeperEnsemble(configuration);

		if (zkQuorum == null || zkQuorum.equals("")) {
			throw new RuntimeException("No valid ZooKeeper quorum has been specified.");
		}

		int sessionTimeout = configuration.getInteger(
				ConfigConstants.ZOOKEEPER_SESSION_TIMEOUT,
				ConfigConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);

		int connectionTimeout = configuration.getInteger(
				ConfigConstants.ZOOKEEPER_CONNECTION_TIMEOUT,
				ConfigConstants.DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT);

		int retryWait = configuration.getInteger(
				ConfigConstants.ZOOKEEPER_RETRY_WAIT,
				ConfigConstants.DEFAULT_ZOOKEEPER_RETRY_WAIT);

		int maxRetryAttempts = configuration.getInteger(
				ConfigConstants.ZOOKEEPER_MAX_RETRY_ATTEMPTS,
				ConfigConstants.DEFAULT_ZOOKEEPER_MAX_RETRY_ATTEMPTS);

		String root = configuration.getString(ConfigConstants.ZOOKEEPER_DIR_KEY,
				ConfigConstants.DEFAULT_ZOOKEEPER_ZNODE_ROOT);

		LOG.info("Using '{}' as root namespace.", root);

		CuratorFramework cf = CuratorFrameworkFactory.builder()
				.connectString(zkQuorum)
				.sessionTimeoutMs(sessionTimeout)
				.connectionTimeoutMs(connectionTimeout)
				.retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
				// Curator prepends a '/' manually and throws an Exception if the
				// namespace starts with a '/'.
				.namespace(root.startsWith("/") ? root.substring(1) : root)
				.build();

		try {
			cf.start();
		}
		catch (Exception e) {
			throw new Exception("Could not start CuratorFramework.", e);
		}

		return cf;
	}

	/**
	 * Returns whether high availability is enabled (<=> ZooKeeper quorum configured).
	 */
	public static boolean isJobManagerHighAvailabilityEnabled(Configuration flinkConf) {
		return flinkConf.containsKey(ConfigConstants.ZOOKEEPER_QUORUM_KEY);
	}

	/**
	 * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
	 * tolerate it).
	 */
	public static String getZooKeeperEnsemble(Configuration flinkConf)
			throws IllegalConfigurationException {

		String zkQuorum = flinkConf.getString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, null);

		if (zkQuorum == null || zkQuorum.equals("")) {
			throw new IllegalConfigurationException("No ZooKeeper quorum specified in config.");
		}

		// Remove all whitespace
		zkQuorum = zkQuorum.replaceAll("\\s+", "");

		return zkQuorum;
	}
}
