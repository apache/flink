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
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.ZooKeeperCompletedCheckpointStore;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.ZooKeeperSubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.runtime.state.StateHandleProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ZooKeeperUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

	/**
	 * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper
	 * quorum.
	 *
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link CuratorFramework} instance
	 */
	public static CuratorFramework startCuratorFramework(Configuration configuration) {
		String zkQuorum = configuration.getString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, "");

		if (zkQuorum == null || zkQuorum.equals("")) {
			throw new RuntimeException("No valid ZooKeeper quorum has been specified. " +
					"You can specify the quorum via the configuration key '" +
					ConfigConstants.ZOOKEEPER_QUORUM_KEY + "'.");
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
				ConfigConstants.DEFAULT_ZOOKEEPER_DIR_KEY);

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

		cf.start();

		return cf;
	}

	/**
	 * Returns whether {@link RecoveryMode#ZOOKEEPER} is configured.
	 */
	public static boolean isZooKeeperRecoveryMode(Configuration flinkConf) {
		return RecoveryMode.fromConfig(flinkConf).equals(RecoveryMode.ZOOKEEPER);
	}

	/**
	 * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
	 * tolerate it).
	 */
	public static String getZooKeeperEnsemble(Configuration flinkConf)
			throws IllegalConfigurationException {

		String zkQuorum = flinkConf.getString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, "");

		if (zkQuorum == null || zkQuorum.equals("")) {
			throw new IllegalConfigurationException("No ZooKeeper quorum specified in config.");
		}

		// Remove all whitespace
		zkQuorum = zkQuorum.replaceAll("\\s+", "");

		return zkQuorum;
	}

	/**
	 * Creates a {@link ZooKeeperLeaderRetrievalService} instance.
	 *
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link ZooKeeperLeaderRetrievalService} instance.
	 * @throws Exception
	 */
	public static ZooKeeperLeaderRetrievalService createLeaderRetrievalService(
			Configuration configuration) throws Exception {
		CuratorFramework client = startCuratorFramework(configuration);
		String leaderPath = configuration.getString(ConfigConstants.ZOOKEEPER_LEADER_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_LEADER_PATH);

		return new ZooKeeperLeaderRetrievalService(client, leaderPath);
	}

	/**
	 * Creates a {@link ZooKeeperLeaderElectionService} instance and a new {@link
	 * CuratorFramework} client.
	 *
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link ZooKeeperLeaderElectionService} instance.
	 * @throws Exception
	 */
	public static ZooKeeperLeaderElectionService createLeaderElectionService(
			Configuration configuration) throws Exception {

		CuratorFramework client = startCuratorFramework(configuration);

		return createLeaderElectionService(client, configuration);
	}

	/**
	 * Creates a {@link ZooKeeperLeaderElectionService} instance.
	 *
	 * @param client        The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link ZooKeeperLeaderElectionService} instance.
	 * @throws Exception
	 */
	public static ZooKeeperLeaderElectionService createLeaderElectionService(
			CuratorFramework client,
			Configuration configuration) throws Exception {

		String latchPath = configuration.getString(ConfigConstants.ZOOKEEPER_LATCH_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_LATCH_PATH);
		String leaderPath = configuration.getString(ConfigConstants.ZOOKEEPER_LEADER_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_LEADER_PATH);

		return new ZooKeeperLeaderElectionService(client, latchPath, leaderPath);
	}

	/**
	 * Creates a {@link ZooKeeperSubmittedJobGraphStore} instance.
	 *
	 * @param client        The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration {@link Configuration} object
	 * @return {@link ZooKeeperSubmittedJobGraphStore} instance
	 */
	public static ZooKeeperSubmittedJobGraphStore createSubmittedJobGraphs(
			CuratorFramework client,
			Configuration configuration) throws Exception {

		checkNotNull(configuration, "Configuration");

		StateHandleProvider<SubmittedJobGraph> stateHandleProvider =
				StateHandleProviderFactory.createRecoveryFileStateHandleProvider(configuration);

		// ZooKeeper submitted jobs root dir
		String zooKeeperSubmittedJobsPath = configuration.getString(
				ConfigConstants.ZOOKEEPER_JOBGRAPHS_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH);

		return new ZooKeeperSubmittedJobGraphStore(
				client, zooKeeperSubmittedJobsPath, stateHandleProvider);
	}

	/**
	 * Creates a {@link ZooKeeperCompletedCheckpointStore} instance.
	 *
	 * @param client                         The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration                  {@link Configuration} object
	 * @param jobId                          ID of job to create the instance for
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain
	 * @param userClassLoader                User code class loader
	 * @return {@link ZooKeeperCompletedCheckpointStore} instance
	 */
	public static CompletedCheckpointStore createCompletedCheckpoints(
			CuratorFramework client,
			Configuration configuration,
			JobID jobId,
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader) throws Exception {

		checkNotNull(configuration, "Configuration");

		StateHandleProvider<CompletedCheckpoint> stateHandleProvider =
				StateHandleProviderFactory.createRecoveryFileStateHandleProvider(configuration);

		String completedCheckpointsPath = configuration.getString(
				ConfigConstants.ZOOKEEPER_CHECKPOINTS_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_CHECKPOINTS_PATH);

		completedCheckpointsPath += ZooKeeperSubmittedJobGraphStore.getPathForJob(jobId);

		return new ZooKeeperCompletedCheckpointStore(
				maxNumberOfCheckpointsToRetain,
				userClassLoader,
				client,
				completedCheckpointsPath,
				stateHandleProvider);
	}

	/**
	 * Creates a {@link ZooKeeperCheckpointIDCounter} instance.
	 *
	 * @param client        The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration {@link Configuration} object
	 * @param jobId         ID of job to create the instance for
	 * @return {@link ZooKeeperCheckpointIDCounter} instance
	 */
	public static ZooKeeperCheckpointIDCounter createCheckpointIDCounter(
			CuratorFramework client,
			Configuration configuration,
			JobID jobId) throws Exception {

		String checkpointIdCounterPath = configuration.getString(
				ConfigConstants.ZOOKEEPER_CHECKPOINT_COUNTER_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_CHECKPOINT_COUNTER_PATH);

		checkpointIdCounterPath += ZooKeeperSubmittedJobGraphStore.getPathForJob(jobId);

		return new ZooKeeperCheckpointIDCounter(client, checkpointIdCounterPath);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ZooKeeperUtils() {
		throw new RuntimeException();
	}
}
