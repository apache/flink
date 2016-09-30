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
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.ZooKeeperCompletedCheckpointStore;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.ZooKeeperSubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.util.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
		String zkQuorum = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_QUORUM_KEY,
				null,
				ConfigConstants.ZOOKEEPER_QUORUM_KEY);

		if (zkQuorum == null || zkQuorum.equals("")) {
			throw new RuntimeException("No valid ZooKeeper quorum has been specified. " +
					"You can specify the quorum via the configuration key '" +
					ConfigConstants.HA_ZOOKEEPER_QUORUM_KEY + "'.");
		}

		int sessionTimeout = ConfigurationUtil.getIntegerWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_SESSION_TIMEOUT,
				ConfigConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT,
				ConfigConstants.ZOOKEEPER_SESSION_TIMEOUT);

		int connectionTimeout = ConfigurationUtil.getIntegerWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_CONNECTION_TIMEOUT,
				ConfigConstants.DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT,
				ConfigConstants.ZOOKEEPER_CONNECTION_TIMEOUT);

		int retryWait = ConfigurationUtil.getIntegerWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_RETRY_WAIT,
				ConfigConstants.DEFAULT_ZOOKEEPER_RETRY_WAIT,
				ConfigConstants.ZOOKEEPER_RETRY_WAIT);

		int maxRetryAttempts = ConfigurationUtil.getIntegerWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_MAX_RETRY_ATTEMPTS,
				ConfigConstants.DEFAULT_ZOOKEEPER_MAX_RETRY_ATTEMPTS,
				ConfigConstants.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

		String root = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_DIR_KEY,
				ConfigConstants.DEFAULT_ZOOKEEPER_DIR_KEY,
				ConfigConstants.ZOOKEEPER_DIR_KEY);

		String namespace = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_NAMESPACE_KEY,
				ConfigConstants.DEFAULT_ZOOKEEPER_NAMESPACE_KEY,
				ConfigConstants.ZOOKEEPER_NAMESPACE_KEY);

		String rootWithNamespace = generateZookeeperPath(root, namespace);

		LOG.info("Using '{}' as Zookeeper namespace.", rootWithNamespace);

		CuratorFramework cf = CuratorFrameworkFactory.builder()
				.connectString(zkQuorum)
				.sessionTimeoutMs(sessionTimeout)
				.connectionTimeoutMs(connectionTimeout)
				.retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
				// Curator prepends a '/' manually and throws an Exception if the
				// namespace starts with a '/'.
				.namespace(rootWithNamespace.startsWith("/") ? rootWithNamespace.substring(1) : rootWithNamespace)
				.build();

		cf.start();

		return cf;
	}

	/**
	 * Returns whether {@link HighAvailabilityMode#ZOOKEEPER} is configured.
	 */
	public static boolean isZooKeeperRecoveryMode(Configuration flinkConf) {
		return HighAvailabilityMode.fromConfig(flinkConf).equals(HighAvailabilityMode.ZOOKEEPER);
	}

	/**
	 * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
	 * tolerate it).
	 */
	public static String getZooKeeperEnsemble(Configuration flinkConf)
			throws IllegalConfigurationException {

		String zkQuorum = ConfigurationUtil.getStringWithDeprecatedKeys(
				flinkConf,
				ConfigConstants.HA_ZOOKEEPER_QUORUM_KEY,
				"",
				ConfigConstants.ZOOKEEPER_QUORUM_KEY);

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
		final Configuration configuration) throws Exception
	{
		final CuratorFramework client = startCuratorFramework(configuration);
		return createLeaderRetrievalService(client, configuration);
	}

	/**
	 * Creates a {@link ZooKeeperLeaderRetrievalService} instance.
	 *
	 * @param client        The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link ZooKeeperLeaderRetrievalService} instance.
	 * @throws Exception
	 */
	public static ZooKeeperLeaderRetrievalService createLeaderRetrievalService(
		final CuratorFramework client,
		final Configuration configuration) throws Exception
	{
		return createLeaderRetrievalService(client, configuration, "");
	}

	/**
	 * Creates a {@link ZooKeeperLeaderRetrievalService} instance.
	 *
	 * @param client        The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @param pathSuffix    The path suffix which we want to append
	 * @return {@link ZooKeeperLeaderRetrievalService} instance.
	 * @throws Exception
	 */
	public static ZooKeeperLeaderRetrievalService createLeaderRetrievalService(
		final CuratorFramework client,
		final Configuration configuration,
		final String pathSuffix) throws Exception
	{
		String leaderPath = ConfigurationUtil.getStringWithDeprecatedKeys(
			configuration,
			ConfigConstants.HA_ZOOKEEPER_LEADER_PATH,
			ConfigConstants.DEFAULT_ZOOKEEPER_LEADER_PATH,
			ConfigConstants.ZOOKEEPER_LEADER_PATH) + pathSuffix;

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

		return createLeaderElectionService(client, configuration, "");
	}

	/**
	 * Creates a {@link ZooKeeperLeaderElectionService} instance.
	 *
	 * @param client        The {@link CuratorFramework} ZooKeeper client to use
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @param pathSuffix    The path suffix which we want to append
	 * @return {@link ZooKeeperLeaderElectionService} instance.
	 * @throws Exception
	 */
	public static ZooKeeperLeaderElectionService createLeaderElectionService(
		final CuratorFramework client,
		final Configuration configuration,
		final String pathSuffix) throws Exception
	{
		final String latchPath = ConfigurationUtil.getStringWithDeprecatedKeys(
			configuration,
			ConfigConstants.HA_ZOOKEEPER_LATCH_PATH,
			ConfigConstants.DEFAULT_ZOOKEEPER_LATCH_PATH,
			ConfigConstants.ZOOKEEPER_LATCH_PATH) + pathSuffix;
		final String leaderPath = ConfigurationUtil.getStringWithDeprecatedKeys(
			configuration,
			ConfigConstants.HA_ZOOKEEPER_LEADER_PATH,
			ConfigConstants.DEFAULT_ZOOKEEPER_LEADER_PATH,
			ConfigConstants.ZOOKEEPER_LEADER_PATH) + pathSuffix;

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

		RetrievableStateStorageHelper<SubmittedJobGraph> stateStorage = createFileSystemStateStorage(configuration, "submittedJobGraph");

		// ZooKeeper submitted jobs root dir
		String zooKeeperSubmittedJobsPath = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_JOBGRAPHS_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH,
				ConfigConstants.ZOOKEEPER_JOBGRAPHS_PATH);

		return new ZooKeeperSubmittedJobGraphStore(
				client, zooKeeperSubmittedJobsPath, stateStorage);
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

		String checkpointsPath = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_CHECKPOINTS_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_CHECKPOINTS_PATH,
				ConfigConstants.ZOOKEEPER_CHECKPOINTS_PATH);

		RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage = createFileSystemStateStorage(
			configuration,
			"completedCheckpoint");

		checkpointsPath += ZooKeeperSubmittedJobGraphStore.getPathForJob(jobId);

		return new ZooKeeperCompletedCheckpointStore(
				maxNumberOfCheckpointsToRetain,
				userClassLoader,
				client,
				checkpointsPath,
				stateStorage);
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

		String checkpointIdCounterPath = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH,
				ConfigConstants.DEFAULT_ZOOKEEPER_CHECKPOINT_COUNTER_PATH,
				ConfigConstants.ZOOKEEPER_CHECKPOINT_COUNTER_PATH);

		checkpointIdCounterPath += ZooKeeperSubmittedJobGraphStore.getPathForJob(jobId);

		return new ZooKeeperCheckpointIDCounter(client, checkpointIdCounterPath);
	}

	/**
	 * Creates a {@link FileSystemStateStorageHelper} instance.
	 *
	 * @param configuration {@link Configuration} object
	 * @param prefix Prefix for the created files
	 * @param <T> Type of the state objects
	 * @return {@link FileSystemStateStorageHelper} instance
	 * @throws IOException
	 */
	public static <T extends Serializable> FileSystemStateStorageHelper<T> createFileSystemStateStorage(
			Configuration configuration,
			String prefix) throws IOException {

		String rootPath = ConfigurationUtil.getStringWithDeprecatedKeys(
				configuration,
				ConfigConstants.HA_ZOOKEEPER_STORAGE_PATH,
				"",
				ConfigConstants.ZOOKEEPER_RECOVERY_PATH);

		if (rootPath.equals("")) {
			throw new IllegalConfigurationException("Missing recovery path. Specify via " +
				"configuration key '" + ConfigConstants.HA_ZOOKEEPER_STORAGE_PATH + "'.");
		} else {
			return new FileSystemStateStorageHelper<T>(rootPath, prefix);
		}
	}

	private static String generateZookeeperPath(String root, String namespace) {
		if (!namespace.startsWith("/")) {
			namespace = "/" + namespace;
		}

		if (namespace.endsWith("/")) {
			namespace = namespace.substring(0, namespace.length() - 1);
		}

		return root + namespace;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ZooKeeperUtils() {
		throw new RuntimeException();
	}
}
