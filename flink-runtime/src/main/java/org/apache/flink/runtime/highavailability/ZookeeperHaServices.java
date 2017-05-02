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

import org.apache.curator.framework.CuratorFramework;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * An implementation of the {@link HighAvailabilityServices} using Apache ZooKeeper.
 * The services store data in ZooKeeper's nodes as illustrated by teh following tree structure:
 * 
 * <pre>
 * /flink
 *      +/cluster_id_1/resource_manager_lock
 *      |            |
 *      |            +/job-id-1/job_manager_lock
 *      |            |         /checkpoints/latest
 *      |            |                     /latest-1
 *      |            |                     /latest-2
 *      |            |
 *      |            +/job-id-2/job_manager_lock
 *      |      
 *      +/cluster_id_2/resource_manager_lock
 *                   |
 *                   +/job-id-1/job_manager_lock
 *                            |/checkpoints/latest
 *                            |            /latest-1
 *                            |/persisted_job_graph
 * </pre>
 * 
 * <p>The root path "/flink" is configurable via the option {@link HighAvailabilityOptions#HA_ZOOKEEPER_ROOT}.
 * This makes sure Flink stores its data under specific subtrees in ZooKeeper, for example to
 * accommodate specific permission.
 * 
 * <p>The "cluster_id" part identifies the data stored for a specific Flink "cluster". 
 * This "cluster" can be either a standalone or containerized Flink cluster, or it can be job
 * on a framework like YARN or Mesos (in a "per-job-cluster" mode).
 * 
 * <p>In case of a "per-job-cluster" on YARN or Mesos, the cluster-id is generated and configured
 * automatically by the client or dispatcher that submits the Job to YARN or Mesos.
 * 
 * <p>In the case of a standalone cluster, that cluster-id needs to be configured via
 * {@link HighAvailabilityOptions#HA_CLUSTER_ID}. All nodes with the same cluster id will join the same
 * cluster and participate in the execution of the same set of jobs.
 */
public class ZookeeperHaServices implements HighAvailabilityServices {

	private static final String RESOURCE_MANAGER_LEADER_PATH = "/resource_manager_lock";

	private static final String JOB_MANAGER_LEADER_PATH = "/job_manager_lock";

	// ------------------------------------------------------------------------
	
	
	/** The ZooKeeper client to use */
	private final CuratorFramework client;

	/** The executor to run ZooKeeper callbacks on */
	private final Executor executor;

	/** The runtime configuration */
	private final Configuration configuration;

	/** The zookeeper based running jobs registry */
	private final RunningJobsRegistry runningJobsRegistry;

	public ZookeeperHaServices(CuratorFramework client, Executor executor, Configuration configuration) {
		this.client = checkNotNull(client);
		this.executor = checkNotNull(executor);
		this.configuration = checkNotNull(configuration);
		this.runningJobsRegistry = new ZookeeperRegistry(client, configuration);
	}

	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return ZooKeeperUtils.createLeaderRetrievalService(client, configuration, RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return ZooKeeperUtils.createLeaderRetrievalService(client, configuration, getPathForJobManager(jobID));
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return ZooKeeperUtils.createLeaderElectionService(client, configuration, RESOURCE_MANAGER_LEADER_PATH);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		return ZooKeeperUtils.createLeaderElectionService(client, configuration, getPathForJobManager(jobID));
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new ZooKeeperCheckpointRecoveryFactory(client, configuration, executor);
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		return ZooKeeperUtils.createSubmittedJobGraphs(client, configuration, executor);
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() {
		return runningJobsRegistry;
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return createBlobStore(configuration);
	}

	/**
	 * Creates the BLOB store in which BLOBs are stored in a highly-available
	 * fashion.
	 *
	 * @param configuration configuration to extract the storage path from
	 * @return Blob store
	 * @throws IOException if the blob store could not be created
	 */
	public static BlobStore createBlobStore(
		final Configuration configuration) throws IOException {
		String storagePath = configuration.getValue(
			HighAvailabilityOptions.HA_STORAGE_PATH);
		if (isNullOrWhitespaceOnly(storagePath)) {
			throw new IllegalConfigurationException("Configuration is missing the mandatory parameter: " +
					HighAvailabilityOptions.HA_STORAGE_PATH);
		}

		final Path path;
		try {
			path = new Path(storagePath);
		} catch (Exception e) {
			throw new IOException("Invalid path for highly available storage (" +
					HighAvailabilityOptions.HA_STORAGE_PATH.key() + ')', e);
		}

		final FileSystem fileSystem;
		try {
			fileSystem = path.getFileSystem();
		} catch (Exception e) {
			throw new IOException("Could not create FileSystem for highly available storage (" +
					HighAvailabilityOptions.HA_STORAGE_PATH.key() + ')', e);
		}

		final String clusterId =
			configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		storagePath += "/" + clusterId;

		return new FileSystemBlobStore(fileSystem, storagePath);
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		client.close();
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		close();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static String getPathForJobManager(final JobID jobID) {
		return "/" + jobID + JOB_MANAGER_LEADER_PATH;
	}
}
