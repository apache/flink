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
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;

/**
 * An implementation of the {@link HighAvailabilityServices} with zookeeper.
 */
public class ZookeeperHaServices implements HighAvailabilityServices {

	private static final String DEFAULT_RESOURCE_MANAGER_PATH_SUFFIX = "/resource-manager";

	/** The ZooKeeper client to use */
	private final CuratorFramework client;

	/** The runtime configuration */
	private final Configuration configuration;

	public ZookeeperHaServices(final CuratorFramework client, final Configuration configuration) {
		this.client = client;
		this.configuration = configuration;
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() throws Exception {
		return ZooKeeperUtils.createLeaderRetrievalService(client, configuration, DEFAULT_RESOURCE_MANAGER_PATH_SUFFIX);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) throws Exception {
		return ZooKeeperUtils.createLeaderRetrievalService(client, configuration, getPathSuffixForJob(jobID));
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() throws Exception {
		return ZooKeeperUtils.createLeaderElectionService(client, configuration, DEFAULT_RESOURCE_MANAGER_PATH_SUFFIX);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) throws Exception {
		return ZooKeeperUtils.createLeaderElectionService(client, configuration, getPathSuffixForJob(jobID));
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception {
		return new ZooKeeperCheckpointRecoveryFactory(client, configuration);
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		return ZooKeeperUtils.createSubmittedJobGraphs(client, configuration);
	}

	private static String getPathSuffixForJob(final JobID jobID) {
		return String.format("/job-managers/%s", jobID);
	}
}
