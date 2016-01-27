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

package org.apache.flink.runtime.checkpoint;

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link CheckpointCoordinator} components in {@link RecoveryMode#ZOOKEEPER}.
 */
public class ZooKeeperCheckpointRecoveryFactory implements CheckpointRecoveryFactory {

	private final CuratorFramework client;

	private final Configuration config;

	public ZooKeeperCheckpointRecoveryFactory(CuratorFramework client, Configuration config) {
		this.client = checkNotNull(client, "Curator client");
		this.config = checkNotNull(config, "Configuration");
	}

	@Override
	public void start() {
		// Nothing to do
	}

	@Override
	public void stop() {
		client.close();
	}

	@Override
	public CompletedCheckpointStore createCompletedCheckpoints(JobID jobId, ClassLoader userClassLoader)
			throws Exception {

		return ZooKeeperUtils.createCompletedCheckpoints(client, config, jobId,
				NUMBER_OF_SUCCESSFUL_CHECKPOINTS_TO_RETAIN, userClassLoader);
	}

	@Override
	public CheckpointIDCounter createCheckpointIDCounter(JobID jobID) throws Exception {
		return ZooKeeperUtils.createCheckpointIDCounter(client, config, jobID);
	}

}
