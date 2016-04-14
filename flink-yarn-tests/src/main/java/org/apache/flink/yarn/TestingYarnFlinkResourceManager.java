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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Flink's testing resource manager for Yarn.
 */
public class TestingYarnFlinkResourceManager extends YarnFlinkResourceManager {

	public TestingYarnFlinkResourceManager(
		Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		LeaderRetrievalService leaderRetrievalService,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers) {

		super(
			flinkConfig,
			yarnConfig,
			leaderRetrievalService,
			applicationMasterHostName,
			webInterfaceURL,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			numInitialTaskManagers);
	}
}
