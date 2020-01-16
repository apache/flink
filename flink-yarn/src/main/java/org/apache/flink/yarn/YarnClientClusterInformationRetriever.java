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

import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;

/**
 * Implementation of {@link YarnClusterInformationRetriever} that work with a {@link YarnClient}.
 */
public class YarnClientClusterInformationRetriever implements YarnClusterInformationRetriever {
	private final YarnClient yarnClient;

	public YarnClientClusterInformationRetriever(final YarnClient yarnClient) {
		this.yarnClient = yarnClient;
	}

	@Override
	public int numMaxVcores() throws Exception {
		// Fetch numYarnMaxVcores from all the RUNNING nodes via yarnClient
		return yarnClient.getNodeReports(NodeState.RUNNING)
			.stream()
			.mapToInt(report -> report.getCapability().getVirtualCores())
			.max()
			.orElse(0);
	}
}
