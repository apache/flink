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

package org.apache.flink.yarn.util;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.net.URL;
import java.util.List;

/**
 * Dummy {@link ClusterClient} for testing purposes (extend as needed).
 */
public class FakeClusterClient extends ClusterClient<ApplicationId> {

	public FakeClusterClient(Configuration flinkConfig) throws Exception {
		super(flinkConfig);
	}

	@Override
	public void waitForClusterToBeReady() {
	}

	@Override
	public String getWebInterfaceURL() {
		return "";
	}

	@Override
	public GetClusterStatusResponse getClusterStatus() {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	@Override
	public List<String> getNewMessages() {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	@Override
	public ApplicationId getClusterId() {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	@Override
	public int getMaxSlots() {
		return 10;
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return false;
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) {
		throw new UnsupportedOperationException("Not needed in test.");
	}
}
