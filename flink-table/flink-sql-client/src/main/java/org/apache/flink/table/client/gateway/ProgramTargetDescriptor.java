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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.common.JobID;

/**
 * Describes the target where a table program has been submitted to.
 */
public class ProgramTargetDescriptor {

	private final String clusterId;

	private final String jobId;

	private final String webInterfaceUrl;

	public ProgramTargetDescriptor(String clusterId, String jobId, String webInterfaceUrl) {
		this.clusterId = clusterId;
		this.jobId = jobId;
		this.webInterfaceUrl = webInterfaceUrl;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getJobId() {
		return jobId;
	}

	public String getWebInterfaceUrl() {
		return webInterfaceUrl;
	}

	@Override
	public String toString() {
		return String.format(
			"Cluster ID: %s\n" +
			"Job ID: %s\n" +
			"Web interface: %s",
			clusterId, jobId, webInterfaceUrl);
	}

	/**
	 * Creates a program target description from deployment classes.
	 *
	 * @param clusterId cluster id
	 * @param jobId job id
	 * @param <C> cluster id type
	 * @return program target descriptor
	 */
	public static <C> ProgramTargetDescriptor of(C clusterId, JobID jobId, String webInterfaceUrl) {
		String clusterIdString;
		try {
			// check if cluster id has a toString method
			clusterId.getClass().getDeclaredMethod("toString");
			clusterIdString = clusterId.toString();
		} catch (NoSuchMethodException e) {
			clusterIdString = clusterId.getClass().getSimpleName();
		}
		return new ProgramTargetDescriptor(clusterIdString, jobId.toString(), webInterfaceUrl);
	}
}
