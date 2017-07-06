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

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

/**
 * Java representation of a running Flink job on YARN.
 * Since flip-6, a flink job will be run as a yarn job by default, each job has a jobmaster,
 * so this class will be used as a client to communicate with yarn and start the job on yarn.
 */
public class YarnClusterClientV2 extends ClusterClient {

	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClientV2.class);

	private YarnClient yarnClient;

	private final AbstractYarnClusterDescriptor clusterDescriptor;

	private ApplicationId appId;

	private String trackingURL;

	/**
	 * Create a client to communicate with YARN cluster.
	 *
	 * @param clusterDescriptor The descriptor used to create yarn job
	 * @param flinkConfig Flink configuration
	 * @throws Exception if the cluster client could not be created
	 */
	public YarnClusterClientV2(
			final AbstractYarnClusterDescriptor clusterDescriptor,
			org.apache.flink.configuration.Configuration flinkConfig) throws Exception {

		super(flinkConfig);

		this.clusterDescriptor = clusterDescriptor;
		this.yarnClient = clusterDescriptor.getYarnClient();
		this.trackingURL = "";
	}

	@Override
	public org.apache.flink.configuration.Configuration getFlinkConfiguration() {
		return flinkConfig;
	}

	@Override
	public int getMaxSlots() {
        // No need not set max slot
		return 0;
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return clusterDescriptor.hasUserJarFiles(userJarFiles);
	}

	@Override
	protected JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		throw new UnsupportedOperationException("Not yet implemented.");
	}

	@Override
	public String getWebInterfaceURL() {
		// there seems to be a difference between HD 2.2.0 and 2.6.0
		if (!trackingURL.startsWith("http://")) {
			return "http://" + trackingURL;
		} else {
			return trackingURL;
		}
	}

	@Override
	public String getClusterIdentifier() {
		return "Yarn cluster with application id " + getApplicationId();
	}

	/**
	 * This method is only available if the cluster hasn't been started in detached mode.
	 */
	@Override
	public GetClusterStatusResponse getClusterStatus() {
		throw new UnsupportedOperationException("Not support getClusterStatus since Flip-6.");
	}

	public ApplicationStatus getApplicationStatus() {
		//TODO: this method is useful for later
		return null;
	}

	@Override
	public List<String> getNewMessages() {
		throw new UnsupportedOperationException("Not support getNewMessages since Flip-6.");
	}

	@Override
	public void finalizeCluster() {
		// Do nothing
	}

	@Override
	public boolean isDetached() {
		return super.isDetached() || clusterDescriptor.isDetachedMode();
	}

	@Override
	public void waitForClusterToBeReady() {
		throw new UnsupportedOperationException("Not support waitForClusterToBeReady since Flip-6.");
	}

	@Override
	public InetSocketAddress getJobManagerAddress() {
		//TODO: just return a local address in order to be compatible with createClient in CliFrontend
		return new InetSocketAddress("localhost", 0);
	}

	public ApplicationId getApplicationId() {
		return appId;
	}

}
