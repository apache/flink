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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import scala.concurrent.Await;
import scala.concurrent.Future;

/**
 * Cluster client for communication with an standalone (on-premise) cluster or an existing cluster that has been
 * brought up independently of a specific job.
 */
public class StandaloneClusterClient extends ClusterClient<StandaloneClusterId> {

	public StandaloneClusterClient(Configuration config) throws Exception {
		super(config);
	}

	public StandaloneClusterClient(Configuration config, HighAvailabilityServices highAvailabilityServices, boolean sharedHaServices) {
		super(config, highAvailabilityServices, sharedHaServices);
	}

	@Override
	public void waitForClusterToBeReady() {}

	@Override
	public String getWebInterfaceURL() {
		final InetSocketAddress inetSocketAddressFromAkkaURL;

		try {
			inetSocketAddressFromAkkaURL = AkkaUtils.getInetSocketAddressFromAkkaURL(getClusterConnectionInfo().getAddress());
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve leader retrieval information.", e);
		}

		String host = inetSocketAddressFromAkkaURL.getHostName();
		int port = getFlinkConfiguration().getInteger(WebOptions.PORT);
		return "http://" +  host + ":" + port;
	}

	@Override
	public GetClusterStatusResponse getClusterStatus() {
		ActorGateway jmGateway;
		try {
			jmGateway = getJobManagerGateway();
			Future<Object> future = jmGateway.ask(GetClusterStatus.getInstance(), timeout);
			Object result = Await.result(future, timeout);
			if (result instanceof GetClusterStatusResponse) {
				return (GetClusterStatusResponse) result;
			} else {
				throw new RuntimeException("Received the wrong reply " + result + " from cluster.");
			}
		} catch (Exception e) {
			throw new RuntimeException("Couldn't retrieve the cluster status.", e);
		}
	}

	@Override
	public List<String> getNewMessages() {
		return Collections.emptyList();
	}

	@Override
	public StandaloneClusterId getClusterId() {
		return StandaloneClusterId.getInstance();
	}

	@Override
	public int getMaxSlots() {
		return MAX_SLOTS_UNKNOWN;
	}

	@Override
	public boolean hasUserJarsInClassPath(List<URL> userJarFiles) {
		return false;
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader)
			throws ProgramInvocationException {
		if (isDetached()) {
			return super.runDetached(jobGraph, classLoader);
		} else {
			return super.run(jobGraph, classLoader);
		}
	}
}
