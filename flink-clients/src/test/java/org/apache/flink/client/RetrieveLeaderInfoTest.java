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

package org.apache.flink.client;

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class RetrieveLeaderInfoTest extends TestLogger {

	@Test
	public void testRestClusterClientProperlyRetrieveLeaderInfo() throws Exception {
		MiniClusterResourceConfiguration cfg = new MiniClusterResourceConfiguration.Builder().build();
		MiniClusterResource resource = new MiniClusterResource(cfg);

		ClusterClient<?> client = null;
		try {
			resource.before();

			HighAvailabilityServices haServices = resource.getMiniCluster().getHighAvailabilityServices();

			LeaderRetriever webMonitorLeaderRetriever = new LeaderRetriever();
			haServices.getWebMonitorLeaderRetriever().start(webMonitorLeaderRetriever);

			URL url = webMonitorLeaderRetriever.getLeaderFuture()
				.thenApply(leaderAddressSessionId -> {
					final String address = leaderAddressSessionId.f0;
					try {
						return new URL(address);
					} catch (MalformedURLException e) {
						throw new IllegalArgumentException("Could not parse URL from " + address, e);
					}
				})
				.get(2L, TimeUnit.SECONDS);

			Configuration conf = new Configuration();
			conf.setString(RestOptions.ADDRESS, url.getHost());
			conf.setInteger(RestOptions.PORT, url.getPort());

			client = new RestClusterClient<>(conf, StandaloneClusterId.getInstance());

			assertThat(client.getClusterConnectionInfo().getHostname(), equalTo("localhost"));
		} finally {
			resource.after();

			if (client != null) {
				client.shutdown();
			}
		}
	}

	@Test
	public void testMiniClusterClientProperlyRetrieveLeaderInfo() throws Exception {
		MiniClusterResourceConfiguration cfg = new MiniClusterResourceConfiguration.Builder().build();
		MiniClusterResource resource = new MiniClusterResource(cfg);

		ClusterClient<?> client = null;
		try {
			resource.before();

			client = new MiniClusterClient(resource.getClientConfiguration(), resource.getMiniCluster());

			assertThat(client.getClusterConnectionInfo().getHostname(), equalTo("localhost"));
		} finally {
			resource.after();

			if (client != null) {
				client.shutdown();
			}
		}
	}

}
