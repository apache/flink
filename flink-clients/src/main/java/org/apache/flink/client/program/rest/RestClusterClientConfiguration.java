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

package org.apache.flink.client.program.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

/**
 * A configuration object for {@link RestClusterClient}s.
 */
public final class RestClusterClientConfiguration {

	private final String blobServerAddress;

	private final RestClientConfiguration restClientConfiguration;

	private final String restServerAddress;

	private final int restServerPort;

	private RestClusterClientConfiguration(
			String blobServerAddress,
			RestClientConfiguration endpointConfiguration,
			String restServerAddress,
			int restServerPort) {
		this.blobServerAddress = Preconditions.checkNotNull(blobServerAddress);
		this.restClientConfiguration = Preconditions.checkNotNull(endpointConfiguration);
		this.restServerAddress = Preconditions.checkNotNull(restServerAddress);
		this.restServerPort = restServerPort;
	}

	public String getBlobServerAddress() {
		return blobServerAddress;
	}

	public String getRestServerAddress() {
		return restServerAddress;
	}

	public int getRestServerPort() {
		return restServerPort;
	}

	public RestClientConfiguration getRestClientConfiguration() {
		return restClientConfiguration;
	}

	public static RestClusterClientConfiguration fromConfiguration(Configuration config) throws ConfigurationException {
		String blobServerAddress = config.getString(JobManagerOptions.ADDRESS);

		String serverAddress = config.getString(RestOptions.REST_ADDRESS);
		int serverPort = config.getInteger(RestOptions.REST_PORT);

		RestClientConfiguration restClientConfiguration = RestClientConfiguration.fromConfiguration(config);

		return new RestClusterClientConfiguration(blobServerAddress, restClientConfiguration, serverAddress, serverPort);
	}
}
