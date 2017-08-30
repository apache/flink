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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * A configuration object for {@link RestServerEndpoint}s.
 */
public final class RestServerEndpointConfiguration {

	@Nullable
	private final String restBindAddress;
	private final int restBindPort;
	@Nullable
	private final SSLEngine sslEngine;

	private RestServerEndpointConfiguration(@Nullable String restBindAddress, int restBindPort, @Nullable SSLEngine sslEngine) {
		this.restBindAddress = restBindAddress;

		Preconditions.checkArgument(0 <= restBindPort && restBindPort < 65536, "The bing rest port " + restBindPort + " is out of range (0, 65536[");
		this.restBindPort = restBindPort;
		this.sslEngine = sslEngine;
	}

	/**
	 * Returns the address that the REST server endpoint should bind itself to.
	 *
	 * @return address that the REST server endpoint should bind itself to
	 */
	public String getEndpointBindAddress() {
		return restBindAddress;
	}

	/**
	 * Returns the port that the REST server endpoint should listen on.
	 *
	 * @return port that the REST server endpoint should listen on
	 */
	public int getEndpointBindPort() {
		return restBindPort;
	}

	/**
	 * Returns the {@link SSLEngine} that the REST server endpoint should use.
	 *
	 * @return SSLEngine that the REST server endpoint should use, or null if SSL was disabled
	 */
	public SSLEngine getSslEngine() {
		return sslEngine;
	}

	/**
	 * Creates and returns a new {@link RestServerEndpointConfiguration} from the given {@link Configuration}.
	 *
	 * @param config configuration from which the REST server endpoint configuration should be created from
	 * @return REST server endpoint configuration
	 * @throws ConfigurationException if SSL was configured incorrectly
	 */
	public static RestServerEndpointConfiguration fromConfiguration(Configuration config) throws ConfigurationException {
		Preconditions.checkNotNull(config);
		String address = config.getString(RestOptions.REST_ADDRESS);

		int port = config.getInteger(RestOptions.REST_PORT);

		SSLEngine sslEngine = null;
		boolean enableSSL = config.getBoolean(SecurityOptions.SSL_ENABLED);
		if (enableSSL) {
			try {
				SSLContext sslContext = SSLUtils.createSSLServerContext(config);
				if (sslContext != null) {
					sslEngine = sslContext.createSSLEngine();
					SSLUtils.setSSLVerAndCipherSuites(sslEngine, config);
					sslEngine.setUseClientMode(false);
				}
			} catch (Exception e) {
				throw new ConfigurationException("Failed to initialize SSLContext for REST server endpoint.", e);
			}
		}

		return new RestServerEndpointConfiguration(address, port, sslEngine);
	}
}
