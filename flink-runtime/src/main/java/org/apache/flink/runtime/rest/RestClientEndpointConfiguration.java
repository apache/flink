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

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * A configuration object for {@link RestClientEndpoint}s.
 */
public final class RestClientEndpointConfiguration {

	private final String targetRestEndpointAddress;
	private final int targetRestEndpointPort;
	private final SSLEngine sslEngine;

	public RestClientEndpointConfiguration(String targetRestEndpointAddress, int targetRestEndpointPort, SSLEngine sslEngine) {
		this.targetRestEndpointAddress = targetRestEndpointAddress;
		this.targetRestEndpointPort = targetRestEndpointPort;
		this.sslEngine = sslEngine;
	}

	/**
	 * Returns the address of the REST server endpoint to connect to.
	 *
	 * @return REST server endpoint address
	 */
	public String getTargetRestEndpointAddress() {
		return targetRestEndpointAddress;
	}

	/**
	 * Returns the por tof the REST server endpoint to connect to.
	 *
	 * @return REST server endpoint port
	 */
	public int getTargetRestEndpointPort() {
		return targetRestEndpointPort;
	}

	/**
	 * Returns the {@link SSLEngine} that the REST client endpoint should use.
	 *
	 * @return SSLEngine that the REST client endpoint should use, or null if SSL was disabled
	 */

	public SSLEngine getSslEngine() {
		return sslEngine;
	}

	/**
	 * Creates and returns a new {@link RestClientEndpointConfiguration} from the given {@link Configuration}.
	 *
	 * @param config configuration from which the REST client endpoint configuration should be created from
	 * @return REST client endpoint configuration
	 * @throws ConfigurationException if SSL was configured incorrectly
	 */

	public static RestClientEndpointConfiguration fromConfiguration(Configuration config) throws ConfigurationException {
		String address = config.getString(RestOptions.REST_ADDRESS);

		int port = config.getInteger(RestOptions.REST_PORT);
		Preconditions.checkArgument(0 <= port && port <= 65536, "Port " + port + " is out of valid port range (0-65536).");

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
				throw new ConfigurationException("Failed to initialize SSLContext for the web frontend", e);
			}
		}

		return new RestClientEndpointConfiguration(address, port, sslEngine);
	}
}
