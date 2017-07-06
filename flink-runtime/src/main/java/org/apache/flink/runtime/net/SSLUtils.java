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

package org.apache.flink.runtime.net;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.Arrays;

/**
 * Common utilities to manage SSL transport settings.
 */
public class SSLUtils {
	private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

	/**
	 * Retrieves the global ssl flag from configuration.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return true if global ssl flag is set
	 */
	public static boolean getSSLEnabled(Configuration sslConfig) {

		Preconditions.checkNotNull(sslConfig);

		return sslConfig.getBoolean(SecurityOptions.SSL_ENABLED);
	}

	/**
	 * Sets SSl version and cipher suites for SSLServerSocket.
	 * @param socket
	 *        Socket to be handled
	 * @param config
	 *        The application configuration
	 */
	public static void setSSLVerAndCipherSuites(ServerSocket socket, Configuration config) {
		if (socket instanceof SSLServerSocket) {
			final String[] protocols = config.getString(SecurityOptions.SSL_PROTOCOL).split(",");

			final String[] cipherSuites = config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");

			if (LOG.isDebugEnabled()) {
				LOG.debug("Configuring TLS version and cipher suites on SSL socket {} / {}",
						Arrays.toString(protocols), Arrays.toString(cipherSuites));
			}

			((SSLServerSocket) socket).setEnabledProtocols(protocols);
			((SSLServerSocket) socket).setEnabledCipherSuites(cipherSuites);
		}
	}

	/**
	 * Sets SSL version and cipher suites for SSLEngine.
	 * @param engine
	 *        SSLEngine to be handled
	 * @param config
	 *        The application configuration
	 */
	public static void setSSLVerAndCipherSuites(SSLEngine engine, Configuration config) {
		engine.setEnabledProtocols(config.getString(SecurityOptions.SSL_PROTOCOL).split(","));
		engine.setEnabledCipherSuites(config.getString(SecurityOptions.SSL_ALGORITHMS).split(","));
	}

	/**
	 * Sets SSL options to verify peer's hostname in the certificate.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @param sslParams
	 *        The SSL parameters that need to be updated
	 */
	public static void setSSLVerifyHostname(Configuration sslConfig, SSLParameters sslParams) {

		Preconditions.checkNotNull(sslConfig);
		Preconditions.checkNotNull(sslParams);

		boolean verifyHostname = sslConfig.getBoolean(SecurityOptions.SSL_VERIFY_HOSTNAME);
		if (verifyHostname) {
			sslParams.setEndpointIdentificationAlgorithm("HTTPS");
		}
	}

	/**
	 * Creates the SSL Context for the client if SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport client
	 * 	       Returns null if SSL is disabled
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLClientContext(Configuration sslConfig) throws Exception {

		Preconditions.checkNotNull(sslConfig);
		SSLContext clientSSLContext = null;

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating client SSL context from configuration");

			String trustStoreFilePath = sslConfig.getString(SecurityOptions.SSL_TRUSTSTORE);
			String trustStorePassword = sslConfig.getString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD);
			String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);

			Preconditions.checkNotNull(trustStoreFilePath, SecurityOptions.SSL_TRUSTSTORE.key() + " was not configured.");
			Preconditions.checkNotNull(trustStorePassword, SecurityOptions.SSL_TRUSTSTORE_PASSWORD.key() + " was not configured.");

			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

			FileInputStream trustStoreFile = null;
			try {
				trustStoreFile = new FileInputStream(new File(trustStoreFilePath));
				trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
			} finally {
				if (trustStoreFile != null) {
					trustStoreFile.close();
				}
			}

			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
				TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trustStore);

			clientSSLContext = SSLContext.getInstance(sslProtocolVersion);
			clientSSLContext.init(null, trustManagerFactory.getTrustManagers(), null);
		}

		return clientSSLContext;
	}

	/**
	 * Creates the SSL Context for the server if SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport server
	 * 	       Returns null if SSL is disabled
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLServerContext(Configuration sslConfig) throws Exception {

		Preconditions.checkNotNull(sslConfig);
		SSLContext serverSSLContext = null;

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating server SSL context from configuration");

			String keystoreFilePath = sslConfig.getString(SecurityOptions.SSL_KEYSTORE);

			String keystorePassword = sslConfig.getString(SecurityOptions.SSL_KEYSTORE_PASSWORD);

			String certPassword = sslConfig.getString(SecurityOptions.SSL_KEY_PASSWORD);

			String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);

			Preconditions.checkNotNull(keystoreFilePath, SecurityOptions.SSL_KEYSTORE.key() + " was not configured.");
			Preconditions.checkNotNull(keystorePassword, SecurityOptions.SSL_KEYSTORE_PASSWORD.key() + " was not configured.");
			Preconditions.checkNotNull(certPassword, SecurityOptions.SSL_KEY_PASSWORD.key() + " was not configured.");

			KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
			FileInputStream keyStoreFile = null;
			try {
				keyStoreFile = new FileInputStream(new File(keystoreFilePath));
				ks.load(keyStoreFile, keystorePassword.toCharArray());
			} finally {
				if (keyStoreFile != null) {
					keyStoreFile.close();
				}
			}

			// Set up key manager factory to use the server key store
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(
					KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(ks, certPassword.toCharArray());

			// Initialize the SSLContext
			serverSSLContext = SSLContext.getInstance(sslProtocolVersion);
			serverSSLContext.init(kmf.getKeyManagers(), null, null);
		}

		return serverSSLContext;
	}
}
