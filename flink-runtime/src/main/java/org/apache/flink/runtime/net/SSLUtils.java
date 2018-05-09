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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
	 * Creates the SSLContext for the client side of a connection, if SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport client.
	 *         Returns null if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLClientContext(Configuration sslConfig) throws Exception {
		// with mutual authentication, client and server context are identical
		return createSslContext(sslConfig, false, true);
	}

	/**
	 * Creates the SSLContext for the server side of a connection, if SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport server.
	 * 	       Returns null if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLServerContext(Configuration sslConfig) throws Exception {
		// with mutual authentication, client and server context are identical
		return createSslContext(sslConfig, true, false);
	}

	/**
	 * Creates the SSLContext for the client side of a connection that is only between intra-cluster
	 * components (JobManager, TaskManager, ResourceManager, Dispatcher, etc.)
	 *
	 * <p>These connections always require mutual authentication and hence need access to KeyStore
	 * and TrustStore.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport client.
	 * 	       Returns null if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLClientContextIntraCluster(Configuration sslConfig) throws Exception {
		return createSslContext(sslConfig, true, true);
	}

	/**
	 * Creates the SSLContext for the server side of a connection that is only between intra-cluster
	 * components (JobManager, TaskManager, ResourceManager, Dispatcher, etc.)
	 *
	 * <p>These connections always require mutual authentication and hence need access to KeyStore
	 * and TrustStore.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport server.
	 * 	       Returns null if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLServerContextIntraCluster(Configuration sslConfig) throws Exception {
		return createSslContext(sslConfig, true, true);
	}

	private static SSLContext createSslContext(
			Configuration sslConfig,
			boolean addKeyStore,
			boolean addTrustStore) throws Exception {

		checkNotNull(sslConfig, "config must not be null");

		if (!getSSLEnabled(sslConfig)) {
			return null;
		}
		else {
			LOG.debug("Creating client SSL context from configuration");

			final String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);
			if (sslProtocolVersion == null) {
				throw new IllegalConfigurationException(SecurityOptions.SSL_PROTOCOL.key() + " is null");
			}

			final KeyManager[] keyManagers = addKeyStore ?
					loadKeyManagerFactory(sslConfig).getKeyManagers() : null;

			final TrustManager[] trustManagers = addTrustStore ?
					loadTrustManagerFactory(sslConfig).getTrustManagers() : null;

			final SSLContext sslContext = SSLContext.getInstance(sslProtocolVersion);
			sslContext.init(keyManagers, trustManagers, null);

			return sslContext;
		}
	}

	private static KeyManagerFactory loadKeyManagerFactory(Configuration config) throws Exception {
		final String keystoreFilePath = config.getString(SecurityOptions.SSL_KEYSTORE);
		final String keystorePassword = config.getString(SecurityOptions.SSL_KEYSTORE_PASSWORD);
		final String certPassword = config.getString(SecurityOptions.SSL_KEY_PASSWORD);

		if (keystoreFilePath == null) {
			throw new IllegalConfigurationException(SecurityOptions.SSL_KEYSTORE.key() + " was not configured.");
		}
		if (keystorePassword == null) {
			throw new IllegalConfigurationException(SecurityOptions.SSL_KEYSTORE_PASSWORD.key() + " was not configured.");
		}
		if (certPassword == null) {
			throw new IllegalConfigurationException(SecurityOptions.SSL_KEY_PASSWORD.key() + " was not configured.");
		}

		KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

		try (FileInputStream keyStoreFile = new FileInputStream(new File(keystoreFilePath))) {
			ks.load(keyStoreFile, keystorePassword.toCharArray());
		}

		// Set up key manager factory to use the server key store
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, certPassword.toCharArray());

		return kmf;
	}

	private static TrustManagerFactory loadTrustManagerFactory(Configuration config) throws Exception {
		final String trustStoreFilePath = config.getString(SecurityOptions.SSL_TRUSTSTORE);
		final String trustStorePassword = config.getString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD);

		if (trustStoreFilePath == null) {
			throw new IllegalConfigurationException(SecurityOptions.SSL_TRUSTSTORE.key() + " was not configured.");
		}
		if (trustStorePassword == null) {
			throw new IllegalConfigurationException(SecurityOptions.SSL_TRUSTSTORE_PASSWORD.key() + " was not configured.");
		}

		KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

		try (FileInputStream trustStoreFile = new FileInputStream(new File(trustStoreFilePath))) {
			trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
		}

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);

		return tmf;
	}
}
