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

import javax.annotation.Nullable;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.KeyStore;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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
		return sslConfig.getBoolean(SecurityOptions.SSL_ENABLED);
	}

	/**
	 * Creates a factory for SSL Server Sockets from the given configuration.
	 */
	public static ServerSocketFactory createSSLServerSocketFactory(Configuration config) throws Exception {
		SSLContext sslContext = createSSLServerContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled");
		}

		String[] protocols = getEnabledProtocols(config);
		String[] cipherSuites = getEnabledCipherSuites(config);

		SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
		return new ConfiguringSSLServerSocketFactory(factory, protocols, cipherSuites);
	}

	/**
	 * Creates a factory for SSL Client Sockets from the given configuration.
	 */
	public static SocketFactory createSSLClientSocketFactory(Configuration config) throws Exception {
		SSLContext sslContext = createSSLServerContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled");
		}

		return sslContext.getSocketFactory();
	}

	/**
	 * Creates a {@link SSLEngineFactory} to be used by the Server.
	 *
	 * @param config The application configuration.
	 */
	public static SSLEngineFactory createServerSSLEngineFactory(final Configuration config) throws Exception {
		return createSSLEngineFactory(config, false);
	}

	/**
	 * Creates a {@link SSLEngineFactory} to be used by the Client.
	 * @param config The application configuration.
	 */
	public static SSLEngineFactory createClientSSLEngineFactory(final Configuration config) throws Exception {
		return createSSLEngineFactory(config, true);
	}

	private static SSLEngineFactory createSSLEngineFactory(
			final Configuration config,
			final boolean clientMode) throws Exception {

		final SSLContext sslContext = clientMode ?
			createSSLClientContext(config) :
			createSSLServerContext(config);

		checkState(sslContext != null, "%s it not enabled", SecurityOptions.SSL_ENABLED.key());

		return new SSLEngineFactory(
			sslContext,
			getEnabledProtocols(config),
			getEnabledCipherSuites(config),
			clientMode);
	}

	/**
	 * Sets SSL version and cipher suites for SSLEngine.
	 *
	 * @param engine SSLEngine to be handled
	 * @param config The application configuration
	 * @deprecated Use {@link #createClientSSLEngineFactory(Configuration)} or
	 * {@link #createServerSSLEngineFactory(Configuration)}.
	 */
	@Deprecated
	public static void setSSLVerAndCipherSuites(SSLEngine engine, Configuration config) {
		engine.setEnabledProtocols(getEnabledProtocols(config));
		engine.setEnabledCipherSuites(getEnabledCipherSuites(config));
	}

	private static String[] getEnabledProtocols(final Configuration config) {
		checkNotNull(config, "config must not be null");
		return config.getString(SecurityOptions.SSL_PROTOCOL).split(",");
	}

	private static String[] getEnabledCipherSuites(final Configuration config) {
		checkNotNull(config, "config must not be null");
		return config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");
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
	@Nullable
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
	@Nullable
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
			try (FileInputStream keyStoreFile = new FileInputStream(new File(keystoreFilePath))) {
				ks.load(keyStoreFile, keystorePassword.toCharArray());
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

	// ------------------------------------------------------------------------
	//  Wrappers for socket factories that additionally configure the sockets
	// ------------------------------------------------------------------------

	private static class ConfiguringSSLServerSocketFactory extends ServerSocketFactory {

		private final SSLServerSocketFactory sslServerSocketFactory;
		private final String[] protocols;
		private final String[] cipherSuites;

		ConfiguringSSLServerSocketFactory(
				SSLServerSocketFactory sslServerSocketFactory,
				String[] protocols,
				String[] cipherSuites) {

			this.sslServerSocketFactory = sslServerSocketFactory;
			this.protocols = protocols;
			this.cipherSuites = cipherSuites;
		}

		@Override
		public ServerSocket createServerSocket(int i) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(i);
			configureServerSocket(socket);
			return socket;
		}

		@Override
		public ServerSocket createServerSocket(int i, int i1) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(i, i1);
			configureServerSocket(socket);
			return socket;
		}

		@Override
		public ServerSocket createServerSocket(int i, int i1, InetAddress inetAddress) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(i, i1, inetAddress);
			configureServerSocket(socket);
			return socket;
		}

		private void configureServerSocket(SSLServerSocket socket) {
			socket.setEnabledProtocols(protocols);
			socket.setEnabledCipherSuites(cipherSuites);
		}
	}
}
