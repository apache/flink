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

import javax.annotation.Nullable;
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

import static java.util.Objects.requireNonNull;
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
		requireNonNull(config, "config must not be null");
		return config.getString(SecurityOptions.SSL_PROTOCOL).split(",");
	}

	private static String[] getEnabledCipherSuites(final Configuration config) {
		requireNonNull(config, "config must not be null");
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
	 * SSL engine provider.
	 */
	public enum SSLProvider {
		JDK,
		/**
		 * OpenSSL with fallback to JDK if not available.
		 */
		OPENSSL;

		public static SSLProvider fromString(String value) {
			Preconditions.checkNotNull(value);
			if (value.equalsIgnoreCase("OPENSSL")) {
				return OPENSSL;
			} else if (value.equalsIgnoreCase("JDK")) {
				return JDK;
			} else {
				throw new IllegalArgumentException("Unknown SSL provider: " + value);
			}
		}
	}

	/**
	 * Instances needed to set up an SSL client connection.
	 */
	public static class SSLClientTools {
		public final SSLProvider preferredSslProvider;
		public final String sslProtocolVersion;
		public final TrustManagerFactory trustManagerFactory;

		public SSLClientTools(
				SSLProvider preferredSslProvider,
				String sslProtocolVersion,
				TrustManagerFactory trustManagerFactory) {
			this.preferredSslProvider = preferredSslProvider;
			this.sslProtocolVersion = sslProtocolVersion;
			this.trustManagerFactory = trustManagerFactory;
		}
	}

	/**
	 * Creates necessary helper objects to use for creating an SSL Context for the client if SSL is
	 * configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLClientTools object which can be used for creating some SSL context object;
	 * 	       returns <tt>null</tt> if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLClientTools createSSLClientTools(Configuration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating client SSL context from configuration");

			String trustStoreFilePath = sslConfig.getString(SecurityOptions.SSL_TRUSTSTORE);
			String trustStorePassword = sslConfig.getString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD);
			String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);
			SSLProvider sslProvider = SSLProvider.fromString(sslConfig.getString(SecurityOptions.SSL_PROVIDER));

			Preconditions.checkNotNull(trustStoreFilePath, SecurityOptions.SSL_TRUSTSTORE.key() + " was not configured.");
			Preconditions.checkNotNull(trustStorePassword, SecurityOptions.SSL_TRUSTSTORE_PASSWORD.key() + " was not configured.");

			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

			try (FileInputStream trustStoreFile = new FileInputStream(new File(trustStoreFilePath))) {
				trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
			}

			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
				TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trustStore);

			return new SSLClientTools(sslProvider, sslProtocolVersion, trustManagerFactory);
		}

		return null;
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
			SSLClientTools clientTools = createSSLClientTools(sslConfig);

			clientSSLContext = SSLContext.getInstance(clientTools.sslProtocolVersion);
			clientSSLContext.init(null, clientTools.trustManagerFactory.getTrustManagers(), null);
		}

		return clientSSLContext;
	}

	/**
	 * Instances needed to set up an SSL client connection.
	 */
	public static class SSLServerTools {
		public final SSLProvider preferredSslProvider;
		public final String sslProtocolVersion;
		public final String[] ciphers;
		public final KeyManagerFactory keyManagerFactory;

		public SSLServerTools(
				SSLProvider preferredSslProvider,
				String sslProtocolVersion,
				String[] ciphers,
				KeyManagerFactory keyManagerFactory) {
			this.preferredSslProvider = preferredSslProvider;
			this.sslProtocolVersion = sslProtocolVersion;
			this.ciphers = ciphers;
			this.keyManagerFactory = keyManagerFactory;
		}
	}

	/**
	 * Creates necessary helper objects to use for creating an SSL Context for the server if SSL is
	 * configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLServerTools object which can be used for creating some SSL context object;
	 * 	       returns <tt>null</tt> if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLServerTools createSSLServerTools(Configuration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating server SSL context from configuration");

			String keystoreFilePath = sslConfig.getString(SecurityOptions.SSL_KEYSTORE);
			String keystorePassword = sslConfig.getString(SecurityOptions.SSL_KEYSTORE_PASSWORD);
			String certPassword = sslConfig.getString(SecurityOptions.SSL_KEY_PASSWORD);
			SSLProvider sslProvider = SSLProvider.fromString(sslConfig.getString(SecurityOptions.SSL_PROVIDER));
			String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);
			String[] sslCipherSuites = sslConfig.getString(SecurityOptions.SSL_ALGORITHMS).split(",");

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

			return new SSLServerTools(sslProvider, sslProtocolVersion, sslCipherSuites, kmf);
		}

		return null;
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
			SSLServerTools serverTools = createSSLServerTools(sslConfig);

			// Initialize the SSLContext
			serverSSLContext = SSLContext.getInstance(serverTools.sslProtocolVersion);
			serverSSLContext.init(serverTools.keyManagerFactory.getKeyManagers(), null, null);
		}

		return serverSSLContext;
	}

}
