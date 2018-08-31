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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;

import javax.annotation.Nullable;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.security.KeyStore;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common utilities to manage SSL transport settings.
 */
public class SSLUtils {

	/**
	 * Checks whether SSL for internal communication (rpc, data transport, blob server) is enabled.
	 */
	public static boolean isInternalSSLEnabled(Configuration sslConfig) {
		@SuppressWarnings("deprecation")
		final boolean fallbackFlag = sslConfig.getBoolean(SecurityOptions.SSL_ENABLED);
		return sslConfig.getBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, fallbackFlag);
	}

	/**
	 * Checks whether SSL for the external REST endpoint is enabled.
	 */
	public static boolean isRestSSLEnabled(Configuration sslConfig) {
		@SuppressWarnings("deprecation")
		final boolean fallbackFlag = sslConfig.getBoolean(SecurityOptions.SSL_ENABLED);
		return sslConfig.getBoolean(SecurityOptions.SSL_REST_ENABLED, fallbackFlag);
	}

	/**
	 * Creates a factory for SSL Server Sockets from the given configuration.
	 * SSL Server Sockets are always part of internal communication.
	 */
	public static ServerSocketFactory createSSLServerSocketFactory(Configuration config) throws Exception {
		SSLContext sslContext = createInternalSSLContext(config);
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
	 * SSL Client Sockets are always part of internal communication.
	 */
	public static SocketFactory createSSLClientSocketFactory(Configuration config) throws Exception {
		SSLContext sslContext = createInternalSSLContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled");
		}

		return sslContext.getSocketFactory();
	}

	/**
	 * Creates a SSLEngineFactory to be used by internal communication server endpoints.
	 */
	public static SSLEngineFactory createInternalServerSSLEngineFactory(final Configuration config) throws Exception {
		SSLContext sslContext = createInternalSSLContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled for internal communication.");
		}

		return new SSLEngineFactory(
				sslContext,
				getEnabledProtocols(config),
				getEnabledCipherSuites(config),
				false,
				true);
	}

	/**
	 * Creates a SSLEngineFactory to be used by internal communication client endpoints.
	 */
	public static SSLEngineFactory createInternalClientSSLEngineFactory(final Configuration config) throws Exception {
		SSLContext sslContext = createInternalSSLContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled for internal communication.");
		}

		return new SSLEngineFactory(
				sslContext,
				getEnabledProtocols(config),
				getEnabledCipherSuites(config),
				true,
				true);
	}

	/**
	 * Creates a {@link SSLEngineFactory} to be used by the REST Servers.
	 *
	 * @param config The application configuration.
	 */
	public static SSLEngineFactory createRestServerSSLEngineFactory(final Configuration config) throws Exception {
		SSLContext sslContext = createRestServerSSLContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled for REST endpoints.");
		}

		return new SSLEngineFactory(
				sslContext,
				getEnabledProtocols(config),
				getEnabledCipherSuites(config),
				false,
				false);
	}

	/**
	 * Creates a {@link SSLEngineFactory} to be used by the REST Clients.
	 *
	 * @param config The application configuration.
	 */
	public static SSLEngineFactory createRestClientSSLEngineFactory(final Configuration config) throws Exception {
		SSLContext sslContext = createRestClientSSLContext(config);
		if (sslContext == null) {
			throw new IllegalConfigurationException("SSL is not enabled for REST endpoints.");
		}

		return new SSLEngineFactory(
				sslContext,
				getEnabledProtocols(config),
				getEnabledCipherSuites(config),
				true,
				false);
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
	 * Creates the SSL Context for internal SSL, if internal SSL is configured.
	 * For internal SSL, the client and server side configuration are identical, because
	 * of mutual authentication.
	 */
	@Nullable
	public static SSLContext createInternalSSLContext(Configuration config) throws Exception {
		checkNotNull(config, "config");

		if (!isInternalSSLEnabled(config)) {
			return null;
		}
		String keystoreFilePath = getAndCheckOption(
				config, SecurityOptions.SSL_INTERNAL_KEYSTORE, SecurityOptions.SSL_KEYSTORE);

		String keystorePassword = getAndCheckOption(
				config, SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, SecurityOptions.SSL_KEYSTORE_PASSWORD);

		String certPassword = getAndCheckOption(
				config, SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, SecurityOptions.SSL_KEY_PASSWORD);

		String trustStoreFilePath = getAndCheckOption(
				config, SecurityOptions.SSL_INTERNAL_TRUSTSTORE, SecurityOptions.SSL_TRUSTSTORE);

		String trustStorePassword = getAndCheckOption(
				config, SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, SecurityOptions.SSL_TRUSTSTORE_PASSWORD);

		String sslProtocolVersion = config.getString(SecurityOptions.SSL_PROTOCOL);

		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		try (InputStream keyStoreFile = Files.newInputStream(new File(keystoreFilePath).toPath())) {
			keyStore.load(keyStoreFile, keystorePassword.toCharArray());
		}

		KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
		try (InputStream trustStoreFile = Files.newInputStream(new File(trustStoreFilePath).toPath())) {
			trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
		}

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, certPassword.toCharArray());

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);

		SSLContext sslContext = SSLContext.getInstance(sslProtocolVersion);
		sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

		return sslContext;
	}

	/**
	 * Creates an SSL context for the external REST endpoint server.
	 */
	@Nullable
	public static SSLContext createRestServerSSLContext(Configuration config) throws Exception {
		checkNotNull(config, "config");

		if (!isRestSSLEnabled(config)) {
			return null;
		}

		String keystoreFilePath = getAndCheckOption(
				config, SecurityOptions.SSL_REST_KEYSTORE, SecurityOptions.SSL_KEYSTORE);

		String keystorePassword = getAndCheckOption(
				config, SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, SecurityOptions.SSL_KEYSTORE_PASSWORD);

		String certPassword = getAndCheckOption(
				config, SecurityOptions.SSL_REST_KEY_PASSWORD, SecurityOptions.SSL_KEY_PASSWORD);

		String sslProtocolVersion = config.getString(SecurityOptions.SSL_PROTOCOL);

		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		try (InputStream keyStoreFile = Files.newInputStream(new File(keystoreFilePath).toPath())) {
			keyStore.load(keyStoreFile, keystorePassword.toCharArray());
		}

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, certPassword.toCharArray());

		SSLContext sslContext = SSLContext.getInstance(sslProtocolVersion);
		sslContext.init(kmf.getKeyManagers(), null, null);

		return sslContext;
	}

	/**
	 * Creates an SSL context for clients against the external REST endpoint.
	 */
	@Nullable
	public static SSLContext createRestClientSSLContext(Configuration config) throws Exception {
		checkNotNull(config, "config");

		if (!isRestSSLEnabled(config)) {
			return null;
		}

		String trustStoreFilePath = getAndCheckOption(
				config, SecurityOptions.SSL_REST_TRUSTSTORE, SecurityOptions.SSL_TRUSTSTORE);

		String trustStorePassword = getAndCheckOption(
				config, SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, SecurityOptions.SSL_TRUSTSTORE_PASSWORD);

		String sslProtocolVersion = config.getString(SecurityOptions.SSL_PROTOCOL);

		KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
		try (InputStream trustStoreFile = Files.newInputStream(new File(trustStoreFilePath).toPath())) {
			trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
		}

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);

		SSLContext sslContext = SSLContext.getInstance(sslProtocolVersion);
		sslContext.init(null, tmf.getTrustManagers(), null);

		return sslContext;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static String getAndCheckOption(Configuration config, ConfigOption<String> primaryOption, ConfigOption<String> fallbackOption) {
		String value = config.getString(primaryOption, config.getString(fallbackOption));
		if (value != null) {
			return value;
		}
		else {
			throw new IllegalConfigurationException("The config option " + primaryOption.key() +
					" or " + fallbackOption.key() + " is missing.");
		}
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
		public ServerSocket createServerSocket(int port) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(port);
			configureServerSocket(socket);
			return socket;
		}

		@Override
		public ServerSocket createServerSocket(int port, int backlog) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(port, backlog);
			configureServerSocket(socket);
			return socket;
		}

		@Override
		public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
			SSLServerSocket socket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(port, backlog, ifAddress);
			configureServerSocket(socket);
			return socket;
		}

		private void configureServerSocket(SSLServerSocket socket) {
			socket.setEnabledProtocols(protocols);
			socket.setEnabledCipherSuites(cipherSuites);
			socket.setNeedClientAuth(true);
		}
	}
}
