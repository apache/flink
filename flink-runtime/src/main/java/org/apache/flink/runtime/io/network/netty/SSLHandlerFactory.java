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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import static java.util.Objects.requireNonNull;

/**
 * Creates and configures {@link SslHandler} instances.
 */
public class SSLHandlerFactory {

	private final SSLContext sslContext;

	private final String[] enabledProtocols;

	private final String[] enabledCipherSuites;

	private final boolean clientMode;

	private final boolean clientAuthentication;

	private final int handshakeTimeoutMs;

	private final int closeNotifyFlushTimeoutMs;

	/**
	 * Create a new {@link SslHandler} factory.
	 *
	 * @param handshakeTimeoutMs
	 * 		SSL session timeout during handshakes (-1 = use system default)
	 * @param closeNotifyFlushTimeoutMs
	 * 		SSL session timeout after flushing the <tt>close_notify</tt> message (-1 = use system
	 * 		default)
	 */
	public SSLHandlerFactory(
			final SSLContext sslContext,
			final String[] enabledProtocols,
			final String[] enabledCipherSuites,
			final boolean clientMode,
			final boolean clientAuthentication,
			final int handshakeTimeoutMs,
			final int closeNotifyFlushTimeoutMs) {

		this.sslContext = requireNonNull(sslContext, "sslContext must not be null");
		this.enabledProtocols = requireNonNull(enabledProtocols, "enabledProtocols must not be null");
		this.enabledCipherSuites = requireNonNull(enabledCipherSuites, "cipherSuites must not be null");
		this.clientMode = clientMode;
		this.clientAuthentication = clientAuthentication;
		this.handshakeTimeoutMs = handshakeTimeoutMs;
		this.closeNotifyFlushTimeoutMs = closeNotifyFlushTimeoutMs;
	}

	public SslHandler createNettySSLHandler() {
		return createNettySSLHandler(createSSLEngine());
	}

	public SslHandler createNettySSLHandler(String hostname, int port) {
		return createNettySSLHandler(createSSLEngine(hostname, port));
	}

	private SslHandler createNettySSLHandler(SSLEngine sslEngine) {
		SslHandler sslHandler = new SslHandler(sslEngine);
		if (handshakeTimeoutMs >= 0) {
			sslHandler.setHandshakeTimeoutMillis(handshakeTimeoutMs);
		}
		if (closeNotifyFlushTimeoutMs >= 0) {
			sslHandler.setCloseNotifyFlushTimeoutMillis(closeNotifyFlushTimeoutMs);
		}

		return sslHandler;
	}

	private SSLEngine createSSLEngine() {
		final SSLEngine sslEngine = sslContext.createSSLEngine();
		configureSSLEngine(sslEngine);
		return sslEngine;
	}

	private SSLEngine createSSLEngine(String hostname, int port) {
		final SSLEngine sslEngine = sslContext.createSSLEngine(hostname, port);
		configureSSLEngine(sslEngine);
		return sslEngine;
	}

	private void configureSSLEngine(SSLEngine sslEngine) {
		sslEngine.setEnabledProtocols(enabledProtocols);
		sslEngine.setEnabledCipherSuites(enabledCipherSuites);
		sslEngine.setUseClientMode(clientMode);
		if (!clientMode) {
			sslEngine.setNeedClientAuth(clientAuthentication);
		}
	}
}
