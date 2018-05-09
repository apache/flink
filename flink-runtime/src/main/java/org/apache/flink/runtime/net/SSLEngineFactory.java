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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import static java.util.Objects.requireNonNull;

/**
 * Creates and configures {@link SSLEngine} instances.
 */
public class SSLEngineFactory {

	private final SSLContext sslContext;

	private final String[] enabledProtocols;

	private final String[] enabledCipherSuites;

	private final boolean clientMode;

	public SSLEngineFactory(
			final SSLContext sslContext,
			final String[] enabledProtocols,
			final String[] enabledCipherSuites,
			final boolean clientMode) {
		this.sslContext = requireNonNull(sslContext, "sslContext must not be null");
		this.enabledProtocols = requireNonNull(enabledProtocols, "enabledProtocols must not be null");
		this.enabledCipherSuites = requireNonNull(enabledCipherSuites, "cipherSuites must not be null");
		this.clientMode = clientMode;
	}

	public SSLEngine createSSLEngine() {
		final SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setEnabledProtocols(enabledProtocols);
		sslEngine.setEnabledCipherSuites(enabledCipherSuites);
		sslEngine.setUseClientMode(clientMode);
		return sslEngine;
	}
}
