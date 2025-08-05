/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.core.security.watch.LocalFSWatchSingleton;

import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.remote.transport.netty.ConfigSSLEngineProvider;
import org.apache.pekko.remote.transport.netty.SSLEngineProvider;
import org.apache.pekko.stream.TLSRole;

import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Extension of the {@link ConfigSSLEngineProvider} to use a {@link FingerprintTrustManagerFactory}.
 */
@SuppressWarnings("deprecation")
public class CustomSSLEngineProvider implements SSLEngineProvider {

    private final String sslTrustStore;
    private final List<String> sslEnabledAlgorithms;
    private final String sslProtocol;
    private final Boolean sslRequireMutualAuthentication;
    private final SSLContextLoader sslContextLoader;

    public CustomSSLEngineProvider(ActorSystem system) throws IOException {
        final Config securityConfig =
                system.settings().config().getConfig("pekko.remote.classic.netty.ssl.security");
        sslTrustStore = securityConfig.getString("trust-store");
        String sslKeyStore = securityConfig.getString("key-store");
        sslEnabledAlgorithms = securityConfig.getStringList("enabled-algorithms");
        sslProtocol = securityConfig.getString("protocol");
        sslRequireMutualAuthentication = securityConfig.getBoolean("require-mutual-authentication");
        Boolean sslEnabledCertReload = securityConfig.getBoolean("enabled-cert-reload");

        sslContextLoader = new SSLContextLoader(sslTrustStore, sslProtocol, securityConfig);
        if (sslEnabledCertReload) {
            LocalFSWatchSingleton localFSWatchSingleton = LocalFSWatchSingleton.getInstance();
            localFSWatchSingleton.registerPath(
                    new Path[] {
                        Path.of(sslTrustStore).getParent(), Path.of(sslKeyStore).getParent()
                    },
                    sslContextLoader);
        }
    }

    @Override
    public SSLEngine createServerSSLEngine() {
        return createSSLEngine(TLSRole.server());
    }

    @Override
    public SSLEngine createClientSSLEngine() {
        return createSSLEngine(TLSRole.client());
    }

    private SSLEngine createSSLEngine(TLSRole role) {
        return createSSLEngine(sslContextLoader.createSSLEngine(), role);
    }

    private SSLEngine createSSLEngine(SSLEngine engine, TLSRole role) {
        engine.setUseClientMode(role == TLSRole.client());
        engine.setEnabledCipherSuites(sslEnabledAlgorithms.toArray(String[]::new));
        engine.setEnabledProtocols(new String[] {sslProtocol});

        if ((role != TLSRole.client()) && sslRequireMutualAuthentication) {
            engine.setNeedClientAuth(true);
        }

        return engine;
    }
}
