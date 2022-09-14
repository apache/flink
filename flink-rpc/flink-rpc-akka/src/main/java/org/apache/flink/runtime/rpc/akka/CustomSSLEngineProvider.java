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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

import akka.actor.ActorSystem;
import akka.remote.RemoteTransportException;
import akka.remote.transport.netty.ConfigSSLEngineProvider;
import com.typesafe.config.Config;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Extension of the {@link ConfigSSLEngineProvider} to use a {@link FingerprintTrustManagerFactory}.
 */
@SuppressWarnings("deprecation")
public class CustomSSLEngineProvider extends ConfigSSLEngineProvider {
    private final String sslTrustStore;
    private final String sslTrustStorePassword;
    private final List<String> sslCertFingerprints;

    public CustomSSLEngineProvider(ActorSystem system) {
        super(system);
        final Config securityConfig =
                system.settings().config().getConfig("akka.remote.classic.netty.ssl.security");
        sslTrustStore = securityConfig.getString("trust-store");
        sslTrustStorePassword = securityConfig.getString("trust-store-password");
        sslCertFingerprints = securityConfig.getStringList("cert-fingerprints");
    }

    @Override
    public TrustManager[] trustManagers() {
        try {
            final TrustManagerFactory trustManagerFactory =
                    sslCertFingerprints.isEmpty()
                            ? TrustManagerFactory.getInstance(
                                    TrustManagerFactory.getDefaultAlgorithm())
                            : FingerprintTrustManagerFactory.builder("SHA1")
                                    .fingerprints(sslCertFingerprints)
                                    .build();

            trustManagerFactory.init(loadKeystore(sslTrustStore, sslTrustStorePassword));
            return trustManagerFactory.getTrustManagers();
        } catch (GeneralSecurityException e) {
            // replicate exception handling from SSLEngineProvider
            throw new RemoteTransportException(
                    "Server SSL connection could not be established because SSL context could not be constructed",
                    e);
        }
    }
}
