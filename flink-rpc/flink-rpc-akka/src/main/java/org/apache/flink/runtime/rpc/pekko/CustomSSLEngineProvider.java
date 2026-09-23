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

import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.remote.RemoteTransportException;
import org.apache.pekko.remote.transport.netty.ConfigSSLEngineProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;

/**
 * Extension of the {@link ConfigSSLEngineProvider} to use a {@link FingerprintTrustManagerFactory}
 * and to support a comma-separated protocol list in {@code security.protocol}.
 *
 * <p>{@link ConfigSSLEngineProvider}'s own engine creation reads {@code security.protocol} as a
 * single string and passes it unsplit to both {@code SSLContext.getInstance(String)} and {@code
 * SSLEngine#setEnabledProtocols(String[])}, so a comma-separated value breaks it. Its {@code
 * sslContext} and {@code createSSLEngine} members are {@code private} and cannot be overridden, so
 * {@link #createServerSSLEngine()} and {@link #createClientSSLEngine()} are reimplemented here
 * instead, splitting the protocol list the same way {@code SSLUtils} does for the data-plane and
 * REST SSL contexts.
 */
@SuppressWarnings("deprecation")
public class CustomSSLEngineProvider extends ConfigSSLEngineProvider {
    private final String sslTrustStore;
    private final String sslTrustStorePassword;
    private final List<String> sslCertFingerprints;
    private final String sslKeyStoreType;
    private final String sslTrustStoreType;
    private final String[] sslProtocols;
    private final String[] sslEnabledAlgorithms;
    private final boolean sslRequireMutualAuthentication;

    private SSLContext sslContext;

    public CustomSSLEngineProvider(ActorSystem system) {
        super(system);
        final Config securityConfig =
                system.settings().config().getConfig("pekko.remote.classic.netty.ssl.security");
        sslTrustStore = securityConfig.getString("trust-store");
        sslTrustStorePassword = securityConfig.getString("trust-store-password");
        sslCertFingerprints = securityConfig.getStringList("cert-fingerprints");
        sslKeyStoreType = securityConfig.getString("key-store-type");
        sslTrustStoreType = securityConfig.getString("trust-store-type");
        sslProtocols = securityConfig.getString("protocol").split(",");
        sslEnabledAlgorithms =
                securityConfig.getStringList("enabled-algorithms").toArray(new String[0]);
        sslRequireMutualAuthentication = securityConfig.getBoolean("require-mutual-authentication");
    }

    @Override
    public SSLEngine createServerSSLEngine() {
        SSLEngine engine = newEngine();
        engine.setUseClientMode(false);
        if (sslRequireMutualAuthentication) {
            engine.setNeedClientAuth(true);
        }
        return engine;
    }

    @Override
    public SSLEngine createClientSSLEngine() {
        SSLEngine engine = newEngine();
        engine.setUseClientMode(true);
        return engine;
    }

    private SSLEngine newEngine() {
        SSLEngine engine = getSslContext().createSSLEngine();
        engine.setEnabledCipherSuites(sslEnabledAlgorithms);
        engine.setEnabledProtocols(sslProtocols);
        return engine;
    }

    private synchronized SSLContext getSslContext() {
        if (sslContext == null) {
            try {
                SSLContext context = SSLContext.getInstance("TLS");
                context.init(keyManagers(), trustManagers(), createSecureRandom());
                sslContext = context;
            } catch (GeneralSecurityException e) {
                throw new RemoteTransportException(
                        "Server SSL connection could not be established because SSL context could not be constructed",
                        e);
            }
        }
        return sslContext;
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

            trustManagerFactory.init(
                    loadKeystore(sslTrustStore, sslTrustStorePassword, sslTrustStoreType));
            return trustManagerFactory.getTrustManagers();
        } catch (GeneralSecurityException | IOException e) {
            // replicate exception handling from SSLEngineProvider
            throw new RemoteTransportException(
                    "Server SSL connection could not be established because SSL context could not be constructed",
                    e);
        }
    }

    @Override
    public KeyStore loadKeystore(String filename, String password) {
        try {
            return loadKeystore(filename, password, sslKeyStoreType);
        } catch (IOException | GeneralSecurityException e) {
            throw new RemoteTransportException(
                    "Server SSL connection could not be established because key store could not be loaded",
                    e);
        }
    }

    private KeyStore loadKeystore(String filename, String password, String keystoreType)
            throws IOException, GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        try (InputStream fin = Files.newInputStream(Paths.get(filename))) {
            char[] passwordCharArray = password.toCharArray();
            keyStore.load(fin, passwordCharArray);
        }
        return keyStore;
    }
}
