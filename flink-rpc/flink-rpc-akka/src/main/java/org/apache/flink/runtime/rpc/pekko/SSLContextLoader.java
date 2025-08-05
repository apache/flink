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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.core.security.watch.LocalFSWatchServiceListener;

import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

import com.typesafe.config.Config;
import org.apache.pekko.remote.RemoteTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SSLContextLoader implements LocalFSWatchServiceListener {

    private static final Logger LOG = LoggerFactory.getLogger(SSLContextLoader.class);

    private final String sslTrustStore;
    private final String sslTrustStorePassword;
    private final List<String> sslCertFingerprints;
    private final String sslKeyStoreType;
    private final String sslTrustStoreType;
    private final String sslProtocol;
    private final String sslKeyStore;
    private final String sslKeyStorePassword;
    private final String sslKeyPassword;
    private final String sslRandomNumberGenerator;

    private final AtomicBoolean toReload = new AtomicBoolean(false);

    private volatile SSLContext sslContext;

    public SSLContextLoader(String sslTrustStore, String sslProtocol, Config securityConfig) {
        this.sslTrustStore = sslTrustStore;
        this.sslProtocol = sslProtocol;

        this.sslTrustStorePassword = securityConfig.getString("trust-store-password");
        this.sslCertFingerprints = securityConfig.getStringList("cert-fingerprints");
        this.sslKeyStoreType = securityConfig.getString("key-store-type");
        this.sslTrustStoreType = securityConfig.getString("trust-store-type");
        this.sslKeyStore = securityConfig.getString("key-store");
        sslKeyStorePassword = securityConfig.getString("key-store-password");
        sslKeyPassword = securityConfig.getString("key-password");
        sslRandomNumberGenerator = securityConfig.getString("random-number-generator");

        loadSSLContext();
    }

    void loadSSLContext() {
        SSLContext ctx;
        try {
            LOG.debug("Loading SSL context for pekko");
            SecureRandom rng = createSecureRandom();
            ctx = SSLContext.getInstance(sslProtocol);
            ctx.init(keyManagers(), trustManagers(), rng);
        } catch (KeyManagementException
                | NoSuchAlgorithmException
                | UnrecoverableKeyException
                | KeyStoreException e) {
            throw new RuntimeException("Cannot load SSL context", e);
        }

        this.sslContext = ctx;
    }

    public SSLEngine createSSLEngine() {
        reloadContextIfNeeded();
        return sslContext.createSSLEngine();
    }

    public SecureRandom createSecureRandom() throws NoSuchAlgorithmException {
        SecureRandom rng;
        if ("".equals(sslRandomNumberGenerator)) {
            rng = new SecureRandom();
        } else {
            rng = SecureRandom.getInstance(sslRandomNumberGenerator);
        }
        rng.nextInt();
        return rng;
    }

    @Override
    public void onFileOrDirectoryModified(Path relativePath) {
        toReload.set(true);
    }

    private synchronized void reloadContextIfNeeded() {
        if (toReload.compareAndSet(true, false)) {
            loadSSLContext();
        }
    }

    /** Subclass may override to customize `KeyManager`. */
    private KeyManager[] keyManagers()
            throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
        KeyManagerFactory factory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        factory.init(loadKeystore(sslKeyStore, sslKeyStorePassword), sslKeyPassword.toCharArray());
        return factory.getKeyManagers();
    }

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
