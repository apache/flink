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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava30.com.google.common.base.Suppliers;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.JdkSslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.OpenSsl;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.OpenSslX509KeyManagerFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider.JDK;
import static org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider.OPENSSL;
import static org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider.OPENSSL_REFCNT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Common utilities to manage SSL transport settings. */
public class SSLUtils {

    /**
     * Creates a factory for SSL Server Sockets from the given configuration. SSL Server Sockets are
     * always part of internal communication.
     */
    public static ServerSocketFactory createSSLServerSocketFactory(Configuration config)
            throws Exception {
        Supplier<SSLContext> sslContextSupplier = createInternalSSLContextSupplier(config, false);
        if (sslContextSupplier == null) {
            throw new IllegalConfigurationException("SSL is not enabled");
        }

        String[] protocols = getEnabledProtocols(config);
        String[] cipherSuites = getEnabledCipherSuites(config);

        Supplier<SSLServerSocketFactory> factorySupplier =
                () -> sslContextSupplier.get().getServerSocketFactory();

        return new ConfiguringSSLServerSocketFactory(factorySupplier, protocols, cipherSuites);
    }

    /**
     * Creates a factory for SSL Client Sockets from the given configuration. SSL Client Sockets are
     * always part of internal communication.
     */
    public static SocketFactory createSSLClientSocketFactory(Configuration config)
            throws Exception {
        Supplier<SSLContext> sslContextSupplier = createInternalSSLContextSupplier(config, true);
        if (sslContextSupplier == null) {
            throw new IllegalConfigurationException("SSL is not enabled");
        }

        return sslContextSupplier.get().getSocketFactory();
    }

    /** Creates a SSLEngineFactory to be used by internal communication server endpoints. */
    public static SSLHandlerFactory createInternalServerSSLEngineFactory(final Configuration config)
            throws Exception {
        Supplier<SslContext> sslContextSupplier =
                createInternalNettySSLContextSupplier(config, false);
        if (sslContextSupplier == null) {
            throw new IllegalConfigurationException(
                    "SSL is not enabled for internal communication.");
        }

        return new SSLHandlerFactory(
                sslContextSupplier,
                config.getInteger(SecurityOptions.SSL_INTERNAL_HANDSHAKE_TIMEOUT),
                config.getInteger(SecurityOptions.SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT));
    }

    /** Creates a SSLEngineFactory to be used by internal communication client endpoints. */
    public static SSLHandlerFactory createInternalClientSSLEngineFactory(final Configuration config)
            throws Exception {
        Supplier<SslContext> sslContextSupplier =
                createInternalNettySSLContextSupplier(config, true);
        if (sslContextSupplier == null) {
            throw new IllegalConfigurationException(
                    "SSL is not enabled for internal communication.");
        }

        return new SSLHandlerFactory(
                sslContextSupplier,
                config.getInteger(SecurityOptions.SSL_INTERNAL_HANDSHAKE_TIMEOUT),
                config.getInteger(SecurityOptions.SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT));
    }

    private static Supplier<SslContext> createUserSslContextSupplier(
            final Configuration configuration,
            String className,
            final boolean clientMode,
            final SslProvider sslProvider,
            ClassLoader classLoader)
            throws Exception {

        final SslContextSupplier supplier =
                InstantiationUtil.instantiate(className, SslContextSupplier.class, classLoader);

        return () -> supplier.get(configuration, clientMode, sslProvider);
    }

    /**
     * Creates a {@link SSLHandlerFactory} to be used by the REST Servers.
     *
     * @param config The application configuration.
     */
    public static SSLHandlerFactory createRestServerSSLEngineFactory(final Configuration config)
            throws Exception {
        ClientAuth clientAuth =
                SecurityOptions.isRestSSLAuthenticationEnabled(config)
                        ? ClientAuth.REQUIRE
                        : ClientAuth.NONE;
        Supplier<SslContext> sslContextSupplier =
                createRestNettySSLContextSupplier(config, false, clientAuth);
        if (sslContextSupplier == null) {
            throw new IllegalConfigurationException("SSL is not enabled for REST endpoints.");
        }

        return new SSLHandlerFactory(sslContextSupplier, -1, -1);
    }

    /**
     * Creates a {@link SSLHandlerFactory} to be used by the REST Clients.
     *
     * @param config The application configuration.
     */
    public static SSLHandlerFactory createRestClientSSLEngineFactory(final Configuration config)
            throws Exception {
        ClientAuth clientAuth =
                SecurityOptions.isRestSSLAuthenticationEnabled(config)
                        ? ClientAuth.REQUIRE
                        : ClientAuth.NONE;
        Supplier<SslContext> sslContextSupplier =
                createRestNettySSLContextSupplier(config, true, clientAuth);
        if (sslContextSupplier == null) {
            throw new IllegalConfigurationException("SSL is not enabled for REST endpoints.");
        }

        return new SSLHandlerFactory(sslContextSupplier, -1, -1);
    }

    private static String[] getEnabledProtocols(final Configuration config) {
        checkNotNull(config, "config must not be null");
        return config.getString(SecurityOptions.SSL_PROTOCOL).split(",");
    }

    private static String[] getEnabledCipherSuites(final Configuration config) {
        checkNotNull(config, "config must not be null");
        return config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");
    }

    @VisibleForTesting
    static SslProvider getSSLProvider(final Configuration config) {
        checkNotNull(config, "config must not be null");
        String providerString = config.getString(SecurityOptions.SSL_PROVIDER);
        if (providerString.equalsIgnoreCase("OPENSSL")) {
            if (OpenSsl.isAvailable()) {
                return OPENSSL;
            } else {
                throw new IllegalConfigurationException(
                        "openSSL not available", OpenSsl.unavailabilityCause());
            }
        } else if (providerString.equalsIgnoreCase("JDK")) {
            return JDK;
        } else {
            throw new IllegalConfigurationException("Unknown SSL provider: %s", providerString);
        }
    }

    private static TrustManagerFactory getTrustManagerFactory(
            Configuration config, boolean internal)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        String trustStoreFilePath =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_TRUSTSTORE
                                : SecurityOptions.SSL_REST_TRUSTSTORE,
                        SecurityOptions.SSL_TRUSTSTORE);

        String trustStorePassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD
                                : SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD,
                        SecurityOptions.SSL_TRUSTSTORE_PASSWORD);

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream trustStoreFile =
                Files.newInputStream(new File(trustStoreFilePath).toPath())) {
            trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
        }

        String certFingerprint =
                config.getString(
                        internal
                                ? SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT
                                : SecurityOptions.SSL_REST_CERT_FINGERPRINT);

        TrustManagerFactory tmf;
        if (StringUtils.isNullOrWhitespaceOnly(certFingerprint)) {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        } else {
            tmf = new FingerprintTrustManagerFactory(certFingerprint.split(","));
        }

        tmf.init(trustStore);

        return tmf;
    }

    private static KeyManagerFactory getKeyManagerFactory(
            Configuration config, boolean internal, SslProvider provider)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
                    UnrecoverableKeyException {
        String keystoreFilePath =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEYSTORE
                                : SecurityOptions.SSL_REST_KEYSTORE,
                        SecurityOptions.SSL_KEYSTORE);

        String keystorePassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD
                                : SecurityOptions.SSL_REST_KEYSTORE_PASSWORD,
                        SecurityOptions.SSL_KEYSTORE_PASSWORD);

        String certPassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEY_PASSWORD
                                : SecurityOptions.SSL_REST_KEY_PASSWORD,
                        SecurityOptions.SSL_KEY_PASSWORD);

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile = Files.newInputStream(new File(keystoreFilePath).toPath())) {
            keyStore.load(keyStoreFile, keystorePassword.toCharArray());
        }

        final KeyManagerFactory kmf;
        if (provider == OPENSSL || provider == OPENSSL_REFCNT) {
            kmf = new OpenSslX509KeyManagerFactory();
        } else {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }
        kmf.init(keyStore, certPassword.toCharArray());

        return kmf;
    }

    /**
     * Creates the SSL Context for internal SSL, if internal SSL is configured. For internal SSL,
     * the client and server side configuration are identical, because of mutual authentication.
     */
    @Nullable
    private static Supplier<SSLContext> createInternalSSLContextSupplier(
            Configuration config, boolean clientMode) throws Exception {
        Supplier<SslContext> nettySslContextSupplier =
                createInternalNettySSLContextSupplier(config, clientMode, JDK);

        if (nettySslContextSupplier != null) {
            return () -> ((JdkSslContext) nettySslContextSupplier.get()).context();
        } else {
            return null;
        }
    }

    @Nullable
    private static Supplier<SslContext> createInternalNettySSLContextSupplier(
            Configuration config, boolean clientMode) throws Exception {
        return createInternalNettySSLContextSupplier(config, clientMode, getSSLProvider(config));
    }

    /**
     * Creates the SSL Context for internal SSL, if internal SSL is configured. For internal SSL,
     * the client and server side configuration are identical, because of mutual authentication.
     */
    @Nullable
    private static Supplier<SslContext> createInternalNettySSLContextSupplier(
            Configuration config, boolean clientMode, SslProvider provider) throws Exception {
        checkNotNull(config, "config");

        if (!SecurityOptions.isInternalSSLEnabled(config)) {
            return null;
        }

        String sslContextSupplierClassName =
                config.get(SecurityOptions.SSL_INTERNAL_SSL_CONTEXT_SUPPLIER);
        if (sslContextSupplierClassName != null) {
            return createUserSslContextSupplier(
                    config,
                    sslContextSupplierClassName,
                    clientMode,
                    provider,
                    SSLUtils.class.getClassLoader());
        }

        String[] sslProtocols = getEnabledProtocols(config);
        List<String> ciphers = Arrays.asList(getEnabledCipherSuites(config));
        int sessionCacheSize = config.getInteger(SecurityOptions.SSL_INTERNAL_SESSION_CACHE_SIZE);
        int sessionTimeoutMs = config.getInteger(SecurityOptions.SSL_INTERNAL_SESSION_TIMEOUT);

        KeyManagerFactory kmf = getKeyManagerFactory(config, true, provider);
        TrustManagerFactory tmf = getTrustManagerFactory(config, true);
        ClientAuth clientAuth = ClientAuth.REQUIRE;

        final SslContextBuilder sslContextBuilder;
        if (clientMode) {
            sslContextBuilder = SslContextBuilder.forClient().keyManager(kmf);
        } else {
            sslContextBuilder = SslContextBuilder.forServer(kmf);
        }

        SslContext context =
                sslContextBuilder
                        .sslProvider(provider)
                        .protocols(sslProtocols)
                        .ciphers(ciphers)
                        .trustManager(tmf)
                        .clientAuth(clientAuth)
                        .sessionCacheSize(sessionCacheSize)
                        .sessionTimeout(sessionTimeoutMs / 1000)
                        .build();

        return Suppliers.ofInstance(context);
    }

    /** Creates an SSL context for clients against the external REST endpoint. */
    @Nullable
    @VisibleForTesting
    public static SSLContext createRestSSLContext(Configuration config, boolean clientMode)
            throws Exception {
        ClientAuth clientAuth =
                SecurityOptions.isRestSSLAuthenticationEnabled(config)
                        ? ClientAuth.REQUIRE
                        : ClientAuth.NONE;
        Supplier<SslContext> nettySSLContextSupplier =
                createRestNettySSLContextSupplier(config, clientMode, clientAuth, JDK);
        if (nettySSLContextSupplier != null) {
            return ((JdkSslContext) nettySSLContextSupplier.get()).context();
        } else {
            return null;
        }
    }

    @Nullable
    private static Supplier<SslContext> createRestNettySSLContextSupplier(
            Configuration config, boolean clientMode, ClientAuth clientAuth) throws Exception {
        return createRestNettySSLContextSupplier(
                config, clientMode, clientAuth, getSSLProvider(config));
    }

    /**
     * Creates an SSL context for the external REST SSL. If mutual authentication is configured the
     * client and the server side configuration are identical.
     */
    @Nullable
    public static SslContext createRestNettySSLContext(
            Configuration config, boolean clientMode, ClientAuth clientAuth, SslProvider provider)
            throws Exception {
        Supplier<SslContext> sslContextSupplier =
                createRestNettySSLContextSupplier(config, clientMode, clientAuth, provider);
        if (sslContextSupplier == null) {
            return null;
        } else {
            return sslContextSupplier.get();
        }
    }

    @Nullable
    private static Supplier<SslContext> createRestNettySSLContextSupplier(
            Configuration config, boolean clientMode, ClientAuth clientAuth, SslProvider provider)
            throws Exception {
        checkNotNull(config, "config");

        if (!SecurityOptions.isRestSSLEnabled(config)) {
            return null;
        }

        final String sslContextSupplierClassName =
                config.get(SecurityOptions.SSL_REST_SSL_CONTEXT_SUPPLIER);
        if (sslContextSupplierClassName != null) {
            return createUserSslContextSupplier(
                    config,
                    sslContextSupplierClassName,
                    clientMode,
                    provider,
                    SSLUtils.class.getClassLoader());
        }

        String[] sslProtocols = getEnabledProtocols(config);

        final SslContextBuilder sslContextBuilder;
        if (clientMode) {
            sslContextBuilder = SslContextBuilder.forClient();
            if (clientAuth != ClientAuth.NONE) {
                KeyManagerFactory kmf = getKeyManagerFactory(config, false, provider);
                sslContextBuilder.keyManager(kmf);
            }
        } else {
            KeyManagerFactory kmf = getKeyManagerFactory(config, false, provider);
            sslContextBuilder = SslContextBuilder.forServer(kmf);
        }

        if (clientMode || clientAuth != ClientAuth.NONE) {
            TrustManagerFactory tmf = getTrustManagerFactory(config, false);
            sslContextBuilder.trustManager(tmf);
        }

        SslContext sslContext =
                sslContextBuilder
                        .sslProvider(provider)
                        .protocols(sslProtocols)
                        .clientAuth(clientAuth)
                        .build();
        return Suppliers.ofInstance(sslContext);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static String getAndCheckOption(
            Configuration config,
            ConfigOption<String> primaryOption,
            ConfigOption<String> fallbackOption) {
        String value = config.getString(primaryOption, config.getString(fallbackOption));
        if (value != null) {
            return value;
        } else {
            throw new IllegalConfigurationException(
                    "The config option "
                            + primaryOption.key()
                            + " or "
                            + fallbackOption.key()
                            + " is missing.");
        }
    }

    // ------------------------------------------------------------------------
    //  Wrappers for socket factories that additionally configure the sockets
    // ------------------------------------------------------------------------

    private static class ConfiguringSSLServerSocketFactory extends ServerSocketFactory {

        private final Supplier<SSLServerSocketFactory> sslServerSocketFactorySupplier;
        private final String[] protocols;
        private final String[] cipherSuites;

        ConfiguringSSLServerSocketFactory(
                Supplier<SSLServerSocketFactory> sslServerSocketFactorySupplier,
                String[] protocols,
                String[] cipherSuites) {

            this.sslServerSocketFactorySupplier = sslServerSocketFactorySupplier;
            this.protocols = protocols;
            this.cipherSuites = cipherSuites;
        }

        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            SSLServerSocket socket =
                    (SSLServerSocket) sslServerSocketFactorySupplier.get().createServerSocket(port);
            configureServerSocket(socket);
            return socket;
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            SSLServerSocket socket =
                    (SSLServerSocket)
                            sslServerSocketFactorySupplier.get().createServerSocket(port, backlog);
            configureServerSocket(socket);
            return socket;
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress)
                throws IOException {
            SSLServerSocket socket =
                    (SSLServerSocket)
                            sslServerSocketFactorySupplier
                                    .get()
                                    .createServerSocket(port, backlog, ifAddress);
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
