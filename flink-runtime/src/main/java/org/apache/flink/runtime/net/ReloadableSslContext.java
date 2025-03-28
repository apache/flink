/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ApplicationProtocolNegotiator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.JdkSslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManagerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.flink.runtime.net.SSLUtils.getEnabledCipherSuites;
import static org.apache.flink.runtime.net.SSLUtils.getEnabledProtocols;
import static org.apache.flink.runtime.net.SSLUtils.getKeyManagerFactory;
import static org.apache.flink.runtime.net.SSLUtils.getTrustManagerFactory;

/** SSL context which is able to reload keystore. */
public class ReloadableSslContext extends SslContext implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ReloadableSslContext.class);

    protected final Configuration config;
    protected final boolean clientMode;
    protected final ClientAuth clientAuth;
    protected final SslProvider provider;
    protected volatile SslContext sslContext;

    public ReloadableSslContext(
            Configuration config, boolean clientMode, ClientAuth clientAuth, SslProvider provider)
            throws Exception {
        this.config = config;
        this.clientMode = clientMode;
        this.clientAuth = clientAuth;
        this.provider = provider;
        loadContext();
    }

    public SSLContext getSSLContext() {
        return ((JdkSslContext) this.sslContext).context();
    }

    @Override
    public boolean isClient() {
        return sslContext.isClient();
    }

    @Override
    public List<String> cipherSuites() {
        return sslContext.cipherSuites();
    }

    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return sslContext.applicationProtocolNegotiator();
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator) {
        return sslContext.newEngine(byteBufAllocator);
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator byteBufAllocator, String s, int i) {
        return sslContext.newEngine(byteBufAllocator, s, i);
    }

    @Override
    public SSLSessionContext sessionContext() {
        return sslContext.sessionContext();
    }

    @Override
    public Void call() throws Exception {
        loadContext();
        return null;
    }

    public void reload() throws Exception {
        loadContext();
    }

    protected void loadContext() throws Exception {
        LOG.info("Loading SSL context from {}", config);

        String[] sslProtocols = getEnabledProtocols(config);
        List<String> ciphers = Arrays.asList(getEnabledCipherSuites(config));

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
            Optional<TrustManagerFactory> tmf = getTrustManagerFactory(config, false);
            tmf.map(
                    // Use specific ciphers and protocols if SSL is configured with self-signed
                    // certificates (user-supplied truststore)
                    tm ->
                            sslContextBuilder
                                    .trustManager(tm)
                                    .protocols(sslProtocols)
                                    .ciphers(ciphers)
                                    .clientAuth(clientAuth));
        }

        sslContext = sslContextBuilder.sslProvider(provider).build();
    }
}
