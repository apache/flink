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
import org.apache.flink.configuration.SecurityOptions;

import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.net.SSLUtils.getEnabledCipherSuites;
import static org.apache.flink.runtime.net.SSLUtils.getEnabledProtocols;
import static org.apache.flink.runtime.net.SSLUtils.getKeyManagerFactory;
import static org.apache.flink.runtime.net.SSLUtils.getTrustManagerFactory;

/** JDK SSL context which is able to reload keystore. */
public class ReloadableJdkSslContext extends ReloadableSslContext {

    private static final Logger LOG = LoggerFactory.getLogger(ReloadableJdkSslContext.class);

    public ReloadableJdkSslContext(Configuration config, boolean clientMode, SslProvider provider)
            throws Exception {
        super(config, clientMode, ClientAuth.NONE, provider);
    }

    @Override
    protected void loadContext() throws Exception {
        LOG.info("Loading JDK SSL context from {}", this.config);

        String[] sslProtocols = getEnabledProtocols(config);
        List<String> ciphers = Arrays.asList(getEnabledCipherSuites(config));
        int sessionCacheSize = config.get(SecurityOptions.SSL_INTERNAL_SESSION_CACHE_SIZE);
        int sessionTimeoutMs = config.get(SecurityOptions.SSL_INTERNAL_SESSION_TIMEOUT);

        KeyManagerFactory kmf = getKeyManagerFactory(config, true, provider);
        ClientAuth clientAuth = ClientAuth.REQUIRE;

        final SslContextBuilder sslContextBuilder;
        if (clientMode) {
            sslContextBuilder = SslContextBuilder.forClient().keyManager(kmf);
        } else {
            sslContextBuilder = SslContextBuilder.forServer(kmf);
        }

        Optional<TrustManagerFactory> tmf = getTrustManagerFactory(config, true);
        tmf.map(sslContextBuilder::trustManager);

        this.sslContext =
                sslContextBuilder
                        .sslProvider(provider)
                        .protocols(sslProtocols)
                        .ciphers(ciphers)
                        .clientAuth(clientAuth)
                        .sessionCacheSize(sessionCacheSize)
                        .sessionTimeout(sessionTimeoutMs / 1000)
                        .build();
    }
}
