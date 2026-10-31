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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.MultiThreadIoEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioIoHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import java.io.File;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link CustomSSLEngineProvider} correctly negotiates TLS when {@link
 * SecurityOptions#SSL_PROTOCOL} is configured with a comma-separated protocol list, and when the
 * store passwords contain characters that must be escaped in the Pekko config.
 *
 * <p>This is a regression test for the fact that {@link
 * org.apache.pekko.remote.transport.netty.ConfigSSLEngineProvider}, which {@link
 * CustomSSLEngineProvider} extends, only supports a single protocol name: it feeds the configured
 * string, unsplit, into both {@code SSLContext.getInstance(String)} and {@code
 * SSLEngine#setEnabledProtocols(String[])}.
 */
class CustomSSLEngineProviderTest {

    private static final String KEY_STORE_PATH =
            checkNotNull(CustomSSLEngineProviderTest.class.getResource("/rpc.keystore")).getFile();
    private static final String TRUST_STORE_PATH =
            checkNotNull(CustomSSLEngineProviderTest.class.getResource("/rpc.truststore"))
                    .getFile();
    private static final String STORE_PASSWORD = "password";

    private static final String TLS_12_CIPHER =
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
    private static final String TLS_13_CIPHER = "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384";

    private ActorSystem actorSystem;

    @AfterEach
    void shutdown() throws Exception {
        if (actorSystem != null) {
            ScalaFutureUtils.toJava(actorSystem.terminate()).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void negotiatesHighestCommonProtocolFromList() throws Exception {
        SSLSession session = handshake("TLSv1.2,TLSv1.3", TLS_12_CIPHER + "," + TLS_13_CIPHER);

        assertThat(session.getProtocol()).isEqualTo("TLSv1.3");
    }

    @Test
    void fallsBackToLowerListedProtocolWhenHigherOneHasNoUsableCipher() throws Exception {
        SSLSession session = handshake("TLSv1.2,TLSv1.3", TLS_12_CIPHER);

        assertThat(session.getProtocol()).isEqualTo("TLSv1.2");
    }

    @Test
    void handshakeWithSpecialCharactersInPasswords(@TempDir Path tempDir) throws Exception {
        final String password = "pa\"ss\\word";
        final Path keyStore =
                copyWithNewPassword(KEY_STORE_PATH, tempDir.resolve("rpc.keystore"), password);
        final Path trustStore =
                copyWithNewPassword(TRUST_STORE_PATH, tempDir.resolve("rpc.truststore"), password);

        SSLSession session =
                handshake(
                        keyStore.toString(),
                        trustStore.toString(),
                        password,
                        "TLSv1.3",
                        TLS_13_CIPHER);

        assertThat(session.getProtocol()).isEqualTo("TLSv1.3");
    }

    private static Path copyWithNewPassword(String source, Path target, String newPassword)
            throws Exception {
        final char[] oldPassword = STORE_PASSWORD.toCharArray();
        final KeyStore keyStore = KeyStore.getInstance(new File(source), oldPassword);
        for (String alias : Collections.list(keyStore.aliases())) {
            if (keyStore.isKeyEntry(alias)) {
                keyStore.setKeyEntry(
                        alias,
                        keyStore.getKey(alias, oldPassword),
                        newPassword.toCharArray(),
                        keyStore.getCertificateChain(alias));
            }
        }
        try (OutputStream out = Files.newOutputStream(target)) {
            keyStore.store(out, newPassword.toCharArray());
        }
        return target;
    }

    private SSLSession handshake(String protocolList, String ciphers) throws Exception {
        return handshake(KEY_STORE_PATH, TRUST_STORE_PATH, STORE_PASSWORD, protocolList, ciphers);
    }

    /**
     * Builds a {@link CustomSSLEngineProvider} from the given stores, protocol list and ciphers,
     * performs a real, socket-based TLS handshake between a server and a client engine it creates,
     * and returns the client's negotiated session.
     */
    private SSLSession handshake(
            String keyStore,
            String trustStore,
            String password,
            String protocolList,
            String ciphers)
            throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEYSTORE, keyStore);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, password);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, password);
        configuration.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE, trustStore);
        configuration.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, password);
        configuration.set(SecurityOptions.SSL_PROTOCOL, protocolList);
        configuration.set(SecurityOptions.SSL_ALGORITHMS, ciphers);

        actorSystem =
                PekkoUtils.createActorSystem(
                        "CustomSSLEngineProviderTest",
                        PekkoUtils.getConfig(configuration, new HostAndPort("localhost", 0)));

        final CustomSSLEngineProvider provider = new CustomSSLEngineProvider(actorSystem);

        final EventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        try {
            final CompletableFuture<SSLSession> serverSession = new CompletableFuture<>();
            final CompletableFuture<SSLSession> clientSession = new CompletableFuture<>();

            final ServerBootstrap serverBootstrap =
                    new ServerBootstrap()
                            .group(group)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(
                                    new ChannelInitializer<Channel>() {
                                        @Override
                                        protected void initChannel(Channel ch) {
                                            SSLEngine engine = provider.createServerSSLEngine();
                                            SslHandler handler = new SslHandler(engine);
                                            ch.pipeline().addLast(handler);
                                            completeWithSession(handler, serverSession);
                                        }
                                    });
            final Channel serverChannel = serverBootstrap.bind(0).sync().channel();
            final int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

            final Bootstrap clientBootstrap =
                    new Bootstrap()
                            .group(group)
                            .channel(NioSocketChannel.class)
                            .handler(
                                    new ChannelInitializer<Channel>() {
                                        @Override
                                        protected void initChannel(Channel ch) {
                                            SSLEngine engine = provider.createClientSSLEngine();
                                            SslHandler handler = new SslHandler(engine);
                                            ch.pipeline().addLast(handler);
                                            completeWithSession(handler, clientSession);
                                        }
                                    });
            clientBootstrap.connect("localhost", port).sync();

            final SSLSession client = clientSession.get(10, TimeUnit.SECONDS);
            final SSLSession server = serverSession.get(10, TimeUnit.SECONDS);
            assertThat(client.getProtocol()).isEqualTo(server.getProtocol());
            assertThat(client.getCipherSuite()).isEqualTo(server.getCipherSuite());

            serverChannel.close().sync();
            return client;
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        }
    }

    private static void completeWithSession(
            SslHandler handler, CompletableFuture<SSLSession> future) {
        handler.handshakeFuture()
                .addListener(
                        f -> {
                            if (f.isSuccess()) {
                                future.complete(handler.engine().getSession());
                            } else {
                                future.completeExceptionally(f.cause());
                            }
                        });
    }
}
