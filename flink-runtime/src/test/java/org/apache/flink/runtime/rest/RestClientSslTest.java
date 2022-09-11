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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SniHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Ssl cases for {@link RestClient} and {@link RestServerEndpoint}. */
public class RestClientSslTest extends TestLogger {

    private static final Time timeout = Time.seconds(10L);

    private static final String TRUST_STORE_PATH =
            RestClientSslTest.class.getResource("/local127.truststore").getFile();

    private static final String KEY_STORE_PATH =
            RestClientSslTest.class.getResource("/local127.keystore").getFile();

    private static final String HOST_SSL = "flink.local";

    private static final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private static final NioEventLoopGroup workerGroup = new NioEventLoopGroup(1);

    private final Configuration config;

    private RestClient restClient;

    private ServerBootstrap serverEndpoint;

    private static String sniHostReceived;

    static {
        // Force to load fake hostname resolution for tests to pass
        try {
            List<sun.net.spi.nameservice.NameService> nameServices =
                    (List<sun.net.spi.nameservice.NameService>)
                            org.apache.commons.lang3.reflect.FieldUtils.readStaticField(
                                    InetAddress.class, "nameServices", true);
            nameServices.add(new LocalHostNameService());
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public RestClientSslTest() {
        this.config = getBaseConfig();
    }

    private static Configuration getBaseConfig() {
        final Configuration sslConfig = new Configuration();
        sslConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        sslConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);
        sslConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "password");
        return sslConfig;
    }

    @BeforeEach
    public void setup() throws Exception {
        serverEndpoint = setUpServer(HOST_SSL, 8443);
        restClient = new RestClient(config, Executors.newFixedThreadPool(2));
    }

    @AfterEach
    public void teardown() throws Exception {
        if (restClient != null) {
            restClient.shutdown(timeout);
            restClient = null;
        }

        if (serverEndpoint != null) {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
            serverEndpoint = null;
        }
    }

    @Test
    void testRestClientSendSNI() throws Exception {
        restClient
                .sendRequest(
                        HOST_SSL,
                        8443,
                        new TestHeaders(),
                        EmptyMessageParameters.getInstance(),
                        new TestRequest())
                .get(10, TimeUnit.SECONDS);
        assertEquals(sniHostReceived, HOST_SSL);
    }

    /**
     * Sets up a server channel bound to the local interface for a given host and port.
     *
     * @throws Exception
     */
    public static ServerBootstrap setUpServer(String inetHost, int inetPort) throws Exception {
        // Retreive certificate from KeyStore
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile = Files.newInputStream(new File(KEY_STORE_PATH).toPath())) {
            keyStore.load(keyStoreFile, "password".toCharArray());
        }
        KeyStore.PrivateKeyEntry pkEntry =
                (KeyStore.PrivateKeyEntry)
                        keyStore.getEntry(
                                "flink.test",
                                new KeyStore.PasswordProtection("password".toCharArray()));
        ChannelHandler handlers =
                getServerHandler(
                        false, pkEntry.getPrivateKey(), (X509Certificate) pkEntry.getCertificate());

        ChannelInitializer<SocketChannel> serverInitializer =
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // Add the ssl handler
                        ch.pipeline().addLast(handlers);
                        ch.pipeline().addLast(new HttpRequestDecoder());
                        ch.pipeline().addLast(new HttpResponseEncoder());
                        ch.pipeline().addLast(new EmptyHttpServerHandler());
                    }
                };

        ServerBootstrap sb =
                new ServerBootstrap()
                        .group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(serverInitializer);
        sb.bind(inetHost, inetPort).syncUninterruptibly();
        return sb;
    }

    private static ChannelHandler getServerHandler(
            boolean requireClientCert, PrivateKey privateKey, X509Certificate certificate)
            throws Exception {
        SslContext sslContext =
                SslContextBuilder.forServer(privateKey, certificate)
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .clientAuth(ClientAuth.NONE)
                        .build();
        return new SniHandler(
                hostname -> {
                    sniHostReceived = hostname;
                    return sslContext;
                });
    }

    /** A handler that send an empty json string. */
    private static class EmptyHttpServerHandler extends ChannelInboundHandlerAdapter {

        StringBuilder responseData = new StringBuilder();

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof HttpRequest || msg instanceof HttpContent)) {
                throw new RuntimeException("handle only message of type HttpRequest & HttpContent");
            }
            responseData.append("{}");
            FullHttpResponse httpResponse =
                    new DefaultFullHttpResponse(
                            HTTP_1_1,
                            OK,
                            Unpooled.copiedBuffer(responseData.toString(), CharsetUtil.UTF_8));
            httpResponse
                    .headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, "aplication/json; charset=UTF-8");
            httpResponse
                    .headers()
                    .setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());

            ctx.write(httpResponse);
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    /** Name Service to add fake DNS entry. */
    @SuppressWarnings("restriction")
    public static class LocalHostNameService implements sun.net.spi.nameservice.NameService {
        @Override
        public InetAddress[] lookupAllHostAddr(String paramString) throws UnknownHostException {
            if (HOST_SSL.equals(paramString)) {
                final byte[] arrayOfByte =
                        sun.net.util.IPAddressUtil.textToNumericFormatV4("127.0.0.1");
                final InetAddress address = InetAddress.getByAddress(paramString, arrayOfByte);
                return new InetAddress[] {address};
            } else {
                throw new UnknownHostException();
            }
        }

        @Override
        public String getHostByAddr(byte[] paramArrayOfByte) throws UnknownHostException {
            throw new UnknownHostException();
        }
    }

    private static class TestRequest implements RequestBody {}

    private static class TestResponse implements ResponseBody {}

    private static class TestHeaders
            implements RuntimeMessageHeaders<TestRequest, TestResponse, EmptyMessageParameters> {

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/";
        }

        @Override
        public Class<TestRequest> getRequestClass() {
            return TestRequest.class;
        }

        @Override
        public Class<TestResponse> getResponseClass() {
            return TestResponse.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }
    }
}
