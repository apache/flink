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
package org.apache.beam.sdk.fn.server;

import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.channel.SocketAddressFactory;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.BindableService;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ServerInterceptors;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.netty.NettyServerBuilder;
import org.apache.beam.vendor.grpc.v1p48p1.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.beam.vendor.grpc.v1p48p1.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import org.apache.beam.vendor.grpc.v1p48p1.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.beam.vendor.grpc.v1p48p1.io.netty.channel.unix.DomainSocketAddress;
import org.apache.beam.vendor.grpc.v1p48p1.io.netty.util.internal.ThreadLocalRandom;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

// This class is copied from Beam's org.apache.beam.sdk.fn.server.ServerFactory,
// can be removed after https://github.com/apache/beam/issues/21598 is fixed.
//
// Changed lines: 53~55

/** A {@link Server gRPC server} factory. */
public abstract class ServerFactory {

    // The BDP ping period is locally-decided and the keep alive time is 20 seconds in client
    // side, so we choose the server to allow pings every 19 seconds.
    private static final int KEEP_ALIVE_TIME_SEC = 19;

    /** Create a default {@link InetSocketAddressServerFactory}. */
    public static ServerFactory createDefault() {
        return new InetSocketAddressServerFactory(UrlFactory.createDefault());
    }

    /** Create a {@link InetSocketAddressServerFactory} that uses the given url factory. */
    public static ServerFactory createWithUrlFactory(UrlFactory urlFactory) {
        return new InetSocketAddressServerFactory(urlFactory);
    }

    /** Create a {@link InetSocketAddressServerFactory} that uses ports from a supplier. */
    public static ServerFactory createWithPortSupplier(Supplier<Integer> portSupplier) {
        return new InetSocketAddressServerFactory(UrlFactory.createDefault(), portSupplier);
    }

    /**
     * Create a {@link InetSocketAddressServerFactory} that uses the given url factory and ports
     * from a supplier.
     */
    public static ServerFactory createWithUrlFactoryAndPortSupplier(
            UrlFactory urlFactory, Supplier<Integer> portSupplier) {
        return new InetSocketAddressServerFactory(urlFactory, portSupplier);
    }

    /** Create a {@link EpollSocket}. */
    public static ServerFactory createEpollSocket() {
        return new EpollSocket();
    }

    /** Create a {@link EpollDomainSocket}. */
    public static ServerFactory createEpollDomainSocket() {
        return new EpollDomainSocket();
    }

    /**
     * Creates an instance of this server using an ephemeral address. The allocation of the address
     * is server type dependent, which means the address may be a port for certain type of server,
     * or a file path for other certain types. The chosen address is accessible to the caller from
     * the URL set in the input {@link Endpoints.ApiServiceDescriptor.Builder}. Server applies
     * {@link GrpcContextHeaderAccessorProvider#interceptor()} to all incoming requests.
     */
    public abstract Server allocateAddressAndCreate(
            List<BindableService> services, Endpoints.ApiServiceDescriptor.Builder builder)
            throws IOException;

    /**
     * Creates an instance of this server at the address specified by the given service descriptor
     * and bound to multiple services. Server applies {@link
     * GrpcContextHeaderAccessorProvider#interceptor()} to all incoming requests.
     */
    public abstract Server create(
            List<BindableService> services, Endpoints.ApiServiceDescriptor serviceDescriptor)
            throws IOException;
    /**
     * Creates a {@link Server gRPC Server} using the default server factory.
     *
     * <p>The server is created listening any open port on "localhost".
     */
    public static class InetSocketAddressServerFactory extends ServerFactory {
        private final UrlFactory urlFactory;
        private final Supplier<Integer> portSupplier;

        private InetSocketAddressServerFactory(UrlFactory urlFactory) {
            this(urlFactory, () -> 0);
        }

        private InetSocketAddressServerFactory(
                UrlFactory urlFactory, Supplier<Integer> portSupplier) {
            this.urlFactory = urlFactory;
            this.portSupplier = portSupplier;
        }

        @Override
        public Server allocateAddressAndCreate(
                List<BindableService> services,
                Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor)
                throws IOException {
            InetSocketAddress address =
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), portSupplier.get());
            Server server = createServer(services, address);
            apiServiceDescriptor.setUrl(
                    urlFactory.createUrl(address.getHostName(), server.getPort()));
            return server;
        }

        @Override
        public Server create(
                List<BindableService> services, Endpoints.ApiServiceDescriptor serviceDescriptor)
                throws IOException {
            SocketAddress socketAddress =
                    SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
            checkArgument(
                    socketAddress instanceof InetSocketAddress,
                    "%s %s requires a host:port socket address, got %s",
                    getClass().getSimpleName(),
                    ServerFactory.class.getSimpleName(),
                    serviceDescriptor.getUrl());
            return createServer(services, (InetSocketAddress) socketAddress);
        }

        private static Server createServer(List<BindableService> services, InetSocketAddress socket)
                throws IOException {
            NettyServerBuilder builder =
                    NettyServerBuilder.forPort(socket.getPort())
                            // Set the message size to max value here. The actual size is governed
                            // by the
                            // buffer size in the layers above.
                            .maxMessageSize(Integer.MAX_VALUE)
                            .permitKeepAliveTime(KEEP_ALIVE_TIME_SEC, TimeUnit.SECONDS);
            services.stream()
                    .forEach(
                            service ->
                                    builder.addService(
                                            ServerInterceptors.intercept(
                                                    service,
                                                    GrpcContextHeaderAccessorProvider
                                                            .interceptor())));
            return builder.build().start();
        }
    }

    /**
     * Creates a {@link Server gRPC Server} using a Unix domain socket. Note that this requires <a
     * href="http://netty.io/wiki/forked-tomcat-native.html">Netty TcNative</a> available to be able
     * to provide a {@link EpollServerDomainSocketChannel}.
     *
     * <p>The unix domain socket is located at ${java.io.tmpdir}/fnapi${random[0-10000)}.sock
     */
    private static class EpollDomainSocket extends ServerFactory {
        private static File chooseRandomTmpFile(int port) {
            return new File(
                    System.getProperty("java.io.tmpdir"), String.format("fnapi%d.sock", port));
        }

        @Override
        public Server allocateAddressAndCreate(
                List<BindableService> services,
                Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor)
                throws IOException {
            File tmp;
            do {
                tmp = chooseRandomTmpFile(ThreadLocalRandom.current().nextInt(10000));
            } while (tmp.exists());
            apiServiceDescriptor.setUrl("unix://" + tmp.getAbsolutePath());
            return create(services, apiServiceDescriptor.build());
        }

        @Override
        public Server create(
                List<BindableService> services, Endpoints.ApiServiceDescriptor serviceDescriptor)
                throws IOException {
            SocketAddress socketAddress =
                    SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
            checkArgument(
                    socketAddress instanceof DomainSocketAddress,
                    "%s requires a Unix domain socket address, got %s",
                    EpollDomainSocket.class.getSimpleName(),
                    serviceDescriptor.getUrl());
            return createServer(services, (DomainSocketAddress) socketAddress);
        }

        private static Server createServer(
                List<BindableService> services, DomainSocketAddress domainSocket)
                throws IOException {
            NettyServerBuilder builder =
                    NettyServerBuilder.forAddress(domainSocket)
                            .channelType(EpollServerDomainSocketChannel.class)
                            .workerEventLoopGroup(new EpollEventLoopGroup())
                            .bossEventLoopGroup(new EpollEventLoopGroup())
                            .maxMessageSize(Integer.MAX_VALUE)
                            .permitKeepAliveTime(KEEP_ALIVE_TIME_SEC, TimeUnit.SECONDS);
            for (BindableService service : services) {
                // Wrap the service to extract headers
                builder.addService(
                        ServerInterceptors.intercept(
                                service, GrpcContextHeaderAccessorProvider.interceptor()));
            }
            return builder.build().start();
        }
    }

    /**
     * Creates a {@link Server gRPC Server} using an Epoll socket. Note that this requires <a
     * href="http://netty.io/wiki/forked-tomcat-native.html">Netty TcNative</a> available to be able
     * to provide a {@link EpollServerSocketChannel}.
     *
     * <p>The server is created listening any open port on "localhost".
     */
    private static class EpollSocket extends ServerFactory {
        @Override
        public Server allocateAddressAndCreate(
                List<BindableService> services,
                Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor)
                throws IOException {
            InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
            Server server = createServer(services, address);
            apiServiceDescriptor.setUrl(
                    HostAndPort.fromParts(address.getHostName(), server.getPort()).toString());
            return server;
        }

        @Override
        public Server create(
                List<BindableService> services, Endpoints.ApiServiceDescriptor serviceDescriptor)
                throws IOException {
            SocketAddress socketAddress =
                    SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
            checkArgument(
                    socketAddress instanceof InetSocketAddress,
                    "%s requires a host:port socket address, got %s",
                    EpollSocket.class.getSimpleName(),
                    serviceDescriptor.getUrl());
            return createServer(services, (InetSocketAddress) socketAddress);
        }

        private static Server createServer(List<BindableService> services, InetSocketAddress socket)
                throws IOException {
            ServerBuilder builder =
                    NettyServerBuilder.forAddress(socket)
                            .channelType(EpollServerSocketChannel.class)
                            .workerEventLoopGroup(new EpollEventLoopGroup())
                            .bossEventLoopGroup(new EpollEventLoopGroup())
                            .maxMessageSize(Integer.MAX_VALUE)
                            .permitKeepAliveTime(KEEP_ALIVE_TIME_SEC, TimeUnit.SECONDS);
            for (BindableService service : services) {
                // Wrap the service to extract headers
                builder.addService(
                        ServerInterceptors.intercept(
                                service, GrpcContextHeaderAccessorProvider.interceptor()));
            }
            return builder.build().start();
        }
    }

    /**
     * Factory that constructs client-accessible URLs from a local server address and port.
     * Necessary when clients access server from a different networking context.
     */
    @FunctionalInterface
    public interface UrlFactory {
        String createUrl(String address, int port);

        static UrlFactory createDefault() {
            return (host, port) -> HostAndPort.fromParts(host, port).toString();
        }
    }
}
