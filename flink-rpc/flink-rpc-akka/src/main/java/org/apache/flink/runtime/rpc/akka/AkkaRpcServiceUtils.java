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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.function.TriFunction;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import static org.apache.flink.util.NetUtils.isValidClientPort;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * These RPC utilities contain helper methods around RPC use, such as starting an RPC service, or
 * constructing RPC addresses.
 */
public class AkkaRpcServiceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcServiceUtils.class);

    private static final String AKKA_TCP = "akka.tcp";
    private static final String AKKA_SSL_TCP = "akka.ssl.tcp";

    static final String SUPERVISOR_NAME = "rpc";

    private static final String SIMPLE_AKKA_CONFIG_TEMPLATE =
            "akka {remote.classic {netty.tcp {maximum-frame-size = %s}}}";

    private static final String MAXIMUM_FRAME_SIZE_PATH =
            "akka.remote.classic.netty.tcp.maximum-frame-size";

    // ------------------------------------------------------------------------
    //  RPC instantiation
    // ------------------------------------------------------------------------

    static AkkaRpcService createRemoteRpcService(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        final AkkaRpcServiceBuilder akkaRpcServiceBuilder =
                AkkaRpcServiceUtils.remoteServiceBuilder(
                        configuration, externalAddress, externalPortRange);

        if (bindAddress != null) {
            akkaRpcServiceBuilder.withBindAddress(bindAddress);
        }

        bindPort.ifPresent(akkaRpcServiceBuilder::withBindPort);

        return akkaRpcServiceBuilder.createAndStart();
    }

    static AkkaRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return new AkkaRpcServiceBuilder(configuration, LOG, externalAddress, externalPortRange);
    }

    @VisibleForTesting
    static AkkaRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration, @Nullable String externalAddress, int externalPort) {
        return remoteServiceBuilder(configuration, externalAddress, String.valueOf(externalPort));
    }

    static AkkaRpcServiceBuilder localServiceBuilder(Configuration configuration) {
        return new AkkaRpcServiceBuilder(configuration, LOG);
    }

    // ------------------------------------------------------------------------
    //  RPC endpoint addressing
    // ------------------------------------------------------------------------

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param config The configuration from which to deduce further settings.
     * @return The RPC URL of the specified RPC endpoint.
     */
    public static String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException {

        checkNotNull(config, "config is null");

        final boolean sslEnabled =
                config.getBoolean(AkkaOptions.SSL_ENABLED)
                        && SecurityOptions.isInternalSSLEnabled(config);

        return getRpcUrl(
                hostname,
                port,
                endpointName,
                addressResolution,
                sslEnabled ? AkkaProtocol.SSL_TCP : AkkaProtocol.TCP);
    }

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param akkaProtocol True, if security/encryption is enabled, false otherwise.
     * @return The RPC URL of the specified RPC endpoint.
     */
    public static String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            AkkaProtocol akkaProtocol)
            throws UnknownHostException {

        checkNotNull(hostname, "hostname is null");
        checkNotNull(endpointName, "endpointName is null");
        checkArgument(isValidClientPort(port), "port must be in [1, 65535]");

        if (addressResolution == AddressResolution.TRY_ADDRESS_RESOLUTION) {
            // Fail fast if the hostname cannot be resolved
            //noinspection ResultOfMethodCallIgnored
            InetAddress.getByName(hostname);
        }

        final String hostPort = NetUtils.unresolvedHostAndPortToNormalizedString(hostname, port);

        return internalRpcUrl(
                endpointName, Optional.of(new RemoteAddressInformation(hostPort, akkaProtocol)));
    }

    public static String getLocalRpcUrl(String endpointName) {
        return internalRpcUrl(endpointName, Optional.empty());
    }

    public static boolean isRecipientTerminatedException(Throwable exception) {
        return exception.getMessage().contains("had already been terminated.");
    }

    private static final class RemoteAddressInformation {
        private final String hostnameAndPort;
        private final AkkaProtocol akkaProtocol;

        private RemoteAddressInformation(String hostnameAndPort, AkkaProtocol akkaProtocol) {
            this.hostnameAndPort = hostnameAndPort;
            this.akkaProtocol = akkaProtocol;
        }

        private String getHostnameAndPort() {
            return hostnameAndPort;
        }

        private AkkaProtocol getAkkaProtocol() {
            return akkaProtocol;
        }
    }

    private static String internalRpcUrl(
            String endpointName, Optional<RemoteAddressInformation> remoteAddressInformation) {
        final String protocolPrefix =
                remoteAddressInformation
                        .map(rai -> akkaProtocolToString(rai.getAkkaProtocol()))
                        .orElse("akka");
        final Optional<String> optionalHostnameAndPort =
                remoteAddressInformation.map(RemoteAddressInformation::getHostnameAndPort);

        final StringBuilder url = new StringBuilder(String.format("%s://flink", protocolPrefix));
        optionalHostnameAndPort.ifPresent(hostPort -> url.append("@").append(hostPort));

        url.append("/user/").append(SUPERVISOR_NAME).append("/").append(endpointName);

        // protocolPrefix://flink[@hostname:port]/user/rpc/endpointName
        return url.toString();
    }

    private static String akkaProtocolToString(AkkaProtocol akkaProtocol) {
        return akkaProtocol == AkkaProtocol.SSL_TCP ? AKKA_SSL_TCP : AKKA_TCP;
    }

    /** Whether to use TCP or encrypted TCP for Akka. */
    public enum AkkaProtocol {
        TCP,
        SSL_TCP
    }

    // ------------------------------------------------------------------------
    //  RPC service configuration
    // ------------------------------------------------------------------------

    public static long extractMaximumFramesize(Configuration configuration) {
        String maxFrameSizeStr = configuration.getString(AkkaOptions.FRAMESIZE);
        String akkaConfigStr = String.format(SIMPLE_AKKA_CONFIG_TEMPLATE, maxFrameSizeStr);
        Config akkaConfig = ConfigFactory.parseString(akkaConfigStr);
        return akkaConfig.getBytes(MAXIMUM_FRAME_SIZE_PATH);
    }

    // ------------------------------------------------------------------------
    //  RPC service builder
    // ------------------------------------------------------------------------

    /** Builder for {@link AkkaRpcService}. */
    static class AkkaRpcServiceBuilder implements RpcSystem.RpcServiceBuilder {

        private final Configuration configuration;
        private final Logger logger;
        @Nullable private final String externalAddress;
        @Nullable private final String externalPortRange;

        private String actorSystemName = AkkaUtils.getFlinkActorSystemName();

        @Nullable private Config actorSystemExecutorConfiguration = null;

        @Nullable private Config customConfig = null;
        private String bindAddress = NetUtils.getWildcardIPAddress();
        @Nullable private Integer bindPort = null;

        /** Builder for creating a remote RPC service. */
        private AkkaRpcServiceBuilder(
                final Configuration configuration,
                final Logger logger,
                @Nullable final String externalAddress,
                final String externalPortRange) {
            this.configuration = Preconditions.checkNotNull(configuration);
            this.logger = Preconditions.checkNotNull(logger);
            this.externalAddress =
                    externalAddress == null
                            ? InetAddress.getLoopbackAddress().getHostAddress()
                            : externalAddress;
            this.externalPortRange = Preconditions.checkNotNull(externalPortRange);
        }

        /** Builder for creating a local RPC service. */
        private AkkaRpcServiceBuilder(final Configuration configuration, final Logger logger) {
            this.configuration = Preconditions.checkNotNull(configuration);
            this.logger = Preconditions.checkNotNull(logger);
            this.externalAddress = null;
            this.externalPortRange = null;
        }

        @Override
        public AkkaRpcServiceBuilder withComponentName(final String actorSystemName) {
            this.actorSystemName = Preconditions.checkNotNull(actorSystemName);
            return this;
        }

        public AkkaRpcServiceBuilder withCustomConfig(final Config customConfig) {
            this.customConfig = customConfig;
            return this;
        }

        @Override
        public AkkaRpcServiceBuilder withBindAddress(final String bindAddress) {
            this.bindAddress = Preconditions.checkNotNull(bindAddress);
            return this;
        }

        @Override
        public AkkaRpcServiceBuilder withBindPort(int bindPort) {
            Preconditions.checkArgument(
                    NetUtils.isValidHostPort(bindPort), "Invalid port number: " + bindPort);
            this.bindPort = bindPort;
            return this;
        }

        @Override
        public RpcSystem.RpcServiceBuilder withExecutorConfiguration(
                RpcSystem.FixedThreadPoolExecutorConfiguration executorConfiguration) {
            this.actorSystemExecutorConfiguration =
                    AkkaUtils.getThreadPoolExecutorConfig(executorConfiguration);
            return this;
        }

        @Override
        public RpcSystem.RpcServiceBuilder withExecutorConfiguration(
                RpcSystem.ForkJoinExecutorConfiguration executorConfiguration) {
            this.actorSystemExecutorConfiguration =
                    AkkaUtils.getForkJoinExecutorConfig(executorConfiguration);
            return this;
        }

        public AkkaRpcService createAndStart() throws Exception {
            return createAndStart(AkkaRpcService::new);
        }

        public AkkaRpcService createAndStart(
                TriFunction<ActorSystem, AkkaRpcServiceConfiguration, ClassLoader, AkkaRpcService>
                        constructor)
                throws Exception {
            if (actorSystemExecutorConfiguration == null) {
                actorSystemExecutorConfiguration =
                        AkkaUtils.getForkJoinExecutorConfig(
                                AkkaBootstrapTools.getForkJoinExecutorConfiguration(configuration));
            }

            final ActorSystem actorSystem;

            // akka internally caches the context class loader
            // make sure it uses the plugin class loader
            try (TemporaryClassLoaderContext ignored =
                    TemporaryClassLoaderContext.of(getClass().getClassLoader())) {
                if (externalAddress == null) {
                    // create local actor system
                    actorSystem =
                            AkkaBootstrapTools.startLocalActorSystem(
                                    configuration,
                                    actorSystemName,
                                    logger,
                                    actorSystemExecutorConfiguration,
                                    customConfig);
                } else {
                    // create remote actor system
                    actorSystem =
                            AkkaBootstrapTools.startRemoteActorSystem(
                                    configuration,
                                    actorSystemName,
                                    externalAddress,
                                    externalPortRange,
                                    bindAddress,
                                    Optional.ofNullable(bindPort),
                                    logger,
                                    actorSystemExecutorConfiguration,
                                    customConfig);
                }
            }

            return constructor.apply(
                    actorSystem,
                    AkkaRpcServiceConfiguration.fromConfiguration(configuration),
                    RpcService.class.getClassLoader());
        }
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    private AkkaRpcServiceUtils() {}
}
