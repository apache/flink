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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.function.FunctionUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.AddressFromURIString;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This class contains utility functions for pekko. It contains methods to start an actor system
 * with a given akka configuration. Furthermore, the akka configuration used for starting the
 * different actor systems resides in this class.
 */
class AkkaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AkkaUtils.class);

    private static final String FLINK_ACTOR_SYSTEM_NAME = "flink";

    public static String getFlinkActorSystemName() {
        return FLINK_ACTOR_SYSTEM_NAME;
    }

    /**
     * Gets the basic Pekko config which is shared by remote and local actor systems.
     *
     * @param configuration instance which contains the user specified values for the configuration
     * @return Flink's basic Pekko config
     */
    private static Config getBasicAkkaConfig(Configuration configuration) {
        final int akkaThroughput = configuration.getInteger(AkkaOptions.DISPATCHER_THROUGHPUT);
        final String jvmExitOnFatalError =
                booleanToOnOrOff(configuration.getBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR));
        final String logLifecycleEvents =
                booleanToOnOrOff(configuration.getBoolean(AkkaOptions.LOG_LIFECYCLE_EVENTS));
        final String supervisorStrategy = EscalatingSupervisorStrategy.class.getCanonicalName();

        return new AkkaConfigBuilder()
                .add("pekko {")
                .add("  daemonic = off")
                .add("  loggers = [\"org.apache.pekko.event.slf4j.Slf4jLogger\"]")
                .add("  logging-filter = \"org.apache.pekko.event.slf4j.Slf4jLoggingFilter\"")
                .add("  log-config-on-start = off")
                .add("  logger-startup-timeout = 50s")
                .add("  loglevel = " + getLogLevel())
                .add("  stdout-loglevel = OFF")
                .add("  log-dead-letters = " + logLifecycleEvents)
                .add("  log-dead-letters-during-shutdown = " + logLifecycleEvents)
                .add("  jvm-exit-on-fatal-error = " + jvmExitOnFatalError)
                .add("  serialize-messages = off")
                .add("  actor {")
                .add("    guardian-supervisor-strategy = " + supervisorStrategy)
                .add("    warn-about-java-serializer-usage = off")
                .add("    allow-java-serialization = on")
                .add("    default-dispatcher {")
                .add("      throughput = " + akkaThroughput)
                .add("    }")
                .add("    supervisor-dispatcher {")
                .add("      type = Dispatcher")
                .add("      executor = \"thread-pool-executor\"")
                .add("      thread-pool-executor {")
                .add("        core-pool-size-min = 1")
                .add("        core-pool-size-max = 1")
                .add("      }")
                .add("    }")
                .add("  }")
                .add("}")
                .build();
    }

    private static String getLogLevel() {
        if (LOG.isTraceEnabled()) {
            // TRACE is not supported by Pekko
            return "DEBUG";
        }
        if (LOG.isDebugEnabled()) {
            return "DEBUG";
        }
        if (LOG.isInfoEnabled()) {
            return "INFO";
        }
        if (LOG.isWarnEnabled()) {
            return "WARNING";
        }
        if (LOG.isErrorEnabled()) {
            return "ERROR";
        }
        return "OFF";
    }

    public static Config getThreadPoolExecutorConfig(
            RpcSystem.FixedThreadPoolExecutorConfiguration configuration) {
        final int threadPriority = configuration.getThreadPriority();
        final int minNumThreads = configuration.getMinNumThreads();
        final int maxNumThreads = configuration.getMaxNumThreads();

        return new AkkaConfigBuilder()
                .add("pekko {")
                .add("  actor {")
                .add("    default-dispatcher {")
                .add("      type = org.apache.flink.runtime.rpc.akka.PriorityThreadsDispatcher")
                .add("      executor = thread-pool-executor")
                .add("      thread-priority = " + threadPriority)
                .add("      thread-pool-executor {")
                .add("          core-pool-size-min = " + minNumThreads)
                .add("          core-pool-size-max = " + maxNumThreads)
                .add("      }")
                .add("    }")
                .add("  }")
                .add("}")
                .build();
    }

    public static Config getForkJoinExecutorConfig(
            RpcSystem.ForkJoinExecutorConfiguration configuration) {
        final double parallelismFactor = configuration.getParallelismFactor();
        final int minNumThreads = configuration.getMinParallelism();
        final int maxNumThreads = configuration.getMaxParallelism();

        return new AkkaConfigBuilder()
                .add("pekko {")
                .add("  actor {")
                .add("    default-dispatcher {")
                .add("      executor = fork-join-executor")
                .add("      fork-join-executor {")
                .add("          parallelism-factor = " + parallelismFactor)
                .add("          parallelism-min = " + minNumThreads)
                .add("          parallelism-max = " + maxNumThreads)
                .add("      }")
                .add("    }")
                .add("  }")
                .add("}")
                .build();
    }

    /**
     * Creates a Pekko config for a remote actor system listening on port on the network interface
     * identified by bindAddress.
     *
     * @param configuration instance containing the user provided configuration values
     * @param bindAddress of the network interface to bind on
     * @param port to bind to or if 0 then Pekko picks a free port automatically
     * @param externalHostname The host name to expect for Pekko messages
     * @param externalPort The port to expect for Pekko messages
     * @return Flink's Pekko configuration for remote actor systems
     */
    private static Config getRemoteAkkaConfig(
            Configuration configuration,
            String bindAddress,
            int port,
            String externalHostname,
            int externalPort) {
        final AkkaConfigBuilder builder = new AkkaConfigBuilder();

        addBaseRemoteAkkaConfig(builder, configuration, port, externalPort);
        addHostnameRemoteAkkaConfig(builder, bindAddress, externalHostname);
        addSslRemoteAkkaConfig(builder, configuration);

        return builder.build();
    }

    private static void addBaseRemoteAkkaConfig(
            AkkaConfigBuilder akkaConfigBuilder,
            Configuration configuration,
            int port,
            int externalPort) {
        final Duration akkaAskTimeout = configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION);

        final String startupTimeout =
                TimeUtils.getStringInMillis(
                        TimeUtils.parseDuration(
                                configuration.getString(
                                        AkkaOptions.STARTUP_TIMEOUT,
                                        TimeUtils.getStringInMillis(
                                                akkaAskTimeout.multipliedBy(10L)))));

        final String akkaTCPTimeout =
                TimeUtils.getStringInMillis(
                        TimeUtils.parseDuration(configuration.getString(AkkaOptions.TCP_TIMEOUT)));

        final String akkaFramesize = configuration.getString(AkkaOptions.FRAMESIZE);

        final int clientSocketWorkerPoolPoolSizeMin =
                configuration.get(AkkaOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_MIN);
        final int clientSocketWorkerPoolPoolSizeMax =
                configuration.get(AkkaOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_MAX);
        final double clientSocketWorkerPoolPoolSizeFactor =
                configuration.get(AkkaOptions.CLIENT_SOCKET_WORKER_POOL_SIZE_FACTOR);
        final int serverSocketWorkerPoolPoolSizeMin =
                configuration.get(AkkaOptions.SERVER_SOCKET_WORKER_POOL_SIZE_MIN);
        final int serverSocketWorkerPoolPoolSizeMax =
                configuration.get(AkkaOptions.SERVER_SOCKET_WORKER_POOL_SIZE_MAX);
        final double serverSocketWorkerPoolPoolSizeFactor =
                configuration.get(AkkaOptions.SERVER_SOCKET_WORKER_POOL_SIZE_FACTOR);

        final String logLifecycleEvents =
                booleanToOnOrOff(configuration.getBoolean(AkkaOptions.LOG_LIFECYCLE_EVENTS));

        final long retryGateClosedFor = configuration.getLong(AkkaOptions.RETRY_GATE_CLOSED_FOR);

        akkaConfigBuilder
                .add("pekko {")
                .add("  actor {")
                .add("    provider = \"org.apache.pekko.remote.RemoteActorRefProvider\"")
                .add("  }")
                .add("  remote.artery.enabled = false")
                .add("  remote.startup-timeout = " + startupTimeout)
                .add("  remote.warn-about-direct-use = off")
                .add("  remote.use-unsafe-remote-features-outside-cluster = on")
                .add("  remote.classic {")
                .add("    # disable the transport failure detector by setting very high values")
                .add("    transport-failure-detector{")
                .add("      acceptable-heartbeat-pause = 6000 s")
                .add("      heartbeat-interval = 1000 s")
                .add("      threshold = 300")
                .add("    }")
                .add("    enabled-transports = [\"pekko.remote.classic.netty.tcp\"]")
                .add("    netty {")
                .add("      tcp {")
                .add(
                        "        transport-class = \"org.apache.pekko.remote.transport.netty.NettyTransport\"")
                .add("        port = " + externalPort)
                .add("        bind-port = " + port)
                .add("        connection-timeout = " + akkaTCPTimeout)
                .add("        maximum-frame-size = " + akkaFramesize)
                .add("        tcp-nodelay = on")
                .add("        client-socket-worker-pool {")
                .add("          pool-size-min = " + clientSocketWorkerPoolPoolSizeMin)
                .add("          pool-size-max = " + clientSocketWorkerPoolPoolSizeMax)
                .add("          pool-size-factor = " + clientSocketWorkerPoolPoolSizeFactor)
                .add("        }")
                .add("        server-socket-worker-pool {")
                .add("          pool-size-min = " + serverSocketWorkerPoolPoolSizeMin)
                .add("          pool-size-max = " + serverSocketWorkerPoolPoolSizeMax)
                .add("          pool-size-factor = " + serverSocketWorkerPoolPoolSizeFactor)
                .add("        }")
                .add("      }")
                .add("    }")
                .add("    log-remote-lifecycle-events = " + logLifecycleEvents)
                .add("    retry-gate-closed-for = " + retryGateClosedFor + " ms")
                .add("  }")
                .add("}");
    }

    private static void addHostnameRemoteAkkaConfig(
            AkkaConfigBuilder akkaConfigBuilder, String bindAddress, String externalHostname) {
        final String normalizedExternalHostname =
                NetUtils.unresolvedHostToNormalizedString(externalHostname);
        final String effectiveHostname =
                normalizedExternalHostname != null && !normalizedExternalHostname.isEmpty()
                        ? normalizedExternalHostname
                        // if bindAddress is null or empty, then leave bindAddress unspecified.
                        // Pekko will pick InetAddress.getLocalHost.getHostAddress
                        : "";

        akkaConfigBuilder
                .add("pekko {")
                .add("  remote.classic {")
                .add("    netty {")
                .add("      tcp {")
                .add("        hostname = \"" + effectiveHostname + "\"")
                .add("        bind-hostname = \"" + bindAddress + "\"")
                .add("      }")
                .add("    }")
                .add("  }")
                .add("}");
    }

    private static void addSslRemoteAkkaConfig(
            AkkaConfigBuilder akkaConfigBuilder, Configuration configuration) {

        final boolean akkaEnableSSLConfig =
                configuration.getBoolean(AkkaOptions.SSL_ENABLED)
                        && SecurityOptions.isInternalSSLEnabled(configuration);

        final String akkaEnableSSL = booleanToOnOrOff(akkaEnableSSLConfig);

        final String akkaSSLKeyStore =
                configuration.getString(
                        SecurityOptions.SSL_INTERNAL_KEYSTORE,
                        configuration.getString(SecurityOptions.SSL_KEYSTORE));

        final String akkaSSLKeyStorePassword =
                configuration.getString(
                        SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD,
                        configuration.getString(SecurityOptions.SSL_KEYSTORE_PASSWORD));

        final String akkaSSLKeyPassword =
                configuration.getString(
                        SecurityOptions.SSL_INTERNAL_KEY_PASSWORD,
                        configuration.getString(SecurityOptions.SSL_KEY_PASSWORD));

        final String akkaSSLTrustStore =
                configuration.getString(
                        SecurityOptions.SSL_INTERNAL_TRUSTSTORE,
                        configuration.getString(SecurityOptions.SSL_TRUSTSTORE));

        final String akkaSSLTrustStorePassword =
                configuration.getString(
                        SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD,
                        configuration.getString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD));

        final String akkaSSLCertFingerprintString =
                configuration.getString(SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT);

        final String akkaSSLCertFingerprints =
                akkaSSLCertFingerprintString != null
                        ? Arrays.stream(akkaSSLCertFingerprintString.split(","))
                                .collect(Collectors.joining("\",\"", "[\"", "\"]"))
                        : "[]";

        final String akkaSSLProtocol = configuration.getString(SecurityOptions.SSL_PROTOCOL);

        final String akkaSSLAlgorithmsString =
                configuration.getString(SecurityOptions.SSL_ALGORITHMS);
        final String akkaSSLAlgorithms =
                Arrays.stream(akkaSSLAlgorithmsString.split(","))
                        .collect(Collectors.joining(",", "[", "]"));

        final String sslEngineProviderName = CustomSSLEngineProvider.class.getCanonicalName();

        akkaConfigBuilder
                .add("pekko {")
                .add("  remote.classic {")
                .add("    enabled-transports = [\"pekko.remote.classic.netty.ssl\"]")
                .add("    netty {")
                .add("      ssl = ${pekko.remote.classic.netty.tcp}")
                .add("      ssl {")
                .add("        enable-ssl = " + akkaEnableSSL)
                .add("        ssl-engine-provider = " + sslEngineProviderName)
                .add("        security {")
                .add("          key-store = \"" + akkaSSLKeyStore + "\"")
                .add("          key-store-password = \"" + akkaSSLKeyStorePassword + "\"")
                .add("          key-password = \"" + akkaSSLKeyPassword + "\"")
                .add("          trust-store = \"" + akkaSSLTrustStore + "\"")
                .add("          trust-store-password = \"" + akkaSSLTrustStorePassword + "\"")
                .add("          protocol = " + akkaSSLProtocol + "")
                .add("          enabled-algorithms = " + akkaSSLAlgorithms + "")
                .add("          random-number-generator = \"\"")
                .add("          require-mutual-authentication = on")
                .add("          cert-fingerprints = " + akkaSSLCertFingerprints + "")
                .add("        }")
                .add("      }")
                .add("    }")
                .add("  }")
                .add("}");
    }

    /**
     * Creates a local actor system without remoting.
     *
     * @param configuration instance containing the user provided configuration values
     * @return The created actor system
     */
    public static ActorSystem createLocalActorSystem(Configuration configuration) {
        final Config akkaConfig = getAkkaConfig(configuration, null);
        return createActorSystem(akkaConfig);
    }

    /**
     * Creates an actor system with the given pekko config.
     *
     * @param akkaConfig configuration for the actor system
     * @return created actor system
     */
    private static ActorSystem createActorSystem(Config akkaConfig) {
        return createActorSystem(getFlinkActorSystemName(), akkaConfig);
    }

    /**
     * Creates an actor system with the given pekko config.
     *
     * @param actorSystemName name of the actor system
     * @param akkaConfig configuration for the actor system
     * @return created actor system
     */
    public static ActorSystem createActorSystem(String actorSystemName, Config akkaConfig) {
        // Initialize slf4j as logger of Akka's Netty instead of java.util.logging (FLINK-1650)
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        return RobustActorSystem.create(actorSystemName, akkaConfig);
    }

    /**
     * Creates an actor system with the default config and listening on a random port of the
     * localhost.
     *
     * @return default actor system listening on a random port of the localhost
     */
    @VisibleForTesting
    public static ActorSystem createDefaultActorSystem() {
        return createActorSystem(getDefaultAkkaConfig());
    }

    /**
     * Creates the default pekko configuration which listens on a random port on the local machine.
     * All configuration values are set to default values.
     *
     * @return Flink's Pekko default config
     */
    private static Config getDefaultAkkaConfig() {
        return getAkkaConfig(new Configuration(), new HostAndPort("", 0));
    }

    /**
     * Creates a pekko config with the provided configuration values. If the listening address is
     * specified, then the actor system will listen on the respective address.
     *
     * @param configuration instance containing the user provided configuration values
     * @param externalAddress optional tuple of bindAddress and port to be reachable at. If null is
     *     given, then an Pekko config for local actor system will be returned
     * @return Pekko config
     */
    public static Config getAkkaConfig(
            Configuration configuration, @Nullable HostAndPort externalAddress) {
        return getAkkaConfig(
                configuration,
                externalAddress,
                null,
                AkkaUtils.getForkJoinExecutorConfig(
                        AkkaBootstrapTools.getForkJoinExecutorConfiguration(configuration)));
    }

    /**
     * Creates an pekko config with the provided configuration values. If the listening address is
     * specified, then the actor system will listen on the respective address.
     *
     * @param configuration instance containing the user provided configuration values
     * @param externalAddress optional tuple of external address and port to be reachable at. If
     *     null is given, then an Pekko config for local actor system will be returned
     * @param bindAddress optional tuple of bind address and port to be used locally. If null is
     *     given, wildcard IP address and the external port wil be used. Takes effect only if
     *     externalAddress is not null.
     * @param executorConfig config defining the used executor by the default dispatcher
     * @return Pekko config
     */
    public static Config getAkkaConfig(
            Configuration configuration,
            @Nullable HostAndPort externalAddress,
            @Nullable HostAndPort bindAddress,
            Config executorConfig) {

        final Config defaultConfig =
                AkkaUtils.getBasicAkkaConfig(configuration).withFallback(executorConfig);

        if (externalAddress != null) {
            if (bindAddress != null) {
                final Config remoteConfig =
                        AkkaUtils.getRemoteAkkaConfig(
                                configuration,
                                bindAddress.getHost(),
                                bindAddress.getPort(),
                                externalAddress.getHost(),
                                externalAddress.getPort());

                return remoteConfig.withFallback(defaultConfig);
            } else {
                final Config remoteConfig =
                        AkkaUtils.getRemoteAkkaConfig(
                                configuration,
                                NetUtils.getWildcardIPAddress(),
                                externalAddress.getPort(),
                                externalAddress.getHost(),
                                externalAddress.getPort());

                return remoteConfig.withFallback(defaultConfig);
            }
        }

        return defaultConfig;
    }

    /**
     * Returns the address of the given {@link ActorSystem}. The {@link Address} object contains the
     * port and the host under which the actor system is reachable.
     *
     * @param system {@link ActorSystem} for which the {@link Address} shall be retrieved
     * @return {@link Address} of the given {@link ActorSystem}
     */
    public static Address getAddress(ActorSystem system) {
        return RemoteAddressExtension.INSTANCE.apply(system).getAddress();
    }

    /**
     * Returns the given {@link ActorRef}'s path string representation with host and port of the
     * {@link ActorSystem} in which the actor is running.
     *
     * @param system {@link ActorSystem} in which the given {@link ActorRef} is running
     * @param actor {@link ActorRef} of the actor for which the URL has to be generated
     * @return String containing the {@link ActorSystem} independent URL of the actor
     */
    public static String getAkkaURL(ActorSystem system, ActorRef actor) {
        final Address address = getAddress(system);
        return actor.path().toStringWithAddress(address);
    }

    /**
     * Extracts the {@link Address} from the given pekko URL.
     *
     * @param akkaURL to extract the {@link Address} from
     * @throws MalformedURLException if the {@link Address} could not be parsed from the given pekko
     *     URL
     * @return Extracted {@link Address} from the given akka URL
     */
    @SuppressWarnings("RedundantThrows") // hidden checked exception coming from Pekko
    public static Address getAddressFromAkkaURL(String akkaURL) throws MalformedURLException {
        return AddressFromURIString.apply(akkaURL);
    }

    /**
     * Extracts the hostname and the port of the remote actor system from the given Pekko URL. The
     * result is an {@link InetSocketAddress} instance containing the extracted hostname and port.
     * If the Pekko URL does not contain the hostname and port information, e.g. a local Pekko URL
     * is provided, then an {@link Exception} is thrown.
     *
     * @param akkaURL The URL to extract the host and port from.
     * @throws java.lang.Exception Thrown, if the given string does not represent a proper url
     * @return The InetSocketAddress with the extracted host and port.
     */
    public static InetSocketAddress getInetSocketAddressFromAkkaURL(String akkaURL)
            throws Exception {
        // AkkaURLs have the form schema://systemName@host:port/.... if it's a remote Pekko URL
        try {
            final Address address = getAddressFromAkkaURL(akkaURL);

            if (address.host().isDefined() && address.port().isDefined()) {
                return new InetSocketAddress(address.host().get(), (int) address.port().get());
            } else {
                throw new MalformedURLException();
            }
        } catch (MalformedURLException e) {
            throw new Exception("Could not retrieve InetSocketAddress from Pekko URL " + akkaURL);
        }
    }

    /**
     * Terminates the given {@link ActorSystem} and returns its termination future.
     *
     * @param actorSystem to terminate
     * @return Termination future
     */
    public static CompletableFuture<Void> terminateActorSystem(ActorSystem actorSystem) {
        return AkkaFutureUtils.toJava(actorSystem.terminate()).thenAccept(FunctionUtils.ignoreFn());
    }

    private static String booleanToOnOrOff(boolean flag) {
        return flag ? "on" : "off";
    }

    private static class AkkaConfigBuilder {
        private final StringWriter stringWriter = new StringWriter();
        private final PrintWriter printWriter = new PrintWriter(stringWriter);

        public AkkaConfigBuilder add(String configLine) {
            printWriter.println(configLine);
            return this;
        }

        public Config build() {
            return ConfigFactory.parseString(stringWriter.toString()).resolve();
        }
    }
}
