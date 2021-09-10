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

package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import org.apache.pulsar.client.api.ProxyProtocol;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.ADMIN_CONFIG_PREFIX;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.CLIENT_CONFIG_PREFIX;

/**
 * Configuration for Pulsar Client, these config options would be used for both source, sink and
 * table.
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(name = "PulsarClient", keyPrefix = CLIENT_CONFIG_PREFIX),
            @ConfigGroup(name = "PulsarAdmin", keyPrefix = ADMIN_CONFIG_PREFIX)
        })
@SuppressWarnings("java:S1192")
public final class PulsarOptions {

    // Pulsar client API config prefix.
    public static final String CLIENT_CONFIG_PREFIX = "pulsar.client.";
    // Pulsar admin API config prefix.
    public static final String ADMIN_CONFIG_PREFIX = "pulsar.admin.";

    private PulsarOptions() {
        // This is a constant class
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "serviceUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Service URL provider for Pulsar service.")
                                    .linebreak()
                                    .text(
                                            "To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.")
                                    .linebreak()
                                    .text(
                                            "You can assign Pulsar protocol URLs to specific clusters and use the %s scheme.",
                                            code("pulsar"))
                                    .linebreak()
                                    .list(
                                            text(
                                                    "This is an example of %s: %s.",
                                                    code("localhost"),
                                                    code("pulsar://localhost:6650")),
                                            text(
                                                    "If you have multiple brokers, the URL is as: %s",
                                                    code(
                                                            "pulsar://localhost:6550,localhost:6651,localhost:6652")),
                                            text(
                                                    "A URL for a production Pulsar cluster is as: %s",
                                                    code(
                                                            "pulsar://pulsar.us-west.example.com:6650")),
                                            text(
                                                    "If you use TLS authentication, the URL is as %s",
                                                    code(
                                                            "pulsar+ssl://pulsar.us-west.example.com:6651")))
                                    .build());

    public static final ConfigOption<String> PULSAR_AUTH_PLUGIN_CLASS_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authPluginClassName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the authentication plugin.");

    public static final ConfigOption<String> PULSAR_AUTH_PARAMS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParams")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Parameters for the authentication plugin.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example:")
                                    .linebreak()
                                    .add(code("key1:val1,key2:val2"))
                                    .build());

    public static final ConfigOption<Map<String, String>> PULSAR_AUTH_PARAM_MAP =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParamMap")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Parameters for the authentication plugin.");

    public static final ConfigOption<Integer> PULSAR_OPERATION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "operationTimeoutMs")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            Description.builder()
                                    .text("Operation timeout (in ms).")
                                    .text(
                                            " Operations such as creating producers, subscribing or unsubscribing topics are retried during this interval.")
                                    .text(
                                            " If the operation is not completed during this interval, the operation will be marked as failed.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_STATS_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "statsIntervalSeconds")
                    .longType()
                    .defaultValue(60L)
                    .withDescription(
                            Description.builder()
                                    .text("Interval between each stats info.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "Stats is activated with positive %s",
                                                    code("statsInterval")),
                                            text(
                                                    "Set %s to 1 second at least.",
                                                    code("statsIntervalSeconds")))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_NUM_IO_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numIoThreads")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of threads used for handling connections to brokers.");

    public static final ConfigOption<Integer> PULSAR_NUM_LISTENER_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numListenerThreads")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of threads used for handling message listeners.")
                                    .text(
                                            " The listener thread pool is shared across all the consumers and readers that are using a %s model to get messages.",
                                            code("listener"))
                                    .text(
                                            " For a given consumer, the listener is always invoked from the same thread to ensure ordering.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_CONNECTIONS_PER_BROKER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionsPerBroker")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum number of connections that the client library will open to a single broker.")
                                    .linebreak()
                                    .text(
                                            " By default, the connection pool will use a single connection for all the producers and consumers.")
                                    .text(
                                            " Increasing this parameter may improve throughput when using many producers over a high latency connection.")
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_USE_TCP_NO_DELAY =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useTcpNoDelay")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to use the TCP no-delay flag on the connection to disable Nagle algorithm.")
                                    .linebreak()
                                    .text(
                                            "No-delay features ensures that packets are sent out on the network as soon as possible,")
                                    .text(" and it is critical to achieve low latency publishes.")
                                    .text(
                                            " On the other hand, sending out a huge number of small packets might limit the overall throughput.")
                                    .text(
                                            " Therefore, if latency is not a concern, it is recommended to set the %s flag to %s.",
                                            code("useTcpNoDelay"), code("false"))
                                    .linebreak()
                                    .text("By default, it is set to %s.", code("true"))
                                    .build());

    public static final ConfigOption<String> PULSAR_TLS_TRUST_CERTS_FILE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustCertsFilePath")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Path to the trusted TLS certificate file.");

    public static final ConfigOption<Boolean> PULSAR_TLS_ALLOW_INSECURE_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsAllowInsecureConnection")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the Pulsar client accepts untrusted TLS certificate from the broker.");

    public static final ConfigOption<Boolean> PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsHostnameVerificationEnable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text("Whether to enable TLS hostname verification.")
                                    .text(
                                            " It allows to validate hostname verification when a client connects to the broker over TLS.")
                                    .text(
                                            " It validates incoming x509 certificate and matches provided hostname (CN/SAN) with the expected broker's host name.")
                                    .text(
                                            " It follows RFC 2818, 3.1. Server Identity hostname verification.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_CONCURRENT_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "concurrentLookupRequest")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on the broker.")
                                    .text(
                                            " It should be configured with a higher value only in case of it requires to produce or subscribe on thousands of topic using a created %s",
                                            code("PulsarClient"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRequest")
                    .intType()
                    .defaultValue(50000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum number of lookup requests allowed on each broker connection to prevent overload on the broker.")
                                    .text(
                                            " It should be greater than %s.",
                                            code("maxConcurrentLookupRequests"))
                                    .text(
                                            " Requests that inside %s are already sent to broker,",
                                            code("maxConcurrentLookupRequests"))
                                    .text(
                                            " and requests beyond %s and under %s will wait in each client cnx.",
                                            code("maxConcurrentLookupRequests"),
                                            code("maxLookupRequests"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REDIRECTS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRedirects")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "The maximum number of times a lookup-request redirections to a broker.");

    public static final ConfigOption<Integer> PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxNumberOfRejectedRequestPerConnection")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum number of rejected requests of a broker in a certain period (30s) after the current connection is closed")
                                    .text(
                                            " and the client creates a new connection to connect to a different broker.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_KEEP_ALIVE_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "keepAliveIntervalSeconds")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "Interval (in seconds) for keeping connection between the Pulsar client and broker alive.");

    public static final ConfigOption<Integer> PULSAR_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionTimeoutMs")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Duration (in ms) of waiting for a connection to a broker to be established.")
                                    .linebreak()
                                    .text(
                                            "If the duration passes without a response from a broker, the connection attempt is dropped.")
                                    .build());

    // TODO This option would be exposed by Pulsar's ClientBuilder in the next Pulsar release.
    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "requestTimeoutMs")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Maximum duration (in ms) for completing a request.");

    public static final ConfigOption<Long> PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "initialBackoffIntervalNanos")
                    .longType()
                    .defaultValue(TimeUnit.MILLISECONDS.toNanos(100))
                    .withDescription("Default duration (in nanoseconds) for a backoff interval.");

    public static final ConfigOption<Long> PULSAR_MAX_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxBackoffIntervalNanos")
                    .longType()
                    .defaultValue(SECONDS.toNanos(60))
                    .withDescription(
                            "The maximum duration (in nanoseconds) for a backoff interval.");

    public static final ConfigOption<Boolean> PULSAR_ENABLE_BUSY_WAIT =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableBusyWait")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text("Option to enable busy-wait settings.")
                                    .linebreak()
                                    .text(
                                            "This option will enable spin-waiting on executors and IO threads in order to reduce latency during context switches.")
                                    .text(
                                            " The spinning will consume 100% CPU even when the broker is not doing any work.")
                                    .text(
                                            " It is recommended to reduce the number of IO threads and BookKeeper client threads to only have fewer CPU cores busy.")
                                    .build());

    public static final ConfigOption<String> PULSAR_LISTENER_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "listenerName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Configure the %s that the broker will return the corresponding %s.",
                                            code("listenerName"), code("advertisedListener"))
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_USE_KEY_STORE_TLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useKeyStoreTls")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If TLS is enabled, whether use the KeyStore type as the TLS configuration parameter.")
                                    .text(
                                            " If it is set to %s, it means to use the default pem type configuration.",
                                            code("false"))
                                    .build());

    public static final ConfigOption<String> PULSAR_SSL_PROVIDER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "sslProvider")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The name of the security provider used for SSL connections.")
                                    .text(
                                            " The default value is the default security provider of the JVM.")
                                    .build());

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_TYPE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStoreType")
                    .stringType()
                    .defaultValue("JKS")
                    .withDescription("The file format of the trust store file.");

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePath")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The location of the trust store file.");

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PASSWORD =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePassword")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The store password for the key store file.");

    // The real config type is Set<String>, you should provided a json str here.
    public static final ConfigOption<List<String>> PULSAR_TLS_CIPHERS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsCiphers")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            Description.builder()
                                    .text("A list of cipher suites.")
                                    .text(
                                            " This is a named combination of authentication, encryption,")
                                    .text(
                                            " MAC and the key exchange algorithm used to negotiate the security settings for a network connection using the TLS or SSL network protocol.")
                                    .text(
                                            " By default all the available cipher suites are supported.")
                                    .build());

    public static final ConfigOption<List<String>> PULSAR_TLS_PROTOCOLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsProtocols")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            Description.builder()
                                    .text("The SSL protocol used to generate the SSLContext.")
                                    .text(
                                            " By default, it is set TLS, which is fine for most cases.")
                                    .text(
                                            " Allowed values in recent JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_MEMORY_LIMIT_BYTES =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "memoryLimitBytes")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The limit (in bytes) on the amount of direct memory that will be allocated by this client instance.")
                                    .linebreak()
                                    .text(
                                            "Note: at this moment this is only limiting the memory for producers.")
                                    .text(" Setting this to %s will disable the limit.", code("0"))
                                    .build());

    public static final ConfigOption<String> PULSAR_PROXY_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyServiceUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Proxy-service URL when a client connects to the broker via the proxy.")
                                    .text(" The client can choose the type of proxy-routing.")
                                    .build());

    public static final ConfigOption<ProxyProtocol> PULSAR_PROXY_PROTOCOL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyProtocol")
                    .enumType(ProxyProtocol.class)
                    .defaultValue(ProxyProtocol.SNI)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Protocol type to determine the type of proxy routing when a client connects to the proxy using %s.",
                                            code("pulsar.client.proxyServiceUrl"))
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_ENABLE_TRANSACTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableTransaction")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If transaction is enabled, start the %s with %s.",
                                            code("transactionCoordinatorClient"),
                                            code("PulsarClient"))
                                    .build());

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for PulsarAdmin part.
    // All the configuration listed below should have the pulsar.admin prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_ADMIN_URL =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "adminUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The Pulsar service HTTP URL for the admin endpoint. For example, %s, or %s for TLS.",
                                            code("http://my-broker.example.com:8080"),
                                            code("https://my-broker.example.com:8443"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_CONNECT_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "connectTimeout")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("The connection time out (in ms) for the PulsarAdmin client.");

    public static final ConfigOption<Integer> PULSAR_READ_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "readTimeout")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "The server response read timeout (in ms) for the PulsarAdmin client for any request.");

    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "requestTimeout")
                    .intType()
                    .defaultValue(300000)
                    .withDescription(
                            "The server request timeout (in ms) for the PulsarAdmin client for any request.");

    public static final ConfigOption<Integer> PULSAR_AUTO_CERT_REFRESH_TIME =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "autoCertRefreshTime")
                    .intType()
                    .defaultValue(300000)
                    .withDescription(
                            "The auto cert refresh time (in ms) if Pulsar admin supports TLS authentication.");
}
