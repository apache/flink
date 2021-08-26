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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import org.apache.pulsar.client.api.ProxyProtocol;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration for Pulsar Client, these config options would be used for both source, sink and
 * table.
 */
@PublicEvolving
public final class PulsarOptions {

    // Pulsar client API config prefix.
    private static final String CLIENT_CONFIG_PREFIX = "pulsar.client.";
    // Pulsar admin API config prefix.
    private static final String ADMIN_CONFIG_PREFIX = "pulsar.admin.";

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
                                    .text(
                                            "String represents parameters for the authentication plugin.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example:")
                                    .linebreak()
                                    .add(code("key1:val1,key2:val2"))
                                    .build());

    public static final ConfigOption<Map<String, String>> PULSAR_AUTH_PARAM_MAP =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParamMap")
                    .mapType()
                    .defaultValue(emptyMap());

    public static final ConfigOption<Integer> PULSAR_OPERATION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "operationTimeoutMs")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Operation timeout.");

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
                                                    "Set %s to 1 second at least",
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
                    .withDescription("The number of threads used for handling message listeners.");

    public static final ConfigOption<Integer> PULSAR_CONNECTIONS_PER_BROKER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionsPerBroker")
                    .intType()
                    .defaultValue(1);

    public static final ConfigOption<Boolean> PULSAR_USE_TCP_NO_DELAY =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useTcpNoDelay")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to use TCP no-delay flag on the connection to disable Nagle algorithm.");

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
                            "Whether the Pulsar client accepts untrusted TLS certificate from broker.");

    public static final ConfigOption<Boolean> PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsHostnameVerificationEnable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable TLS hostname verification.");

    public static final ConfigOption<Integer> PULSAR_CONCURRENT_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "concurrentLookupRequest")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on broker.");

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRequest")
                    .intType()
                    .defaultValue(50000)
                    .withDescription(
                            "The maximum number of lookup requests allowed on each broker connection to prevent overload on broker.");

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REDIRECTS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRedirects")
                    .intType()
                    .defaultValue(20);

    public static final ConfigOption<Integer> PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxNumberOfRejectedRequestPerConnection")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The maximum number of rejected requests of a broker in a certain time"
                                    + " frame (30 seconds) after the current connection is closed and"
                                    + " the client creates a new connection to connect to a different broker.");

    public static final ConfigOption<Integer> PULSAR_KEEP_ALIVE_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "keepAliveIntervalSeconds")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "Seconds of keeping alive interval for each client broker connection.");

    public static final ConfigOption<Integer> PULSAR_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionTimeoutMs")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Duration of waiting for a connection to a broker to be established.")
                                    .linebreak()
                                    .text(
                                            "If the duration passes without a response from a broker, the connection attempt is dropped.")
                                    .build());

    // TODO This option would be exposed by Pulsar's ClientBuilder in next release.
    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "requestTimeoutMs")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Maximum duration for completing a request.");

    public static final ConfigOption<Long> PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "initialBackoffIntervalNanos")
                    .longType()
                    .defaultValue(TimeUnit.MILLISECONDS.toNanos(100))
                    .withDescription("Default duration for a backoff interval.");

    public static final ConfigOption<Long> PULSAR_MAX_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxBackoffIntervalNanos")
                    .longType()
                    .defaultValue(TimeUnit.SECONDS.toNanos(60))
                    .withDescription("Maximum duration for a backoff interval.");

    public static final ConfigOption<Boolean> PULSAR_ENABLE_BUSY_WAIT =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableBusyWait")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<String> PULSAR_LISTENER_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "listenerName").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> PULSAR_USE_KEY_STORE_TLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useKeyStoreTls")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<String> PULSAR_SSL_PROVIDER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "sslProvider").stringType().noDefaultValue();

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_TYPE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStoreType")
                    .stringType()
                    .defaultValue("JKS");

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePath")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PASSWORD =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePassword")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<List<String>> PULSAR_TLS_CIPHERS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsCiphers")
                    .stringType()
                    .asList()
                    .defaultValues();

    public static final ConfigOption<List<String>> PULSAR_TLS_PROTOCOLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsProtocols")
                    .stringType()
                    .asList()
                    .defaultValues();

    public static final ConfigOption<Long> PULSAR_MEMORY_LIMIT_BYTES =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "memoryLimitBytes")
                    .longType()
                    .defaultValue(0L);

    public static final ConfigOption<String> PULSAR_PROXY_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyServiceUrl")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<ProxyProtocol> PULSAR_PROXY_PROTOCOL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyProtocol")
                    .enumType(ProxyProtocol.class)
                    .noDefaultValue();

    public static final ConfigOption<Boolean> PULSAR_ENABLE_TRANSACTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableTransaction")
                    .booleanType()
                    .defaultValue(false);

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
                    .withDescription("Admin URL for Pulsar service.");

    // The network connect timeout in millis.
    public static final ConfigOption<Integer> PULSAR_CONNECT_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "connectTimeout").intType().defaultValue(60000);

    // The read timeout in millis.
    public static final ConfigOption<Integer> PULSAR_READ_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "readTimeout").intType().defaultValue(60000);

    // The request timeout in millis.
    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "requestTimeout")
                    .intType()
                    .defaultValue(300000);

    // The auto refresh time for certification in millis.
    public static final ConfigOption<Integer> PULSAR_AUTO_CERT_REFRESH_TIME =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "autoCertRefreshTime")
                    .intType()
                    .defaultValue(300000);
}
