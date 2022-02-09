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

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import java.util.Map;
import java.util.TreeSet;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTO_CERT_REFRESH_TIME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONCURRENT_LOOKUP_REQUEST;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECTIONS_PER_BROKER;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECTION_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_CONNECT_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ENABLE_BUSY_WAIT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ENABLE_TRANSACTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_KEEP_ALIVE_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_LISTENER_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_BACKOFF_INTERVAL_NANOS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_LOOKUP_REDIRECTS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_LOOKUP_REQUEST;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_MEMORY_LIMIT_BYTES;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_NUM_IO_THREADS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_NUM_LISTENER_THREADS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_OPERATION_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_PROXY_PROTOCOL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_PROXY_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_READ_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_REQUEST_TIMEOUT;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_REQUEST_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SSL_PROVIDER;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_CIPHERS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_PROTOCOLS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_STORE_PASSWORD;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_STORE_PATH;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_TLS_TRUST_STORE_TYPE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_USE_KEY_STORE_TLS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_USE_TCP_NO_DELAY;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.pulsar.client.api.SizeUnit.BYTES;

/** The factory for creating pulsar client classes from {@link PulsarConfiguration}. */
@Internal
public final class PulsarClientFactory {

    private PulsarClientFactory() {
        // No need to create instance.
    }

    /** Create a PulsarClient by using the flink Configuration and the config customizer. */
    public static PulsarClient createClient(PulsarConfiguration configuration) {
        ClientBuilder builder = PulsarClient.builder();

        // requestTimeoutMs don't have a setter method on ClientBuilder. We have to use low level
        // setter method instead. So we put this at the beginning of the builder.
        Integer requestTimeoutMs = configuration.get(PULSAR_REQUEST_TIMEOUT_MS);
        builder.loadConf(singletonMap("requestTimeoutMs", requestTimeoutMs));

        // Create the authentication instance for the Pulsar client.
        builder.authentication(createAuthentication(configuration));

        configuration.useOption(PULSAR_SERVICE_URL, builder::serviceUrl);
        configuration.useOption(PULSAR_LISTENER_NAME, builder::listenerName);
        configuration.useOption(
                PULSAR_OPERATION_TIMEOUT_MS,
                timeout -> builder.operationTimeout(timeout, MILLISECONDS));
        configuration.useOption(PULSAR_NUM_IO_THREADS, builder::ioThreads);
        configuration.useOption(PULSAR_NUM_LISTENER_THREADS, builder::listenerThreads);
        configuration.useOption(PULSAR_CONNECTIONS_PER_BROKER, builder::connectionsPerBroker);
        configuration.useOption(PULSAR_USE_TCP_NO_DELAY, builder::enableTcpNoDelay);
        configuration.useOption(PULSAR_TLS_TRUST_CERTS_FILE_PATH, builder::tlsTrustCertsFilePath);
        configuration.useOption(
                PULSAR_TLS_ALLOW_INSECURE_CONNECTION, builder::allowTlsInsecureConnection);
        configuration.useOption(
                PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, builder::enableTlsHostnameVerification);
        configuration.useOption(PULSAR_USE_KEY_STORE_TLS, builder::useKeyStoreTls);
        configuration.useOption(PULSAR_SSL_PROVIDER, builder::sslProvider);
        configuration.useOption(PULSAR_TLS_TRUST_STORE_TYPE, builder::tlsTrustStoreType);
        configuration.useOption(PULSAR_TLS_TRUST_STORE_PATH, builder::tlsTrustStorePath);
        configuration.useOption(PULSAR_TLS_TRUST_STORE_PASSWORD, builder::tlsTrustStorePassword);
        configuration.useOption(PULSAR_TLS_CIPHERS, TreeSet::new, builder::tlsCiphers);
        configuration.useOption(PULSAR_TLS_PROTOCOLS, TreeSet::new, builder::tlsProtocols);
        configuration.useOption(
                PULSAR_MEMORY_LIMIT_BYTES, bytes -> builder.memoryLimit(bytes, BYTES));
        configuration.useOption(
                PULSAR_STATS_INTERVAL_SECONDS, v -> builder.statsInterval(v, SECONDS));
        configuration.useOption(
                PULSAR_CONCURRENT_LOOKUP_REQUEST, builder::maxConcurrentLookupRequests);
        configuration.useOption(PULSAR_MAX_LOOKUP_REQUEST, builder::maxLookupRequests);
        configuration.useOption(PULSAR_MAX_LOOKUP_REDIRECTS, builder::maxLookupRedirects);
        configuration.useOption(
                PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION,
                builder::maxNumberOfRejectedRequestPerConnection);
        configuration.useOption(
                PULSAR_KEEP_ALIVE_INTERVAL_SECONDS, v -> builder.keepAliveInterval(v, SECONDS));
        configuration.useOption(
                PULSAR_CONNECTION_TIMEOUT_MS, v -> builder.connectionTimeout(v, MILLISECONDS));
        configuration.useOption(
                PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS,
                v -> builder.startingBackoffInterval(v, NANOSECONDS));
        configuration.useOption(
                PULSAR_MAX_BACKOFF_INTERVAL_NANOS, v -> builder.maxBackoffInterval(v, NANOSECONDS));
        configuration.useOption(PULSAR_ENABLE_BUSY_WAIT, builder::enableBusyWait);
        if (configuration.contains(PULSAR_PROXY_SERVICE_URL)) {
            String proxyServiceUrl = configuration.get(PULSAR_PROXY_SERVICE_URL);
            ProxyProtocol proxyProtocol = configuration.get(PULSAR_PROXY_PROTOCOL);
            builder.proxyServiceUrl(proxyServiceUrl, proxyProtocol);
        }
        configuration.useOption(PULSAR_ENABLE_TRANSACTION, builder::enableTransaction);

        return sneakyClient(builder::build);
    }

    /**
     * PulsarAdmin shares almost the same configuration with PulsarClient, but we separate this
     * create method for directly creating it.
     */
    public static PulsarAdmin createAdmin(PulsarConfiguration configuration) {
        PulsarAdminBuilder builder = PulsarAdmin.builder();

        // Create the authentication instance for the Pulsar client.
        builder.authentication(createAuthentication(configuration));

        configuration.useOption(PULSAR_ADMIN_URL, builder::serviceHttpUrl);
        configuration.useOption(PULSAR_TLS_TRUST_CERTS_FILE_PATH, builder::tlsTrustCertsFilePath);
        configuration.useOption(
                PULSAR_TLS_ALLOW_INSECURE_CONNECTION, builder::allowTlsInsecureConnection);
        configuration.useOption(
                PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, builder::enableTlsHostnameVerification);
        configuration.useOption(PULSAR_USE_KEY_STORE_TLS, builder::useKeyStoreTls);
        configuration.useOption(PULSAR_SSL_PROVIDER, builder::sslProvider);
        configuration.useOption(PULSAR_TLS_TRUST_STORE_TYPE, builder::tlsTrustStoreType);
        configuration.useOption(PULSAR_TLS_TRUST_STORE_PATH, builder::tlsTrustStorePath);
        configuration.useOption(PULSAR_TLS_TRUST_STORE_PASSWORD, builder::tlsTrustStorePassword);
        configuration.useOption(PULSAR_TLS_CIPHERS, TreeSet::new, builder::tlsCiphers);
        configuration.useOption(PULSAR_TLS_PROTOCOLS, TreeSet::new, builder::tlsProtocols);
        configuration.useOption(
                PULSAR_CONNECT_TIMEOUT, v -> builder.connectionTimeout(v, MILLISECONDS));
        configuration.useOption(PULSAR_READ_TIMEOUT, v -> builder.readTimeout(v, MILLISECONDS));
        configuration.useOption(
                PULSAR_REQUEST_TIMEOUT, v -> builder.requestTimeout(v, MILLISECONDS));
        configuration.useOption(
                PULSAR_AUTO_CERT_REFRESH_TIME, v -> builder.autoCertRefreshTime(v, MILLISECONDS));

        return sneakyClient(builder::build);
    }

    /**
     * Create the {@link Authentication} instance for both {@code PulsarClient} and {@code
     * PulsarAdmin}. If the user didn't provide configuration, a {@link AuthenticationDisabled}
     * instance would be returned.
     *
     * <p>This method behavior is the same as the pulsar command line tools.
     */
    private static Authentication createAuthentication(PulsarConfiguration configuration) {
        if (configuration.contains(PULSAR_AUTH_PLUGIN_CLASS_NAME)) {
            String authPluginClassName = configuration.get(PULSAR_AUTH_PLUGIN_CLASS_NAME);

            if (configuration.contains(PULSAR_AUTH_PARAMS)) {
                String authParamsString = configuration.get(PULSAR_AUTH_PARAMS);
                return sneakyClient(
                        () -> AuthenticationFactory.create(authPluginClassName, authParamsString));
            } else if (configuration.contains(PULSAR_AUTH_PARAM_MAP)) {
                Map<String, String> paramsMap = configuration.get(PULSAR_AUTH_PARAM_MAP);
                return sneakyClient(
                        () -> AuthenticationFactory.create(authPluginClassName, paramsMap));
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "No %s or %s provided",
                                PULSAR_AUTH_PARAMS.key(), PULSAR_AUTH_PARAM_MAP.key()));
            }
        }

        return AuthenticationDisabled.INSTANCE;
    }
}
