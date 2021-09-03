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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

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
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
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

/** The util for creating pulsar configuration class from flink's {@link Configuration}. */
@Internal
public final class PulsarConfigUtils {

    private PulsarConfigUtils() {
        // No need to create instance.
    }

    /** Create a PulsarClient by using the flink Configuration and the config customizer. */
    public static PulsarClient createClient(Configuration configuration) {
        ClientBuilder builder = PulsarClient.builder();

        setOptionValue(configuration, PULSAR_SERVICE_URL, builder::serviceUrl);
        setOptionValue(configuration, PULSAR_LISTENER_NAME, builder::listenerName);
        builder.authentication(createAuthentication(configuration));
        setOptionValue(
                configuration,
                PULSAR_OPERATION_TIMEOUT_MS,
                timeout -> builder.operationTimeout(timeout, MILLISECONDS));
        setOptionValue(configuration, PULSAR_NUM_IO_THREADS, builder::ioThreads);
        setOptionValue(configuration, PULSAR_NUM_LISTENER_THREADS, builder::listenerThreads);
        setOptionValue(configuration, PULSAR_CONNECTIONS_PER_BROKER, builder::connectionsPerBroker);
        setOptionValue(configuration, PULSAR_USE_TCP_NO_DELAY, builder::enableTcpNoDelay);
        setOptionValue(
                configuration, PULSAR_TLS_TRUST_CERTS_FILE_PATH, builder::tlsTrustCertsFilePath);
        setOptionValue(
                configuration,
                PULSAR_TLS_ALLOW_INSECURE_CONNECTION,
                builder::allowTlsInsecureConnection);
        setOptionValue(
                configuration,
                PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE,
                builder::enableTlsHostnameVerification);
        setOptionValue(configuration, PULSAR_USE_KEY_STORE_TLS, builder::useKeyStoreTls);
        setOptionValue(configuration, PULSAR_SSL_PROVIDER, builder::sslProvider);
        setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_TYPE, builder::tlsTrustStoreType);
        setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_PATH, builder::tlsTrustStorePath);
        setOptionValue(
                configuration, PULSAR_TLS_TRUST_STORE_PASSWORD, builder::tlsTrustStorePassword);
        setOptionValue(configuration, PULSAR_TLS_CIPHERS, TreeSet::new, builder::tlsCiphers);
        setOptionValue(configuration, PULSAR_TLS_PROTOCOLS, TreeSet::new, builder::tlsProtocols);
        setOptionValue(
                configuration,
                PULSAR_MEMORY_LIMIT_BYTES,
                bytes -> builder.memoryLimit(bytes, BYTES));
        setOptionValue(
                configuration,
                PULSAR_STATS_INTERVAL_SECONDS,
                v -> builder.statsInterval(v, SECONDS));
        setOptionValue(
                configuration,
                PULSAR_CONCURRENT_LOOKUP_REQUEST,
                builder::maxConcurrentLookupRequests);
        setOptionValue(configuration, PULSAR_MAX_LOOKUP_REQUEST, builder::maxLookupRequests);
        setOptionValue(configuration, PULSAR_MAX_LOOKUP_REDIRECTS, builder::maxLookupRedirects);
        setOptionValue(
                configuration,
                PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION,
                builder::maxNumberOfRejectedRequestPerConnection);
        setOptionValue(
                configuration,
                PULSAR_KEEP_ALIVE_INTERVAL_SECONDS,
                v -> builder.keepAliveInterval(v, SECONDS));
        setOptionValue(
                configuration,
                PULSAR_CONNECTION_TIMEOUT_MS,
                v -> builder.connectionTimeout(v, MILLISECONDS));
        setOptionValue(
                configuration,
                PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS,
                v -> builder.startingBackoffInterval(v, NANOSECONDS));
        setOptionValue(
                configuration,
                PULSAR_MAX_BACKOFF_INTERVAL_NANOS,
                v -> builder.maxBackoffInterval(v, NANOSECONDS));
        setOptionValue(configuration, PULSAR_ENABLE_BUSY_WAIT, builder::enableBusyWait);
        if (configuration.contains(PULSAR_PROXY_SERVICE_URL)) {
            String proxyServiceUrl = configuration.get(PULSAR_PROXY_SERVICE_URL);
            ProxyProtocol proxyProtocol = configuration.get(PULSAR_PROXY_PROTOCOL);
            builder.proxyServiceUrl(proxyServiceUrl, proxyProtocol);
        }
        setOptionValue(configuration, PULSAR_ENABLE_TRANSACTION, builder::enableTransaction);

        return sneakyClient(builder::build);
    }

    /**
     * PulsarAdmin shares almost the same configuration with PulsarClient, but we separate this
     * create method for directly create it.
     */
    public static PulsarAdmin createAdmin(Configuration configuration) {
        PulsarAdminBuilder builder = PulsarAdmin.builder();

        setOptionValue(configuration, PULSAR_ADMIN_URL, builder::serviceHttpUrl);
        builder.authentication(createAuthentication(configuration));
        setOptionValue(
                configuration, PULSAR_TLS_TRUST_CERTS_FILE_PATH, builder::tlsTrustCertsFilePath);
        setOptionValue(
                configuration,
                PULSAR_TLS_ALLOW_INSECURE_CONNECTION,
                builder::allowTlsInsecureConnection);
        setOptionValue(
                configuration,
                PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE,
                builder::enableTlsHostnameVerification);
        setOptionValue(configuration, PULSAR_USE_KEY_STORE_TLS, builder::useKeyStoreTls);
        setOptionValue(configuration, PULSAR_SSL_PROVIDER, builder::sslProvider);
        setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_TYPE, builder::tlsTrustStoreType);
        setOptionValue(configuration, PULSAR_TLS_TRUST_STORE_PATH, builder::tlsTrustStorePath);
        setOptionValue(
                configuration, PULSAR_TLS_TRUST_STORE_PASSWORD, builder::tlsTrustStorePassword);
        setOptionValue(configuration, PULSAR_TLS_CIPHERS, TreeSet::new, builder::tlsCiphers);
        setOptionValue(configuration, PULSAR_TLS_PROTOCOLS, TreeSet::new, builder::tlsProtocols);
        setOptionValue(
                configuration,
                PULSAR_CONNECT_TIMEOUT,
                v -> builder.connectionTimeout(v, MILLISECONDS));
        setOptionValue(
                configuration, PULSAR_READ_TIMEOUT, v -> builder.readTimeout(v, MILLISECONDS));
        setOptionValue(
                configuration,
                PULSAR_REQUEST_TIMEOUT,
                v -> builder.requestTimeout(v, MILLISECONDS));
        setOptionValue(
                configuration,
                PULSAR_AUTO_CERT_REFRESH_TIME,
                v -> builder.autoCertRefreshTime(v, MILLISECONDS));

        return sneakyClient(builder::build);
    }

    /**
     * Create the {@link Authentication} instance for both {@code PulsarClient} and {@code
     * PulsarAdmin}. If the user didn't provide configuration, a {@link AuthenticationDisabled}
     * instance would be returned.
     *
     * <p>This method behavior is the same as the pulsar command line tools.
     */
    private static Authentication createAuthentication(Configuration configuration) {
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

    /** Get the option value str from given config, convert it into the real value instance. */
    public static <F, T> T getOptionValue(
            Configuration configuration, ConfigOption<F> option, Function<F, T> convertor) {
        F value = configuration.get(option);
        if (value != null) {
            return convertor.apply(value);
        } else {
            return null;
        }
    }

    /** Set the config option's value to a given builder. */
    public static <T> void setOptionValue(
            Configuration configuration, ConfigOption<T> option, Consumer<T> setter) {
        setOptionValue(configuration, option, identity(), setter);
    }

    /**
     * Query the config option's value, convert it into a required type, set it to a given builder.
     */
    public static <T, V> void setOptionValue(
            Configuration configuration,
            ConfigOption<T> option,
            Function<T, V> convertor,
            Consumer<V> setter) {
        if (configuration.contains(option)) {
            V value = getOptionValue(configuration, option, convertor);
            setter.accept(value);
        }
    }
}
