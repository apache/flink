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
import org.apache.flink.connector.pulsar.common.utils.PulsarJsonUtils;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_USE_TLS;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarJsonUtils.configMap;

/** The util for creating pulsar configuration class from flink's {@link Configuration}. */
@Internal
public final class PulsarConfigUtils {

    private PulsarConfigUtils() {
        // No need to create instance.
    }

    /**
     * Create a common {@link ClientConfigurationData} for both pulsar source and sink connector by
     * using the given {@link Configuration}. Add the configuration was listed in {@link
     * PulsarSourceOptions}, and we would parse it one by one.
     *
     * @param configuration The flink configuration
     */
    public static ClientConfigurationData createClientConfig(
            Configuration configuration,
            @Nullable ConfigurationDataCustomizer<ClientConfigurationData> customizer) {
        ClientConfigurationData data = new ClientConfigurationData();

        // Set the properties one by one.
        data.setServiceUrl(configuration.get(PULSAR_SERVICE_URL));
        data.setAuthPluginClassName(configuration.get(PULSAR_AUTH_PLUGIN_CLASS_NAME));
        data.setAuthParams(configuration.get(PULSAR_AUTH_PARAMS));
        data.setAuthParamMap(
                getOptionValue(
                        configuration,
                        PULSAR_AUTH_PARAM_MAP,
                        v -> PulsarJsonUtils.toMap(String.class, String.class, v)));
        data.setOperationTimeoutMs(configuration.get(PULSAR_OPERATION_TIMEOUT_MS));
        data.setStatsIntervalSeconds(configuration.get(PULSAR_STATS_INTERVAL_SECONDS));
        data.setNumIoThreads(configuration.get(PULSAR_NUM_IO_THREADS));
        data.setNumListenerThreads(configuration.get(PULSAR_NUM_LISTENER_THREADS));
        data.setConnectionsPerBroker(configuration.get(PULSAR_CONNECTIONS_PER_BROKER));
        data.setUseTcpNoDelay(configuration.get(PULSAR_USE_TCP_NO_DELAY));
        data.setUseTls(configuration.get(PULSAR_USE_TLS));
        data.setTlsTrustCertsFilePath(configuration.get(PULSAR_TLS_TRUST_CERTS_FILE_PATH));
        data.setTlsAllowInsecureConnection(configuration.get(PULSAR_TLS_ALLOW_INSECURE_CONNECTION));
        data.setTlsHostnameVerificationEnable(
                configuration.get(PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE));
        data.setConcurrentLookupRequest(configuration.get(PULSAR_CONCURRENT_LOOKUP_REQUEST));
        data.setMaxLookupRequest(configuration.get(PULSAR_MAX_LOOKUP_REQUEST));
        data.setMaxLookupRedirects(configuration.get(PULSAR_MAX_LOOKUP_REDIRECTS));
        data.setMaxNumberOfRejectedRequestPerConnection(
                configuration.get(PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION));
        data.setKeepAliveIntervalSeconds(configuration.get(PULSAR_KEEP_ALIVE_INTERVAL_SECONDS));
        data.setConnectionTimeoutMs(configuration.get(PULSAR_CONNECTION_TIMEOUT_MS));
        data.setRequestTimeoutMs(configuration.get(PULSAR_REQUEST_TIMEOUT_MS));
        data.setInitialBackoffIntervalNanos(
                configuration.get(PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS));
        data.setMaxBackoffIntervalNanos(configuration.get(PULSAR_MAX_BACKOFF_INTERVAL_NANOS));
        data.setEnableBusyWait(configuration.get(PULSAR_ENABLE_BUSY_WAIT));
        data.setListenerName(configuration.get(PULSAR_LISTENER_NAME));
        data.setUseKeyStoreTls(configuration.get(PULSAR_USE_KEY_STORE_TLS));
        data.setSslProvider(configuration.get(PULSAR_SSL_PROVIDER));
        data.setTlsTrustStoreType(configuration.get(PULSAR_TLS_TRUST_STORE_TYPE));
        data.setTlsTrustStorePath(configuration.get(PULSAR_TLS_TRUST_STORE_PATH));
        data.setTlsTrustStorePassword(configuration.get(PULSAR_TLS_TRUST_STORE_PASSWORD));
        data.setTlsCiphers(
                getOptionValue(
                        configuration,
                        PULSAR_TLS_CIPHERS,
                        v -> PulsarJsonUtils.toSet(String.class, v)));
        data.setTlsProtocols(
                getOptionValue(
                        configuration,
                        PULSAR_TLS_PROTOCOLS,
                        v -> PulsarJsonUtils.toSet(String.class, v)));
        data.setMemoryLimitBytes(configuration.get(PULSAR_MEMORY_LIMIT_BYTES));
        data.setProxyServiceUrl(configuration.get(PULSAR_PROXY_SERVICE_URL));
        data.setProxyProtocol(configuration.get(PULSAR_PROXY_PROTOCOL));
        data.setEnableTransaction(configuration.get(PULSAR_ENABLE_TRANSACTION));
        data.setAuthentication(createAuthentication(configuration));

        if (customizer != null) {
            customizer.customize(data);
        }

        return data;
    }

    /**
     * Create the {@link Authentication} instance for both {@code PulsarClient} and {@code
     * PulsarAdmin}. If the user didn't provide configuration, a {@link AuthenticationDisabled}
     * instance would be returned.
     *
     * <p>This method behavior is the same as the pulsar command line tools.
     */
    public static Authentication createAuthentication(Configuration configuration) {
        if (configuration.contains(PULSAR_AUTH_PLUGIN_CLASS_NAME)) {
            String authPluginClassName = configuration.get(PULSAR_AUTH_PLUGIN_CLASS_NAME);

            if (configuration.contains(PULSAR_AUTH_PARAMS)) {
                String authParamsString = configuration.get(PULSAR_AUTH_PARAMS);
                return sneakyClient(
                        () -> AuthenticationFactory.create(authPluginClassName, authParamsString));
            } else if (configuration.contains(PULSAR_AUTH_PARAM_MAP)) {
                Map<String, String> paramsMap =
                        getOptionValue(
                                configuration,
                                PULSAR_AUTH_PARAM_MAP,
                                v -> PulsarJsonUtils.toMap(String.class, String.class, v));
                return sneakyClient(
                        () -> AuthenticationFactory.create(authPluginClassName, paramsMap));
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

    /** Create a PulsarClient by using the flink Configuration and the config customizer. */
    public static PulsarClient createClient(
            Configuration configuration,
            ConfigurationDataCustomizer<ClientConfigurationData> customizer) {
        return createClient(createClientConfig(configuration, customizer));
    }

    /**
     * Create a PulsarClient by using the given config, all the config could be directly loaded
     * except the authentication.
     */
    public static PulsarClient createClient(ClientConfigurationData config) {
        // PulsarClientBuilder don't support using the given ClientConfigurationData directly.
        Map<String, Object> configMap = configMap(config);

        // These two auth config would be serialized to a secret information by default.
        configMap.put("authParamMap", config.getAuthParamMap());
        configMap.put("authParams", config.getAuthParams());

        ClientBuilder clientBuilder = PulsarClient.builder().loadConf(configMap);

        // Set some non-serializable fields.
        if (config.getAuthentication() != null) {
            clientBuilder.authentication(config.getAuthentication());
        }
        if (config.getClock() != null) {
            clientBuilder.clock(config.getClock());
        }
        if (config.getServiceUrlProvider() != null) {
            clientBuilder.serviceUrlProvider(config.getServiceUrlProvider());
        }

        return sneakyClient(clientBuilder::build);
    }

    /**
     * PulsarAdmin shares almost the same configuration with PulsarClient, but we separate this
     * create method for directly create it.
     */
    public static PulsarAdmin createAdmin(
            Configuration configuration,
            ConfigurationDataCustomizer<ClientConfigurationData> customizer) {
        ClientConfigurationData clientConfig = createClientConfig(configuration, customizer);
        String adminUrl = configuration.get(PULSAR_ADMIN_URL);
        int connectionTimeout = Math.toIntExact(configuration.get(PULSAR_CONNECT_TIMEOUT));
        int readTimeout = Math.toIntExact(configuration.get(PULSAR_READ_TIMEOUT));
        int requestTimeout = Math.toIntExact(configuration.get(PULSAR_REQUEST_TIMEOUT));
        int autoCertRefreshTime = Math.toIntExact(configuration.get(PULSAR_AUTO_CERT_REFRESH_TIME));

        PulsarAdminBuilder builder =
                PulsarAdmin.builder()
                        .serviceHttpUrl(adminUrl)
                        .authentication(clientConfig.getAuthentication())
                        .tlsTrustCertsFilePath(clientConfig.getTlsTrustCertsFilePath())
                        .allowTlsInsecureConnection(clientConfig.isTlsAllowInsecureConnection())
                        .enableTlsHostnameVerification(
                                clientConfig.isTlsHostnameVerificationEnable())
                        .useKeyStoreTls(clientConfig.isUseKeyStoreTls())
                        .sslProvider(clientConfig.getSslProvider())
                        .tlsTrustStoreType(clientConfig.getTlsTrustStoreType())
                        .tlsTrustStorePath(clientConfig.getTlsTrustStorePath())
                        .tlsTrustStorePassword(clientConfig.getTlsTrustStorePassword())
                        .tlsCiphers(clientConfig.getTlsCiphers())
                        .tlsProtocols(clientConfig.getTlsProtocols())
                        .connectionTimeout(connectionTimeout, MILLISECONDS)
                        .readTimeout(readTimeout, MILLISECONDS)
                        .requestTimeout(requestTimeout, MILLISECONDS)
                        .autoCertRefreshTime(autoCertRefreshTime, MILLISECONDS);

        return sneakyClient(builder::build);
    }
}
