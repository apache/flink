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

package org.apache.flink.connector.pulsar.source.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.ConfigurationDataCustomizer;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_CERT_REFRESH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONNECT_TIMEOUT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_READ_TIMEOUT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REQUEST_TIMEOUT;
import static org.apache.flink.connector.pulsar.source.config.PulsarConfigurationUtils.createClientConfig;
import static org.apache.flink.connector.pulsar.source.config.PulsarConfigurationUtils.createConsumerConfig;
import static org.apache.flink.connector.pulsar.source.utils.PulsarJsonUtils.configMap;

public final class PulsarClientUtils {

    private PulsarClientUtils() {
        // No need to create instance.
    }

    /** Create a PulsarClient by using the flink Configuration and the config customizer. */
    public static PulsarClient createClient(
            Configuration configuration,
            ConfigurationDataCustomizer<ClientConfigurationData> customizer)
            throws PulsarClientException {
        ClientConfigurationData clientConfig = createClientConfig(configuration);
        if (customizer != null) {
            customizer.customize(clientConfig);
        }

        return createClient(clientConfig);
    }

    /**
     * Create a PulsarClient by using the given config, all the config could be directly loaded
     * except the authentication.
     */
    public static PulsarClient createClient(ClientConfigurationData config)
            throws PulsarClientException {
        // PulsarClientBuilder don't support using the given ClientConfigurationData directly.
        ClientBuilder clientBuilder = PulsarClient.builder().loadConf(configMap(config));

        // Authentication instance should be added manually.
        if (config.getAuthentication() != null) {
            clientBuilder.authentication(config.getAuthentication());
        }
        // Override the clock config directly.
        clientBuilder.clock(config.getClock());

        return clientBuilder.build();
    }

    /** Create a Pulsar Consumer by using the flink Configuration and the config customizer. */
    public static <T> Consumer<T> createConsumer(
            PulsarClient client,
            Schema<T> schema,
            Configuration configuration,
            ConfigurationDataCustomizer<ConsumerConfigurationData<T>> customizer)
            throws PulsarClientException {
        ConsumerConfigurationData<T> consumerConfig = createConsumerConfig(configuration);
        if (customizer != null) {
            customizer.customize(consumerConfig);
        }

        return createConsumer(client, schema, consumerConfig);
    }

    /** Create a pulsar consumer by using the given ConsumerConfigurationData and PulsarClient. */
    public static <T> Consumer<T> createConsumer(
            PulsarClient client, Schema<T> schema, ConsumerConfigurationData<T> config)
            throws PulsarClientException {
        // ConsumerBuilder don't support using the given ConsumerConfigurationData directly.
        ConsumerBuilder<T> consumerBuilder = client.newConsumer(schema).loadConf(configMap(config));

        // Set some non-serializable fields.
        if (config.getMessageListener() != null) {
            consumerBuilder.messageListener(config.getMessageListener());
        }
        if (config.getConsumerEventListener() != null) {
            consumerBuilder.consumerEventListener(config.getConsumerEventListener());
        }
        if (config.getCryptoKeyReader() != null) {
            consumerBuilder.cryptoKeyReader(config.getCryptoKeyReader());
        }
        if (config.getMessageCrypto() != null) {
            consumerBuilder.messageCrypto(config.getMessageCrypto());
        }
        if (config.getBatchReceivePolicy() != null) {
            consumerBuilder.batchReceivePolicy(config.getBatchReceivePolicy());
        }

        return consumerBuilder.subscribe();
    }

    /**
     * PulsarAdmin shares almost the same configuration with PulsarClient, but we separate this
     * create method for directly create it.
     */
    public static PulsarAdmin createAdmin(Configuration configuration)
            throws PulsarClientException {
        ClientConfigurationData clientConfig = createClientConfig(configuration);
        return PulsarAdmin.builder()
                .serviceHttpUrl(clientConfig.getServiceUrl())
                .authentication(clientConfig.getAuthentication())
                .tlsTrustCertsFilePath(clientConfig.getTlsTrustCertsFilePath())
                .allowTlsInsecureConnection(clientConfig.isTlsAllowInsecureConnection())
                .enableTlsHostnameVerification(clientConfig.isTlsHostnameVerificationEnable())
                .useKeyStoreTls(clientConfig.isUseKeyStoreTls())
                .sslProvider(clientConfig.getSslProvider())
                .tlsTrustStoreType(clientConfig.getTlsTrustStoreType())
                .tlsTrustStorePath(clientConfig.getTlsTrustStorePath())
                .tlsTrustStorePassword(clientConfig.getTlsTrustStorePassword())
                .tlsCiphers(clientConfig.getTlsCiphers())
                .tlsProtocols(clientConfig.getTlsProtocols())
                .connectionTimeout(
                        Math.toIntExact(configuration.get(PULSAR_CONNECT_TIMEOUT)), MILLISECONDS)
                .readTimeout(Math.toIntExact(configuration.get(PULSAR_READ_TIMEOUT)), MILLISECONDS)
                .requestTimeout(
                        Math.toIntExact(configuration.get(PULSAR_REQUEST_TIMEOUT)), MILLISECONDS)
                .autoCertRefreshTime(
                        Math.toIntExact(configuration.get(PULSAR_AUTO_CERT_REFRESH_TIME)),
                        MILLISECONDS)
                .build();
    }
}
