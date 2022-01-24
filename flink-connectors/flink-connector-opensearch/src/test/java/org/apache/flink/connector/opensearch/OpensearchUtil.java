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

package org.apache.flink.connector.opensearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.opensearch.RestClientFactory;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/** Collection of utility methods for Opensearch tests. */
@Internal
public class OpensearchUtil {

    private OpensearchUtil() {}

    /**
     * Creates a preconfigured {@link OpensearchContainer} with limited memory allocation and aligns
     * the internal Opensearch log levels with the ones used by the capturing logger.
     *
     * @param dockerImageVersion describing the Opensearch image
     * @param log to derive the log level from
     * @return configured Opensearch container
     */
    public static OpensearchContainer createOpensearchContainer(
            String dockerImageVersion, Logger log) {
        String logLevel;
        if (log.isTraceEnabled()) {
            logLevel = "TRACE";
        } else if (log.isDebugEnabled()) {
            logLevel = "DEBUG";
        } else if (log.isInfoEnabled()) {
            logLevel = "INFO";
        } else if (log.isWarnEnabled()) {
            logLevel = "WARN";
        } else if (log.isErrorEnabled()) {
            logLevel = "ERROR";
        } else {
            logLevel = "OFF";
        }

        return new OpensearchContainer(DockerImageName.parse(dockerImageVersion))
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms2g -Xmx2g")
                .withEnv("logger.org.opensearch", logLevel)
                .withLogConsumer(new Slf4jLogConsumer(log));
    }

    /**
     * Creates a preconfigured {@link RestHighLevelClient} instance for specific {@link
     * OpensearchContainer} instance.
     *
     * @return preconfigured {@link RestHighLevelClient} instance
     */
    public static RestHighLevelClient createClient(OpensearchContainer container) {
        final String username = container.getUsername();
        final String password = container.getPassword();

        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(container.getHttpHostAddress()))
                        .setHttpClientConfigCallback(
                                createClientConfigCallback(username, password)));
    }

    /**
     * Creates a preconfigured {@link RestClientFactory} instance for specific {@link
     * OpensearchContainer} instance.
     *
     * @return preconfigured {@link RestClientFactory} instance
     */
    public static RestClientFactory createClientFactory(OpensearchContainer container) {
        final String username = container.getUsername();
        final String password = container.getPassword();

        return factory ->
                factory.setHttpClientConfigCallback(createClientConfigCallback(username, password));
    }

    /**
     * Creates a dedicated {@link HttpClientConfigCallback} instance for specific {@link
     * OpensearchContainer} instance.
     *
     * @return dedicated {@link HttpClientConfigCallback} instance
     */
    private static HttpClientConfigCallback createClientConfigCallback(
            final String username, final String password) {

        return (httpClientBuilder) -> {
            try {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY, new UsernamePasswordCredentials(username, password));

                return httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setSSLContext(
                                SSLContexts.custom()
                                        .loadTrustMaterial(new TrustAllStrategy())
                                        .build());
            } catch (final NoSuchAlgorithmException
                    | KeyStoreException
                    | KeyManagementException ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    /** A mock {@link DynamicTableSink.Context} for Opensearch tests. */
    public static class MockContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public TypeInformation<?> createTypeInformation(LogicalType consumedLogicalType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }
    }
}
