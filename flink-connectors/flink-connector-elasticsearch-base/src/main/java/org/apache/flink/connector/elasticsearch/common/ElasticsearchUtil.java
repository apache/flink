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

package org.apache.flink.connector.elasticsearch.common;

import org.apache.flink.annotation.Internal;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClientBuilder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Collection of utility methods for the Elasticsearch source and sink. */
@Internal
public class ElasticsearchUtil {

    public static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig config) {
        checkNotNull(builder);
        checkNotNull(config);

        if (config.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(config.getConnectionPathPrefix());
        }
        if (config.getPassword() != null && config.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            builder.setHttpClientConfigCallback(
                    httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        if (config.getConnectionRequestTimeout() != null
                || config.getConnectionTimeout() != null
                || config.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (config.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    config.getConnectionRequestTimeout());
                        }
                        if (config.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(config.getConnectionTimeout());
                        }
                        if (config.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(config.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }
}
