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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.elasticsearch.table.ElasticsearchSourceOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchSourceOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchSourceOptions.NUMBER_OF_SLICES_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchSourceOptions.PIT_KEEP_ALIVE_OPTION;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class ElasticsearchSourceConfig {
    protected final ReadableConfig config;

    ElasticsearchSourceConfig(ReadableConfig config) {
        this.config = checkNotNull(config);
    }

    public String getIndex() {
        return config.get(INDEX_OPTION);
    }

    public List<HttpHost> getHosts() {
        return config.get(HOSTS_OPTION).stream()
                .map(ElasticsearchSourceConfig::validateAndParseHostsString)
                .collect(Collectors.toList());
    }

    public Integer getNumberOfSlices() {
        return config.get(NUMBER_OF_SLICES_OPTION);
    }

    public Duration getPitKeepAlive() {
        return config.get(PIT_KEEP_ALIVE_OPTION);
    }

    private static HttpHost validateAndParseHostsString(String host) {
        try {
            HttpHost httpHost = HttpHost.create(host);
            if (httpHost.getPort() < 0) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
                                host, ElasticsearchConnectorOptions.HOSTS_OPTION.key()));
            }

            if (httpHost.getSchemeName() == null) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
                                host, ElasticsearchConnectorOptions.HOSTS_OPTION.key()));
            }
            return httpHost;
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
                            host, ElasticsearchConnectorOptions.HOSTS_OPTION.key()),
                    e);
        }
    }
}
