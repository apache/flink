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

package org.apache.flink.connector.elasticsearch.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSearchHitDeserializationSchema;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The @builder class for {@link ElasticsearchSource}.
 *
 * <p>The following example shows the minimum setup to create a ElasticsearchSource that reads the
 * String values from an Elasticsearch index.
 *
 * <pre>{@code
 * TODO: add example
 * }</pre>
 *
 * <p>The ElasticsearchSource runs in a {@link Boundedness#BOUNDED} mode and stops when the entire
 * index has been read.
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * ElasticsearchSource.
 */
public class ElasticsearchSourceBuilder<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceBuilder.class);

    private Duration pitKeepAlive = Duration.ofMinutes(5);
    private int numberOfSearchSlices = 1;
    private String indexName;
    private ElasticsearchSearchHitDeserializationSchema<OUT> deserializationSchema;

    private List<HttpHost> hosts;
    private String username;
    private String password;
    private String connectionPathPrefix;

    ElasticsearchSourceBuilder() {}

    private boolean isGreaterOrEqual(Duration d1, Duration d2) {
        return ((d1.compareTo(d2) >= 0));
    }

    /**
     * Sets the keep alive duration for the Point-in-Time (PIT). The PIT is required to a snapshot
     * of the data in the index, to avoid reading the same data on subsequent search calls. If the
     * PIT has expired the source will not be able to read data from this snapshot again. This also
     * means that recovering from a checkpoint whose PIT has expired is not possible.
     *
     * @param pitKeepAlive duration of the PIT keep alive
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setPitKeepAlive(Duration pitKeepAlive) {
        checkNotNull(pitKeepAlive);
        checkArgument(
                isGreaterOrEqual(pitKeepAlive, Duration.ofMinutes(5)),
                "PIT keep alive should be at least 5 minutes.");
        this.pitKeepAlive = pitKeepAlive;
        return this;
    }

    /**
     * Sets the number of search slices to be used to read the index. The number of search slices
     * should be a multiple of the number of shards in your Elasticsearch cluster.
     *
     * @param numberOfSearchSlices the number of search slices
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setNumberOfSearchSlices(int numberOfSearchSlices) {
        checkArgument(numberOfSearchSlices > 0, "Number of search slices must be greater than 0.");
        this.numberOfSearchSlices = numberOfSearchSlices;
        return this;
    }

    /**
     * Sets the name of the Elasticsearch index to be read.
     *
     * @param indexName name of the Elasticsearch index
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setIndexName(String indexName) {
        checkNotNull(indexName);
        this.indexName = indexName;
        return this;
    }

    /**
     * Sets the {@link ElasticsearchSearchHitDeserializationSchema} for the Elasticsearch source.
     * The given schema will be used to deserialize {@link org.elasticsearch.search.SearchHit}s into
     * the output type.
     *
     * @param deserializationSchema the deserialization schema to use
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setDeserializationSchema(
            ElasticsearchSearchHitDeserializationSchema<OUT> deserializationSchema) {
        checkNotNull(deserializationSchema);
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Sets the hosts where the Elasticsearch cluster nodes are reachable.
     *
     * @param hosts http addresses describing the node locations
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty.");
        this.hosts = Arrays.asList(hosts);
        return this;
    }

    /**
     * Sets the username used to authenticate the connection with the Elasticsearch cluster.
     *
     * @param username of the Elasticsearch cluster user
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setConnectionUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }

    /**
     * Sets the password used to authenticate the connection with the Elasticsearch cluster.
     *
     * @param password of the Elasticsearch cluster user
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setConnectionPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }

    /**
     * Sets a prefix which used for every REST communication to the Elasticsearch cluster.
     *
     * @param prefix for the communication
     * @return this builder
     */
    public ElasticsearchSourceBuilder<OUT> setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return this;
    }

    private void checkRequiredParameters() {
        checkNotNull(hosts);
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");
        checkArgument(!indexName.isEmpty(), "Index name cannot be empty.");
        checkArgument(numberOfSearchSlices > 0, "Number of search slices must be greater than 0.");
        checkArgument(
                isGreaterOrEqual(pitKeepAlive, Duration.ofMinutes(5)),
                "PIT keep alive should be at least 5 minutes.");
        checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");
    }

    /** Builds the {@link ElasticsearchSource} using this preconfigured builder. */
    public ElasticsearchSource<OUT> build() {
        checkRequiredParameters();

        // TODO: Integrate timeout settings
        NetworkClientConfig networkClientConfig =
                new NetworkClientConfig(username, password, connectionPathPrefix, null, null, null);

        ElasticsearchSourceConfiguration sourceConfiguration =
                new ElasticsearchSourceConfiguration(
                        hosts, indexName, numberOfSearchSlices, pitKeepAlive);

        return new ElasticsearchSource<>(
                deserializationSchema, sourceConfiguration, networkClientConfig);
    }
}
