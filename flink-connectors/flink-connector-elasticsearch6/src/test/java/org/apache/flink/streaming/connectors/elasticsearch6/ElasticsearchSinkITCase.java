/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;
import org.apache.flink.util.DockerImageVersions;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.ArrayList;
import java.util.List;

/** IT cases for the {@link ElasticsearchSink}. */
public class ElasticsearchSinkITCase
        extends ElasticsearchSinkTestBase<RestHighLevelClient, HttpHost> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkITCase.class);

    @ClassRule
    public static ElasticsearchContainer elasticsearchContainer =
            ElasticsearchUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_6, LOG);

    @Override
    protected String getClusterName() {
        return "docker-cluster";
    }

    @Override
    protected final RestHighLevelClient getClient() {
        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(elasticsearchContainer.getHttpHostAddress())));
    }

    @Test
    public void testElasticsearchSink() throws Exception {
        runElasticsearchSinkTest();
    }

    @Test
    public void testElasticsearchSinkWithSmile() throws Exception {
        runElasticsearchSinkSmileTest();
    }

    @Test
    public void testNullAddresses() {
        runNullAddressesTest();
    }

    @Test
    public void testEmptyAddresses() {
        runEmptyAddressesTest();
    }

    @Test
    public void testInvalidElasticsearchCluster() throws Exception {
        runInvalidElasticsearchClusterTest();
    }

    @Override
    protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient>
            createElasticsearchSink(
                    int bulkFlushMaxActions,
                    String clusterName,
                    List<HttpHost> httpHosts,
                    ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

        ElasticsearchSink.Builder<Tuple2<Integer, String>> builder =
                new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(bulkFlushMaxActions);

        return builder.build();
    }

    @Override
    protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient>
            createElasticsearchSinkForEmbeddedNode(
                    int bulkFlushMaxActions,
                    String clusterName,
                    ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

        return createElasticsearchSinkForNode(
                bulkFlushMaxActions,
                clusterName,
                elasticsearchSinkFunction,
                elasticsearchContainer.getHttpHostAddress());
    }

    @Override
    protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient>
            createElasticsearchSinkForNode(
                    int bulkFlushMaxActions,
                    String clusterName,
                    ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction,
                    String hostAddress) {

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(HttpHost.create(hostAddress));

        ElasticsearchSink.Builder<Tuple2<Integer, String>> builder =
                new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
        builder.setBulkFlushMaxActions(bulkFlushMaxActions);

        return builder.build();
    }
}
