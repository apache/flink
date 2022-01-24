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

package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.opensearch.OpensearchUtil;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.opensearch.testutils.SourceSinkDataTestKit;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.DockerImageVersions;

import org.apache.http.HttpHost;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/** IT cases for the {@link OpensearchSink}. */
public class OpensearchSinkITCase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OpensearchSinkITCase.class);

    @ClassRule
    public static final OpensearchContainer OS_CONTAINER =
            OpensearchUtil.createOpensearchContainer(DockerImageVersions.OPENSEARCH_1, LOG);

    @Test
    public void testOpensearchSink() throws Exception {
        runOpensearchSinkTest(
                "opensearch-sink-test-json-index", SourceSinkDataTestKit::getJsonSinkFunction);
    }

    @Test
    public void testOpensearchSinkWithSmile() throws Exception {
        runOpensearchSinkTest(
                "opensearch-sink-test-smile-index", SourceSinkDataTestKit::getSmileSinkFunction);
    }

    @Test
    public void testNullAddresses() {
        try {
            createOpensearchSink(
                    1, getClusterName(), null, SourceSinkDataTestKit.getJsonSinkFunction("test"));
        } catch (IllegalArgumentException | NullPointerException expectedException) {
            // test passes
            return;
        }

        fail();
    }

    @Test
    public void testEmptyAddresses() {
        try {
            createOpensearchSink(
                    1,
                    getClusterName(),
                    Collections.emptyList(),
                    SourceSinkDataTestKit.getJsonSinkFunction("test"));
        } catch (IllegalArgumentException expectedException) {
            // test passes
            return;
        }

        fail();
    }

    @Test
    public void testInvalidOpensearchCluster() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, String>> source =
                env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

        source.addSink(
                createOpensearchSinkForNode(
                        1,
                        "invalid-cluster-name",
                        SourceSinkDataTestKit.getJsonSinkFunction("test"),
                        "123.123.123.123")); // incorrect ip address

        try {
            env.execute("Opensearch Sink Test");
        } catch (JobExecutionException expectedException) {
            assertThat(expectedException.getCause(), instanceOf(JobException.class));
            return;
        }

        fail();
    }

    private String getClusterName() {
        return "docker-cluster";
    }

    private OpensearchSink<Tuple2<Integer, String>> createOpensearchSink(
            int bulkFlushMaxActions,
            String clusterName,
            List<HttpHost> httpHosts,
            OpensearchSinkFunction<Tuple2<Integer, String>> opensearchSinkFunction) {

        OpensearchSink.Builder<Tuple2<Integer, String>> builder =
                new OpensearchSink.Builder<>(httpHosts, opensearchSinkFunction);
        builder.setBulkFlushMaxActions(bulkFlushMaxActions);

        return builder.build();
    }

    private OpensearchSink<Tuple2<Integer, String>> createOpensearchSinkForNode(
            int bulkFlushMaxActions,
            String clusterName,
            OpensearchSinkFunction<Tuple2<Integer, String>> opensearchSinkFunction,
            String hostAddress) {

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(HttpHost.create(hostAddress));

        OpensearchSink.Builder<Tuple2<Integer, String>> builder =
                new OpensearchSink.Builder<>(httpHosts, opensearchSinkFunction);
        builder.setBulkFlushMaxActions(bulkFlushMaxActions);
        builder.setRestClientFactory(OpensearchUtil.createClientFactory(OS_CONTAINER));

        return builder.build();
    }

    private void runOpensearchSinkTest(
            String index,
            Function<String, OpensearchSinkFunction<Tuple2<Integer, String>>> functionFactory)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, String>> source =
                env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

        source.addSink(
                createOpensearchSinkForNode(
                        1,
                        getClusterName(),
                        functionFactory.apply(index),
                        OS_CONTAINER.getHttpHostAddress()));

        env.execute("Opensearch Sink Test");

        // verify the results
        final RestHighLevelClient client = OpensearchUtil.createClient(OS_CONTAINER);

        SourceSinkDataTestKit.verifyProducedSinkData(client, index);

        client.close();
    }
}
