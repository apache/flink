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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.testutils.MiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpHost;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link Elasticsearch7DynamicTableSource}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class Elasticsearch7DynamicTableSourceITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(Elasticsearch7DynamicTableSourceITCase.class);
    private static final int NUM_RECORDS = 10;
    private static final String INDEX = "my-index";

    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(new Configuration())
                            .build());

    @RegisterExtension
    public static final AllCallbackWrapper ALL_CALLBACK_WRAPPER =
            new AllCallbackWrapper(MINI_CLUSTER_RESOURCE);

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_7, LOG);

    private RestHighLevelClient client;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setup() {
        client =
                new RestHighLevelClient(
                        RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())));

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    }

    @AfterEach
    void teardown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testElasticsearchDynamicTableSource() throws Exception {
        writeTestData(NUM_RECORDS, INDEX);

        final String createTable =
                String.format(
                        "create table elasticsearch (\n"
                                + "data integer\n"
                                + ") with (\n"
                                + "'connector' = '%s',\n"
                                + "'%s' = '%s',\n "
                                + "'%s' = '%s'\n"
                                + ")",
                        Elasticsearch7DynamicTableSourceFactory.FACTORY_IDENTIFIER,
                        Elasticsearch7SourceOptions.HOSTS_OPTION.key(),
                        ES_CONTAINER.getHttpHostAddress(),
                        Elasticsearch7SourceOptions.INDEX_OPTION.key(),
                        INDEX);
        tEnv.executeSql(createTable);

        String query = "select data from elasticsearch";
        DataStream<Integer> result = tEnv.toDataStream(tEnv.sqlQuery(query), Integer.class);
        TestingSinkFunction sink = new TestingSinkFunction();
        result.addSink(sink).setParallelism(1);

        env.execute();

        Assertions.assertThat(TestingSinkFunction.rows)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    private void writeTestData(int numberOfRecords, String index) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numberOfRecords; i++) {
            bulkRequest.add(createIndexRequest(i, index));
        }

        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Could not write test data to Elasticsearch.");
        }
    }

    private static IndexRequest createIndexRequest(int datum, String index) {
        Map<String, Object> document = new HashMap<>();
        document.put("data", datum);
        return new IndexRequest(index).id(String.valueOf(datum)).source(document);
    }

    private static final class TestingSinkFunction implements SinkFunction<Integer> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private TestingSinkFunction() {
            rows.clear();
        }

        @Override
        public void invoke(Integer value, Context context) {

            rows.add(value.toString());
        }
    }
}
