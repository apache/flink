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
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.DockerImageVersions;

import org.apache.http.HttpHost;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link org.apache.flink.connector.elasticsearch.table.ElasticsearchDynamicSource}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class ElasticsearchDynamicSourceITCase {
    private static final Logger LOG =
            LoggerFactory.getLogger(ElasticsearchDynamicSourceITCase.class);
    private static final int NUM_RECORDS = 10;
    private static final String INDEX = "my-index";

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            new ElasticsearchContainer(DockerImageName.parse(DockerImageVersions.ELASTICSEARCH_7))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

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
                        Elasticsearch7DynamicSourceFactory.FACTORY_IDENTIFIER,
                        ElasticsearchSourceOptions.HOSTS_OPTION.key(),
                        ES_CONTAINER.getHttpHostAddress(),
                        ElasticsearchSourceOptions.INDEX_OPTION.key(),
                        INDEX);
        tEnv.executeSql(createTable);

        String query = "select data from elasticsearch";
        DataStream<Integer> result = tEnv.toDataStream(tEnv.sqlQuery(query), Integer.class);
        TestingSinkFunction sink = new TestingSinkFunction();
        result.addSink(sink).setParallelism(1);
        result.print("elasticsearch ");

        env.execute();

        Assertions.assertThat(TestingSinkFunction.rows)
                .containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    private void writeTestData(int numberOfRecords, String index) {
        for (int i = 0; i < numberOfRecords; i++) {
            try {
                client.index(
                        createIndexRequest(i, index),
                        RequestOptions.DEFAULT); // TODO: use bulkrequest
            } catch (IOException e) {
                throw new RuntimeException("Could not write test data to Elasticsearch.");
            }
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
