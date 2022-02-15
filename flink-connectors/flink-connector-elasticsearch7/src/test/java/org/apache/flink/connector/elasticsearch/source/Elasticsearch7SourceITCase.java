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

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.reader.Elasticsearch7SearchHitDeserializationSchema;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.DockerImageVersions;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link Elasticsearch7Source}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
public class Elasticsearch7SourceITCase {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7SourceITCase.class);
    private static final int NUM_RECORDS = 100;
    private static final String INDEX = "my-index";

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_7, LOG);

    private RestHighLevelClient client;

    @BeforeEach
    void setup() {
        client =
                new RestHighLevelClient(
                        RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())));
    }

    @AfterEach
    void teardown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testReadingWithSource() throws Exception {
        NetworkClientConfig networkClientConfig =
                new NetworkClientConfig(null, null, null, null, null, null);

        Elasticsearch7SourceConfiguration sourceConfiguration =
                new Elasticsearch7SourceConfiguration(
                        Collections.singletonList(
                                HttpHost.create(ES_CONTAINER.getHttpHostAddress())),
                        INDEX,
                        3,
                        Duration.ofMinutes(5));

        Elasticsearch7Source<String> source =
                new Elasticsearch7Source<>(
                        new Elasticsearch7StringDeserializationSchema(),
                        sourceConfiguration,
                        networkClientConfig);

        testReadingFromSource(source);
    }

    @Test
    public void testReadingWithBuilder() throws Exception {
        Elasticsearch7Source<String> source =
                Elasticsearch7Source.<String>builder()
                        .setHosts(HttpHost.create(ES_CONTAINER.getHttpHostAddress()))
                        .setIndexName(INDEX)
                        .setDeserializationSchema(
                                new Elasticsearch7SearchHitDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            SearchHit record, Collector<String> out) {
                                        out.collect(record.getSourceAsString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return TypeInformation.of(String.class);
                                    }
                                })
                        .build();

        testReadingFromSource(source);
    }

    private void testReadingFromSource(Elasticsearch7Source<String> source) throws Exception {
        writeTestData(NUM_RECORDS, INDEX);

        final Configuration configuration = new Configuration();
        configuration.set(
                ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        final StreamExecutionEnvironment env = new LocalStreamEnvironment(configuration);
        env.enableCheckpointing(100L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<String> sourceRecords =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "elasticsearch-source");

        sourceRecords.addSink(
                new RichSinkFunction<String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext().addAccumulator("result", new ListAccumulator<String>());
                    }

                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });

        sourceRecords.print("Reading from source");

        List<String> result = env.execute().getAccumulatorResult("result");
        Assertions.assertEquals(NUM_RECORDS, result.size());
        for (int i = 0; i < NUM_RECORDS; i++) {
            Assertions.assertTrue(result.contains("{\"data\":" + i + "}"));
        }
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
}
