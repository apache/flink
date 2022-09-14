/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ElasticsearchSink}. */
@ExtendWith(TestLoggerExtension.class)
abstract class ElasticsearchSinkBaseITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkBaseITCase.class);
    protected static final String ELASTICSEARCH_PASSWORD = "test-password";
    protected static final String ELASTICSEARCH_USER = "elastic";

    private static boolean failed;

    private RestHighLevelClient client;
    private TestClientBase context;

    abstract String getElasticsearchHttpHostAddress();

    abstract TestClientBase createTestClient(RestHighLevelClient client);

    abstract ElasticsearchSinkBuilderBase<
                    Tuple2<Integer, String>, ? extends ElasticsearchSinkBuilderBase>
            getSinkBuilder();

    private RestHighLevelClient createRestHighLevelClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD));
        return new RestHighLevelClient(
                RestClient.builder(HttpHost.create(getElasticsearchHttpHostAddress()))
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder.setDefaultCredentialsProvider(
                                                credentialsProvider)));
    }

    @BeforeEach
    void setUp() {
        failed = false;
        client = createRestHighLevelClient();
        context = createTestClient(client);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testWriteToElasticSearchWithDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        final String index = "test-es-with-delivery-" + deliveryGuarantee;
        boolean failure = false;
        try {
            runTest(index, false, TestEmitter::jsonEmitter, deliveryGuarantee, null);
        } catch (IllegalStateException e) {
            failure = true;
            assertThat(deliveryGuarantee).isSameAs(DeliveryGuarantee.EXACTLY_ONCE);
        } finally {
            assertThat(failure).isEqualTo(deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE);
        }
    }

    @ParameterizedTest
    @MethodSource("elasticsearchEmitters")
    void testWriteJsonToElasticsearch(
            BiFunction<String, String, ElasticsearchEmitter<Tuple2<Integer, String>>>
                    emitterProvider)
            throws Exception {
        final String index = "test-elasticsearch-sink-" + UUID.randomUUID();
        runTest(index, false, emitterProvider, null);
    }

    @Test
    void testRecovery() throws Exception {
        final String index = "test-recovery-elasticsearch-sink";
        runTest(index, true, TestEmitter::jsonEmitter, new FailingMapper());
        assertThat(failed).isTrue();
    }

    private void runTest(
            String index,
            boolean allowRestarts,
            BiFunction<String, String, ElasticsearchEmitter<Tuple2<Integer, String>>>
                    emitterProvider,
            @Nullable MapFunction<Long, Long> additionalMapper)
            throws Exception {
        runTest(
                index,
                allowRestarts,
                emitterProvider,
                DeliveryGuarantee.AT_LEAST_ONCE,
                additionalMapper);
    }

    private void runTest(
            String index,
            boolean allowRestarts,
            BiFunction<String, String, ElasticsearchEmitter<Tuple2<Integer, String>>>
                    emitterProvider,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable MapFunction<Long, Long> additionalMapper)
            throws Exception {
        final ElasticsearchSink<Tuple2<Integer, String>> sink =
                getSinkBuilder()
                        .setHosts(HttpHost.create(getElasticsearchHttpHostAddress()))
                        .setEmitter(emitterProvider.apply(index, context.getDataFieldName()))
                        .setBulkFlushMaxActions(5)
                        .setConnectionUsername(ELASTICSEARCH_USER)
                        .setConnectionPassword(ELASTICSEARCH_PASSWORD)
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .build();

        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(100L);
        if (!allowRestarts) {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }
        DataStream<Long> stream = env.fromSequence(1, 5);

        if (additionalMapper != null) {
            stream = stream.map(additionalMapper);
        }

        stream.map(
                        new MapFunction<Long, Tuple2<Integer, String>>() {
                            @Override
                            public Tuple2<Integer, String> map(Long value) throws Exception {
                                return Tuple2.of(
                                        value.intValue(),
                                        TestClientBase.buildMessage(value.intValue()));
                            }
                        })
                .sinkTo(sink);
        env.execute();
        context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);
    }

    private static List<BiFunction<String, String, ElasticsearchEmitter<Tuple2<Integer, String>>>>
            elasticsearchEmitters() {
        return Lists.newArrayList(TestEmitter::jsonEmitter, TestEmitter::smileEmitter);
    }

    private static class FailingMapper implements MapFunction<Long, Long>, CheckpointListener {

        private int emittedRecords = 0;

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(50);
            emittedRecords++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (failed || emittedRecords == 0) {
                return;
            }
            failed = true;
            throw new Exception("Expected failure");
        }
    }
}
