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

package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.OpensearchUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link OpensearchSink}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class OpensearchSinkITCase {
    protected static final Logger LOG = LoggerFactory.getLogger(OpensearchSinkITCase.class);
    private static boolean failed;

    private RestHighLevelClient client;
    private OpensearchTestClient context;

    @Container
    private static final OpensearchContainer OS_CONTAINER =
            OpensearchUtil.createOpensearchContainer(DockerImageVersions.OPENSEARCH_1, LOG);

    @BeforeEach
    void setUp() {
        failed = false;
        client = OpensearchUtil.createClient(OS_CONTAINER);
        context = new OpensearchTestClient(client);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testWriteToOpensearchWithDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee)
            throws Exception {
        final String index = "test-opensearch-with-delivery-" + deliveryGuarantee;
        boolean failure = false;
        try {
            runTest(index, false, TestEmitter::jsonEmitter, deliveryGuarantee, null);
        } catch (IllegalStateException e) {
            failure = true;
            assertSame(deliveryGuarantee, DeliveryGuarantee.EXACTLY_ONCE);
        } finally {
            assertEquals(failure, deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE);
        }
    }

    @ParameterizedTest
    @MethodSource("opensearchEmitters")
    void testWriteJsonToOpensearch(
            BiFunction<String, String, OpensearchEmitter<Tuple2<Integer, String>>> emitterProvider)
            throws Exception {
        final String index = "test-opensearch-sink-" + UUID.randomUUID();
        runTest(index, false, emitterProvider, null);
    }

    @Test
    void testRecovery() throws Exception {
        final String index = "test-recovery-opensearch-sink";
        runTest(index, true, TestEmitter::jsonEmitter, new FailingMapper());
        assertTrue(failed);
    }

    private void runTest(
            String index,
            boolean allowRestarts,
            BiFunction<String, String, OpensearchEmitter<Tuple2<Integer, String>>> emitterProvider,
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
            BiFunction<String, String, OpensearchEmitter<Tuple2<Integer, String>>> emitterProvider,
            DeliveryGuarantee deliveryGuarantee,
            @Nullable MapFunction<Long, Long> additionalMapper)
            throws Exception {
        final OpensearchSink<Tuple2<Integer, String>> sink =
                new OpensearchSinkBuilder<>()
                        .setHosts(HttpHost.create(OS_CONTAINER.getHttpHostAddress()))
                        .setEmitter(emitterProvider.apply(index, context.getDataFieldName()))
                        .setBulkFlushMaxActions(5)
                        .setConnectionUsername(OS_CONTAINER.getUsername())
                        .setConnectionPassword(OS_CONTAINER.getPassword())
                        .setDeliveryGuarantee(deliveryGuarantee)
                        .setAllowInsecure(true)
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
                                        OpensearchTestClient.buildMessage(value.intValue()));
                            }
                        })
                .sinkTo(sink);
        env.execute();
        context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);
    }

    private static List<BiFunction<String, String, OpensearchEmitter<Tuple2<Integer, String>>>>
            opensearchEmitters() {
        return Arrays.asList(TestEmitter::jsonEmitter, TestEmitter::smileEmitter);
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
