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

package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.hbase.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;
import org.apache.flink.connector.hbase.testutil.FailureSink;
import org.apache.flink.connector.hbase.testutil.FileSignal;
import org.apache.flink.connector.hbase.testutil.HBaseTestCluster;
import org.apache.flink.connector.hbase.testutil.TestsWithTestHBaseCluster;
import org.apache.flink.connector.hbase.testutil.Util;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;

/** Tests the most basic use cases of the source with a mocked HBase system. */
public class HBaseSourceITCase extends TestsWithTestHBaseCluster {

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(8)
                            .build());

    private DataStream<String> streamFromHBaseSource(
            StreamExecutionEnvironment environment, String tableName) {
        return streamFromHBaseSource(environment, tableName, 1);
    }

    private DataStream<String> streamFromHBaseSource(
            StreamExecutionEnvironment environment, String tableName, int parallelism) {
        HBaseStringDeserializationScheme deserializationScheme =
                new HBaseStringDeserializationScheme();
        HBaseSource<String> source =
                HBaseSource.builder()
                        .setTableName(tableName)
                        .setSourceDeserializer(deserializationScheme)
                        .setHBaseConfiguration(cluster.getConfig())
                        .build();
        environment.setParallelism(parallelism);
        DataStream<String> stream =
                environment.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "hbaseSourceITCase",
                        deserializationScheme.getProducedType());
        return stream;
    }

    private static <T> void expectFirstValuesToBe(
            DataStream<T> stream, T[] expectedValues, String message) {

        List<T> collectedValues = new ArrayList<>();
        stream.flatMap(
                new RichFlatMapFunction<T, Object>() {

                    @Override
                    public void flatMap(T value, Collector<Object> out) {
                        LOG.info("Test collected: {}", value);
                        collectedValues.add(value);
                        if (collectedValues.size() == expectedValues.length) {
                            assertArrayEquals(message, expectedValues, collectedValues.toArray());
                            throw new SuccessException();
                        }
                    }
                });
    }

    private void doAndWaitForSuccess(
            StreamExecutionEnvironment env, RunnableWithException action, int timeout) {
        try {
            JobClient jobClient = env.executeAsync();
            Util.waitForClusterStart(miniClusterResource.getMiniCluster());

            action.run();
            jobClient.getJobExecutionResult().get(timeout, TimeUnit.SECONDS);
            jobClient.cancel();
            throw new RuntimeException("Waiting for the correct data timed out");
        } catch (Exception exception) {
            if (!causedBySuccess(exception)) {
                throw new RuntimeException("Test failed", exception);
            } else {
                // Normal termination
            }
        }
    }

    private void waitUntilNReplicationPeers(int n)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture.runAsync(
                        ThrowingRunnable.unchecked(
                                () -> {
                                    while (cluster.getReplicationPeers().size() != n) {
                                        sleep(1000);
                                    }
                                }))
                .get(90, TimeUnit.SECONDS);
    }

    @Test
    public void testBasicPut() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        cluster.makeTable(baseTableName, DEFAULT_COLUMNFAMILY_COUNT);
        String[] expectedValues = uniqueValues(2 * DEFAULT_COLUMNFAMILY_COUNT);

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the right values after a basic put operation");
        doAndWaitForSuccess(
                env,
                () -> cluster.put(baseTableName, DEFAULT_COLUMNFAMILY_COUNT, expectedValues),
                120);
    }

    @Test
    public void testOnlyReplicateSpecifiedTable() throws Exception {
        String secondTable = baseTableName + "-table2";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        cluster.makeTable(baseTableName, DEFAULT_COLUMNFAMILY_COUNT);
        cluster.makeTable(secondTable, DEFAULT_COLUMNFAMILY_COUNT);
        String[] expectedValues = uniqueValues(DEFAULT_COLUMNFAMILY_COUNT);

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the values of the correct table");
        doAndWaitForSuccess(
                env,
                () -> {
                    cluster.put(
                            secondTable,
                            DEFAULT_COLUMNFAMILY_COUNT,
                            uniqueValues(DEFAULT_COLUMNFAMILY_COUNT));
                    sleep(2000);
                    cluster.put(baseTableName, DEFAULT_COLUMNFAMILY_COUNT, expectedValues);
                },
                180);
    }

    @Test
    public void testRecordsAreProducedExactlyOnceWithCheckpoints() throws Exception {
        final String collectedValueSignal = "collectedValue";
        final FileSignal testOracle = this.testOracle;
        String[] expectedValues = uniqueValues(20);
        cluster.makeTable(baseTableName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName);
        stream.addSink(
                new FailureSink<String>(3500) {

                    private void checkForSuccess() {
                        List<String> checkpointed = getCheckpointedValues();
                        LOG.info(
                                "\n\tUncheckpointed: {}\n\tCheckpointed: {}",
                                unCheckpointedValues,
                                checkpointed);
                        if (checkpointed.size() == expectedValues.length) {
                            try {
                                assertArrayEquals(
                                        "Wrong values were produced.",
                                        expectedValues,
                                        checkpointed.toArray());
                                testOracle.signalSuccess();
                            } catch (ArrayComparisonFailure e) {
                                LOG.error("Comparison failed", e);
                                testOracle.signalFailure();
                                throw e;
                            }
                        }
                    }

                    @Override
                    public void collectValue(String value) {

                        if (getCheckpointedValues().contains(value)) {
                            LOG.error("Unique value {} was not seen only once", value);
                            testOracle.signalFailure();
                            throw new RuntimeException(
                                    "Unique value " + value + " was not seen only once");
                        }
                        checkForSuccess();
                        testOracle.signal(collectedValueSignal + value);
                    }

                    @Override
                    public void checkpoint() {
                        checkForSuccess();
                    }
                });

        JobClient jobClient = env.executeAsync();
        Util.waitForClusterStart(miniClusterResource.getMiniCluster());
        try {
            Thread.sleep(8000);
            int putsPerPackage = 5;
            for (int i = 0; i < expectedValues.length; i += putsPerPackage) {
                LOG.info("Sending next package ...");
                CompletableFuture<String>[] signalsToWait = new CompletableFuture[putsPerPackage];
                for (int j = i; j < expectedValues.length && j < i + putsPerPackage; j++) {
                    cluster.put(baseTableName, expectedValues[j]);
                    signalsToWait[j - i] =
                            testOracle.awaitSignal(collectedValueSignal + expectedValues[j]);
                }

                // Assert that values have actually been sent over so there was an opportunity to
                // checkpoint them
                testOracle.awaitThrowOnFailure(
                        CompletableFuture.allOf(signalsToWait),
                        240,
                        TimeUnit.SECONDS,
                        "Failure occurred while waiting for package to be collected");

                Thread.sleep(3000);
                LOG.info("Consuming collection signal");
            }
            LOG.info("Finished sending packages, awaiting success ...");
            testOracle.awaitSuccess(120, TimeUnit.SECONDS);
            LOG.info("Received success, ending test ...");
        } finally {
            LOG.info("Cancelling job client");
            jobClient.cancel();
        }
        LOG.info("End of test method reached");
    }

    @Test
    public void testMultipleSplitReadersAreCreated() throws Exception {
        int parallelism = 4;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        streamFromHBaseSource(env, baseTableName, parallelism).print();
        cluster.makeTable(baseTableName, parallelism);
        JobClient jobClient = env.executeAsync();
        waitUntilNReplicationPeers(parallelism);
        jobClient.cancel();
    }

    @Test
    public void testBasicPutWhenMoreColumnFamiliesThanThreads() throws Exception {
        int parallelism = 1;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = streamFromHBaseSource(env, baseTableName, parallelism);

        String[] expectedValues = new String[] {"foo", "bar", "baz", "boo"};
        assert expectedValues.length > parallelism;
        cluster.makeTable(baseTableName, expectedValues.length);
        Put put = new Put("rowkey".getBytes());
        for (int i = 0; i < expectedValues.length; i++) {
            put.addColumn(
                    (HBaseTestCluster.COLUMN_FAMILY_BASE + i).getBytes(),
                    expectedValues[i].getBytes(),
                    expectedValues[i].getBytes());
        }
        cluster.commitPut(baseTableName, put);

        expectFirstValuesToBe(
                stream,
                expectedValues,
                "HBase source did not produce the right values after a multi-column-family put");
        doAndWaitForSuccess(env, () -> {}, 120);
    }

    /** Simple Deserialization Scheme to get event payloads as String. */
    public static class HBaseStringDeserializationScheme
            implements HBaseSourceDeserializer<String> {
        @Override
        public String deserialize(HBaseSourceEvent event) {
            return new String(event.getPayload(), HBaseEvent.DEFAULT_CHARSET);
        }
    }
}
