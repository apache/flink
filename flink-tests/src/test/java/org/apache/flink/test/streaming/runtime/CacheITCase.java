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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.scheduler.ClusterDatasetCorruptedException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.CachedDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test datastream cache. */
public class CacheITCase extends AbstractTestBase {
    private StreamExecutionEnvironment env;
    private MiniClusterWithClientResource miniClusterWithClientResource;

    @BeforeEach
    void setUp() throws Exception {

        final Configuration configuration = new Configuration();
        miniClusterWithClientResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(8)
                                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                .withHaLeadershipControl()
                                .build());
        miniClusterWithClientResource.before();

        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        env =
                new TestStreamEnvironment(
                        miniClusterWithClientResource.getMiniCluster(),
                        configuration,
                        8,
                        Collections.emptyList(),
                        Collections.emptyList());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    }

    @AfterEach
    void tearDown() {
        miniClusterWithClientResource.after();
    }

    @Test
    void testCacheProduceAndConsume(@TempDir java.nio.file.Path tmpDir) throws Exception {
        File file = prepareTestData(tmpDir);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new org.apache.flink.core.fs.Path(file.getPath()))
                        .build();
        final CachedDataStream<Integer> cachedDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                        .map(i -> Integer.parseInt(i) + 1)
                        .cache();

        executeAndVerifyResult(tmpDir, cachedDataStream, "2", "3", "4");
        executeAndVerifyResult(tmpDir, cachedDataStream, "2", "3", "4");
    }

    @Test
    void testInvalidateCache(@TempDir java.nio.file.Path tmpDir) throws Exception {
        File file = prepareTestData(tmpDir);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new org.apache.flink.core.fs.Path(file.getPath()))
                        .build();

        final CachedDataStream<Integer> cachedDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                        .map(i -> Integer.parseInt(i) + 1)
                        .cache();

        executeAndVerifyResult(tmpDir, cachedDataStream, "2", "3", "4");
        cachedDataStream.invalidate();

        // overwrite the content of the source file
        assertThat(file.delete()).isTrue();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("4\n5\n6\n");
        }

        // after cache is invalidated it should re-read from source file with the updated content
        executeAndVerifyResult(tmpDir, cachedDataStream, "5", "6", "7");
    }

    @Test
    void testBatchProduceCacheStreamConsume(@TempDir java.nio.file.Path tmpDir) throws Exception {

        File file = prepareTestData(tmpDir);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new org.apache.flink.core.fs.Path(file.getPath()))
                        .build();
        final CachedDataStream<Integer> cachedDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                        .map(Integer::parseInt)
                        .map(i -> i + 1)
                        .cache();

        executeAndVerifyResult(tmpDir, cachedDataStream, "2", "3", "4");

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        final SingleOutputStreamOperator<Integer> dataStream = cachedDataStream.map(i -> i + 1);
        executeAndVerifyResult(tmpDir, dataStream, "3", "4", "5");
    }

    @Test
    void testCacheProduceAndConsumeWithDifferentPartitioner(@TempDir java.nio.file.Path tmpDir)
            throws Exception {

        final DataStreamSource<Tuple2<Integer, Integer>> ds =
                env.fromElements(new Tuple2<>(1, 1), new Tuple2<>(2, 1), new Tuple2<>(2, 1));

        final CachedDataStream<Tuple2<Integer, Integer>> cacheSource = ds.cache();
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result =
                cacheSource.keyBy(v -> v.f0).reduce((v1, v2) -> new Tuple2<>(v1.f0, v1.f1 + v2.f1));

        executeAndVerifyResult(tmpDir, result, "(1,1)", "(2,2)");

        result =
                cacheSource.keyBy(t -> t.f1).reduce((v1, v2) -> new Tuple2<>(v1.f0 + v2.f0, v1.f1));

        executeAndVerifyResult(tmpDir, result, "(5,1)");
    }

    @Test
    void testCacheSideOutput(@TempDir java.nio.file.Path tmpDir) throws Exception {
        OutputTag<Integer> tag = new OutputTag<Integer>("2") {};
        final DataStreamSource<Tuple2<Integer, Integer>> ds =
                env.fromElements(new Tuple2<>(1, 1), new Tuple2<>(2, 1), new Tuple2<>(2, 2));

        final SingleOutputStreamOperator<Integer> processed =
                ds.process(
                        new ProcessFunction<Tuple2<Integer, Integer>, Integer>() {
                            @Override
                            public void processElement(
                                    Tuple2<Integer, Integer> v,
                                    ProcessFunction<Tuple2<Integer, Integer>, Integer>.Context ctx,
                                    Collector<Integer> out) {
                                if (v.f0 == 2) {
                                    ctx.output(tag, v.f1);
                                    return;
                                }
                                out.collect(v.f1);
                            }
                        });

        final CachedDataStream<Integer> cachedSideOutput = processed.getSideOutput(tag).cache();
        executeAndVerifyResult(tmpDir, cachedSideOutput, "1", "2");

        executeAndVerifyResult(tmpDir, cachedSideOutput, "1", "2");
    }

    @Test
    void testRetryOnCorruptedClusterDataset(@TempDir java.nio.file.Path tmpDir) throws Exception {
        File file = prepareTestData(tmpDir);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new org.apache.flink.core.fs.Path(file.getPath()))
                        .build();
        final CachedDataStream<Integer> cachedDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                        .map(i -> Integer.parseInt(i) + 1)
                        .cache();

        executeAndVerifyResult(tmpDir, cachedDataStream, "2", "3", "4");

        final AbstractID datasetId =
                ((CacheTransformation<Integer>) cachedDataStream.getTransformation())
                        .getDatasetId();

        // overwrite the content of the source file
        assertThat(file.delete()).isTrue();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("4\n5\n6\n");
        }

        final SingleOutputStreamOperator<Integer> dataStream =
                cachedDataStream
                        .flatMap(
                                (FlatMapFunction<Integer, Integer>)
                                        (value, out) -> {
                                            if (value < 5) {
                                                // Simulate ClusterDatasetCorruptedException.
                                                throw new ClusterDatasetCorruptedException(
                                                        null,
                                                        Collections.singletonList(
                                                                new IntermediateDataSetID(
                                                                        datasetId)));
                                            }
                                            out.collect(value);
                                        })
                        .returns(Integer.class);
        executeAndVerifyResult(tmpDir, dataStream, "5", "6", "7");
    }

    private <T> void executeAndVerifyResult(
            Path tmpDir, DataStream<T> dataStream, String... expectedResult) throws Exception {
        File outputFile = new File(tmpDir.toFile(), UUID.randomUUID().toString());
        dataStream.sinkTo(getFileSink(outputFile));
        env.execute();
        assertThat(getFileContent(outputFile)).containsExactlyInAnyOrder(expectedResult);
    }

    private <T> FileSink<T> getFileSink(File outputFile) {
        return FileSink.forRowFormat(
                        new org.apache.flink.core.fs.Path(outputFile.getPath()),
                        new SimpleStringEncoder<T>())
                .build();
    }

    private static List<String> getFileContent(File directory) throws IOException {
        List<String> res = new ArrayList<>();

        final Collection<File> filesInBucket = FileUtils.listFiles(directory, null, true);
        for (File file : filesInBucket) {
            res.addAll(Arrays.asList(FileUtils.readFileToString(file).split("\n")));
        }

        return res;
    }

    private File prepareTestData(Path tmpDir) throws IOException {
        final File datafile = new File(tmpDir.toFile(), UUID.randomUUID().toString());
        try (FileWriter writer = new FileWriter(datafile)) {
            writer.write("1\n2\n3\n");
        }
        return datafile;
    }
}
