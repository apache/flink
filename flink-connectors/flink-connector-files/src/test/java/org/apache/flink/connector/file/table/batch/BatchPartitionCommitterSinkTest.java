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

package org.apache.flink.connector.file.table.batch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemCommitterTest;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.PartitionCommitPolicyFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connector.file.table.batch.compact.BatchPartitionCommitterSink;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BatchPartitionCommitterSinkTest}. */
public class BatchPartitionCommitterSinkTest {
    private final FileSystemFactory fileSystemFactory = FileSystem::get;

    private TableMetaStoreFactory metaStoreFactory;
    private ObjectIdentifier identifier;
    @TempDir private java.nio.file.Path path;
    @TempDir private java.nio.file.Path outputPath;

    @BeforeEach
    public void before() {
        metaStoreFactory =
                new FileSystemCommitterTest.TestMetaStoreFactory(new Path(outputPath.toString()));
        identifier = ObjectIdentifier.of("hiveCatalog", "default", "test");
    }

    @Test
    public void testPartitionCommit() throws Exception {
        BatchPartitionCommitterSink committerSink =
                new BatchPartitionCommitterSink(
                        fileSystemFactory,
                        metaStoreFactory,
                        false,
                        false,
                        new Path(path.toString()),
                        new String[] {"p1", "p2"},
                        new LinkedHashMap<>(),
                        identifier,
                        new PartitionCommitPolicyFactory(null, null, null));
        committerSink.open(new Configuration());

        List<Path> pathList1 = createFiles(path, "task-1/p1=0/p2=0/", "f1", "f2");
        List<Path> pathList2 = createFiles(path, "task-2/p1=0/p2=0/", "f3");
        List<Path> pathList3 = createFiles(path, "task-2/p1=0/p2=1/", "f4");

        Map<String, List<Path>> compactedFiles = new HashMap<>();
        pathList1.addAll(pathList2);
        compactedFiles.put("p1=0/p2=0/", pathList1);
        compactedFiles.put("p1=0/p2=1/", pathList3);

        committerSink.invoke(new CompactMessages.CompactOutput(compactedFiles), TEST_SINK_CONTEXT);
        committerSink.setRuntimeContext(TEST_RUNTIME_CONTEXT);
        committerSink.finish();
        committerSink.close();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f1")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f2")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=0/f3")).exists();
        assertThat(new File(outputPath.toFile(), "p1=0/p2=1/f4")).exists();
    }

    private List<Path> createFiles(java.nio.file.Path parent, String path, String... files)
            throws IOException {
        java.nio.file.Path dir = Files.createDirectories(Paths.get(parent.toString(), path));
        List<Path> paths = new ArrayList<>();
        for (String file : files) {
            paths.add(new Path(Files.createFile(dir.resolve(file)).toFile().getPath()));
        }
        return paths;
    }

    static final RuntimeContext TEST_RUNTIME_CONTEXT = getRuntimeContext(new JobID());

    static final SinkFunction.Context TEST_SINK_CONTEXT =
            new SinkFunction.Context() {
                @Override
                public long currentProcessingTime() {
                    return 0;
                }

                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return null;
                }
            };

    static RuntimeContext getRuntimeContext(final JobID jobID) {
        return new RuntimeContext() {

            @Override
            public JobID getJobId() {
                return jobID;
            }

            @Override
            public String getTaskName() {
                return "test";
            }

            @Override
            public OperatorMetricGroup getMetricGroup() {
                return null;
            }

            @Override
            public int getNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public int getMaxNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public int getIndexOfThisSubtask() {
                return 0;
            }

            @Override
            public int getAttemptNumber() {
                return 0;
            }

            @Override
            public String getTaskNameWithSubtasks() {
                return "test";
            }

            @Override
            public ExecutionConfig getExecutionConfig() {
                return null;
            }

            @Override
            public ClassLoader getUserCodeClassLoader() {
                return null;
            }

            @Override
            public <V, A extends Serializable> void addAccumulator(
                    String name, Accumulator<V, A> accumulator) {}

            @Override
            public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
                return null;
            }

            @Override
            public void registerUserCodeClassLoaderReleaseHookIfAbsent(
                    String releaseHookName, Runnable releaseHook) {
                throw new UnsupportedOperationException();
            }

            @Override
            public IntCounter getIntCounter(String name) {
                return null;
            }

            @Override
            public LongCounter getLongCounter(String name) {
                return null;
            }

            @Override
            public DoubleCounter getDoubleCounter(String name) {
                return null;
            }

            @Override
            public Histogram getHistogram(String name) {
                return null;
            }

            @Override
            public boolean hasBroadcastVariable(String name) {
                return false;
            }

            @Override
            public <RT> List<RT> getBroadcastVariable(String name) {
                return null;
            }

            @Override
            public <T, C> C getBroadcastVariableWithInitializer(
                    String name, BroadcastVariableInitializer<T, C> initializer) {
                return null;
            }

            @Override
            public DistributedCache getDistributedCache() {
                return null;
            }

            @Override
            public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
                return null;
            }

            @Override
            public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
                return null;
            }

            @Override
            public <T> ReducingState<T> getReducingState(
                    ReducingStateDescriptor<T> stateProperties) {
                return null;
            }

            @Override
            public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
                    AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
                return null;
            }

            @Override
            public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <UK, UV> MapState<UK, UV> getMapState(
                    MapStateDescriptor<UK, UV> stateProperties) {
                return null;
            }
        };
    }
}
