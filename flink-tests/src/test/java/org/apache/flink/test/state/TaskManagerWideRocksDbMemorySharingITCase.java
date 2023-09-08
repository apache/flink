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

package org.apache.flink.test.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils.RocksDBMemoryFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.Cache;
import org.rocksdb.WriteBufferManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;

/**
 * Tests that {@link RocksDBOptions#FIX_PER_TM_MEMORY_SIZE} works as expected, i.e. make RocksDB use
 * the same BlockCache and WriteBufferManager objects. It does so using RocksDB metrics.
 */
public class TaskManagerWideRocksDbMemorySharingITCase extends TestLogger {
    private static final int PARALLELISM = 4;
    private static final int NUMBER_OF_JOBS = 5;
    private static final int NUMBER_OF_TASKS = NUMBER_OF_JOBS * PARALLELISM;
    private static final MemorySize SHARED_MEMORY = MemorySize.ofMebiBytes(NUMBER_OF_TASKS * 25);
    private MiniClusterWithClientResource cluster;

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Before
    public void init() throws Exception {
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getConfiguration())
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(NUMBER_OF_TASKS)
                                .build());
        cluster.before();
    }

    @After
    public void destroy() {
        cluster.after();
    }

    @Test
    public void testBlockCache() throws Exception {
        List<Cache> createdCaches = new CopyOnWriteArrayList<>();
        List<WriteBufferManager> createdWriteBufferManagers = new CopyOnWriteArrayList<>();
        TestingRocksDBMemoryFactory memoryFactory =
                new TestingRocksDBMemoryFactory(
                        sharedObjects.add(createdCaches),
                        sharedObjects.add(createdWriteBufferManagers));
        List<JobID> jobIDs = new ArrayList<>(NUMBER_OF_JOBS);
        try {
            for (int i = 0; i < NUMBER_OF_JOBS; i++) {
                jobIDs.add(cluster.getRestClusterClient().submitJob(dag(memoryFactory)).get());
            }
            for (JobID jid : jobIDs) {
                waitForAllTaskRunning(cluster.getMiniCluster(), jid, false);
            }
            Assert.assertEquals(1, createdCaches.size());
            Assert.assertEquals(1, createdWriteBufferManagers.size());

        } finally {
            for (JobID jobID : jobIDs) {
                try {
                    cluster.getRestClusterClient().cancel(jobID).get();
                } catch (Exception e) {
                    log.warn("Can not cancel job {}", jobID, e);
                }
            }
        }
    }

    private JobGraph dag(RocksDBMemoryFactory memoryFactory) {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);

        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        backend.setRocksDBMemoryFactory(memoryFactory);
        env.setStateBackend(backend);

        // don't flush memtables by checkpoints
        env.enableCheckpointing(24 * 60 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(noRestart());

        DataStreamSource<Long> src = env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE);
        src.keyBy(number -> number)
                .map(
                        new RichMapFunction<Long, Long>() {
                            private ListState<byte[]> state;
                            private int payloadSize;

                            @Override
                            public void open(OpenContext openContext) throws Exception {
                                super.open(openContext);
                                this.state =
                                        getRuntimeContext()
                                                .getListState(
                                                        new ListStateDescriptor<>(
                                                                "state", byte[].class));
                                // let each task to grow its state at a different speed
                                // to increase the probability of reporting different memory usages
                                // among different tasks
                                this.payloadSize = 4 + new Random().nextInt(7);
                            }

                            @Override
                            public Long map(Long value) throws Exception {
                                state.add(new byte[payloadSize]);
                                Thread.sleep(1L);
                                return value;
                            }
                        })
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph();
    }

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(RocksDBOptions.FIX_PER_TM_MEMORY_SIZE, SHARED_MEMORY);
        configuration.set(RocksDBOptions.USE_MANAGED_MEMORY, false);

        return configuration;
    }

    private static class TestingRocksDBMemoryFactory implements RocksDBMemoryFactory {
        private final SharedReference<List<Cache>> createdCaches;
        private final SharedReference<List<WriteBufferManager>> createdWriteBufferManagers;

        private TestingRocksDBMemoryFactory(
                SharedReference<List<Cache>> createdCaches,
                SharedReference<List<WriteBufferManager>> createdWriteBufferManagers) {
            this.createdCaches = createdCaches;
            this.createdWriteBufferManagers = createdWriteBufferManagers;
        }

        @Override
        public Cache createCache(long cacheCapacity, double highPriorityPoolRatio) {
            Cache cache =
                    RocksDBMemoryFactory.DEFAULT.createCache(cacheCapacity, highPriorityPoolRatio);
            createdCaches.get().add(cache);
            return cache;
        }

        @Override
        public WriteBufferManager createWriteBufferManager(
                long writeBufferManagerCapacity, Cache cache) {
            WriteBufferManager writeBufferManager =
                    RocksDBMemoryFactory.DEFAULT.createWriteBufferManager(
                            writeBufferManagerCapacity, cache);
            createdWriteBufferManagers.get().add(writeBufferManager);
            return writeBufferManager;
        }
    }
}
