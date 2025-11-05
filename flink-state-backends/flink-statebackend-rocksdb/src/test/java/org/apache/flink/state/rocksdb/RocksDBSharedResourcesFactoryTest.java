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

package org.apache.flink.state.rocksdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils.RocksDBMemoryFactory;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.FIX_PER_TM_MEMORY_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.USE_MANAGED_MEMORY;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.WRITE_BUFFER_RATIO;
import static org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution;
import static org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded;
import static org.apache.flink.state.rocksdb.MemoryShareScope.SLOT;
import static org.apache.flink.state.rocksdb.RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity;
import static org.apache.flink.state.rocksdb.RocksDBSharedResourcesFactory.SLOT_SHARED_MANAGED;
import static org.apache.flink.state.rocksdb.RocksDBSharedResourcesFactory.SLOT_SHARED_UNMANAGED;
import static org.apache.flink.state.rocksdb.RocksDBSharedResourcesFactory.TM_SHARED_UNMANAGED;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** {@link RocksDBSharedResourcesFactory} test. */
@SuppressWarnings({"rawtypes", "DataFlowIssue", "SameParameterValue"})
public class RocksDBSharedResourcesFactoryTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBSharedResourcesFactoryTest.class);

    private static final MemorySize TM_SIZE = new MemorySize(20);
    private static final MemorySize MANAGED_MEMORY_SIZE = new MemorySize(15);
    public static final MemorySize PER_SLOT = new MemorySize(10);

    @TempDir static Path tempDir;

    @BeforeAll
    static void init() throws IOException {
        ensureRocksDBIsLoaded(tempDir.toAbsolutePath().toString());
    }

    @Test
    public void testDefaults() throws Exception {
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                emptyMap(),
                emptyMap(),
                SLOT,
                MANAGED_MEMORY_SIZE,
                true,
                SLOT_SHARED_MANAGED);
    }

    @Test
    public void testDisableManaged() throws Exception {
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                singletonMap(USE_MANAGED_MEMORY, false),
                emptyMap(),
                null, // everything else below is ignored
                MemorySize.ZERO,
                false,
                null);
    }

    @Test
    public void testEnableManaged() throws Exception {
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                singletonMap(USE_MANAGED_MEMORY, true),
                singletonMap(FIX_PER_TM_MEMORY_SIZE, TM_SIZE), // ignore
                SLOT,
                MANAGED_MEMORY_SIZE,
                true,
                SLOT_SHARED_MANAGED);
    }

    @Test
    public void testEnableManagedByDefault() throws Exception {
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                emptyMap(),
                singletonMap(FIX_PER_TM_MEMORY_SIZE, TM_SIZE), // ignore
                SLOT,
                MANAGED_MEMORY_SIZE,
                true,
                SLOT_SHARED_MANAGED);
    }

    @Test
    public void testPrioritizeFixedPerSlotOverTm() throws Exception {
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                singletonMap(FIX_PER_SLOT_MEMORY_SIZE, PER_SLOT),
                singletonMap(FIX_PER_TM_MEMORY_SIZE, TM_SIZE),
                SLOT,
                PER_SLOT,
                false,
                SLOT_SHARED_UNMANAGED);
    }

    @Test
    public void testPrioritizeFixedPerSlotOverManaged() throws Exception {
        MemorySize perSlot = PER_SLOT;
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                map(entry(FIX_PER_SLOT_MEMORY_SIZE, perSlot), entry(USE_MANAGED_MEMORY, true)),
                singletonMap(FIX_PER_TM_MEMORY_SIZE, TM_SIZE),
                SLOT,
                perSlot,
                false,
                SLOT_SHARED_UNMANAGED);
    }

    @Test
    public void testFixedPerTm() throws Exception {
        testSelectionStrategy(
                MANAGED_MEMORY_SIZE,
                singletonMap(USE_MANAGED_MEMORY, false),
                singletonMap(FIX_PER_TM_MEMORY_SIZE, TM_SIZE),
                MemoryShareScope.TM,
                MemorySize.ZERO,
                false,
                TM_SHARED_UNMANAGED);
    }

    private void testSelectionStrategy(
            MemorySize managedMemorySize,
            Map<ConfigOption, Object> jobOptions,
            Map<ConfigOption, Object> tmOptions,
            MemoryShareScope expectedScope,
            MemorySize expectedSize,
            Boolean expectManaged,
            RocksDBSharedResourcesFactory expectedFactory)
            throws Exception {

        Configuration jobConfig = new Configuration();
        jobOptions.forEach(jobConfig::set);

        Configuration tmConfig = new Configuration();
        tmOptions.forEach(tmConfig::set);

        RocksDBMemoryConfiguration jobMemoryConfig =
                RocksDBMemoryConfiguration.fromConfiguration(jobConfig);
        RocksDBSharedResourcesFactory actualFactory =
                RocksDBSharedResourcesFactory.from(jobMemoryConfig, getEnv(tmConfig));
        assertEquals(expectedFactory, actualFactory);
        if (expectedScope == null) {
            return;
        }
        assertEquals(expectManaged, actualFactory.isManaged());
        assertEquals(expectedScope, actualFactory.getShareScope());
        MockEnvironment env =
                MockEnvironment.builder()
                        .setManagedMemorySize(managedMemorySize.getBytes())
                        .build();
        OpaqueMemoryResource<RocksDBSharedResources> resource1 =
                actualFactory.create(jobMemoryConfig, env, 1, LOG, RocksDBMemoryFactory.DEFAULT);
        assertEquals(expectedSize.getBytes(), resource1.getSize());
        OpaqueMemoryResource<RocksDBSharedResources> resource2 =
                actualFactory.create(jobMemoryConfig, env, 1, LOG, RocksDBMemoryFactory.DEFAULT);
        assertSame(resource1.getResourceHandle(), resource2.getResourceHandle());
    }

    @Test
    public void testTmSharedMemorySize() throws Exception {
        long size = 123L;
        double writeBufferRatio = .5;
        Configuration tmConfig = new Configuration();
        tmConfig.set(FIX_PER_TM_MEMORY_SIZE, new MemorySize(size));
        tmConfig.set(WRITE_BUFFER_RATIO, writeBufferRatio);

        try (OpaqueMemoryResource<RocksDBSharedResources> resource =
                TM_SHARED_UNMANAGED.create(
                        RocksDBMemoryConfiguration.fromConfiguration(new Configuration()),
                        getEnv(tmConfig),
                        0, // managed memory fraction must be ignored
                        LOG,
                        RocksDBMemoryFactory.DEFAULT)) {

            assertEquals(size, resource.getSize());
            assertEquals(
                    calculateWriteBufferManagerCapacity(size, writeBufferRatio),
                    resource.getResourceHandle().getWriteBufferManagerCapacity());
        }
    }

    private static Environment getEnv(Configuration tmConfig) throws IOException {
        return MockEnvironment.builder()
                .setTaskManagerRuntimeInfo(
                        TaskManagerConfiguration.fromConfiguration(
                                tmConfig,
                                resourceSpecFromConfigForLocalExecution(tmConfig),
                                "localhost",
                                File.createTempFile("prefix", "suffix")))
                .build();
    }
}
