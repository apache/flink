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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend.ensureRocksDBIsLoaded;
import static org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.FIX_PER_TM_MEMORY_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.USE_MANAGED_MEMORY;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.WRITE_BUFFER_RATIO;
import static org.apache.flink.contrib.streaming.state.RocksDBSharedResourcesFactory.SLOT_SHARED_MANAGED;
import static org.apache.flink.contrib.streaming.state.RocksDBSharedResourcesFactory.SLOT_SHARED_UNMANAGED;
import static org.apache.flink.contrib.streaming.state.RocksDBSharedResourcesFactory.TM_SHARED_UNMANAGED;
import static org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** {@link RocksDBSharedResourcesFactory} test. */
public class RocksDBSharedResourcesFactoryTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBSharedResourcesFactoryTest.class);

    @TempDir static Path tempDir;

    @BeforeAll
    static void init() throws IOException {
        ensureRocksDBIsLoaded(tempDir.toAbsolutePath().toString());
    }

    private static Stream<Arguments> getSelectionStrategyParams() {
        Map<Object, Object> defaults = emptyMap();

        // format: job options, tm options, expected factory type
        return Stream.of(
                // default: per slot, managed
                arguments(defaults, defaults, SLOT_SHARED_MANAGED),
                // no sharing (allocate per column family), unmanaged
                arguments(singletonMap(USE_MANAGED_MEMORY, false), defaults, null),
                // prioritize managed (set explicitly)
                arguments(
                        singletonMap(USE_MANAGED_MEMORY, true),
                        singletonMap(FIX_PER_TM_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                        SLOT_SHARED_MANAGED),
                // prioritize managed (job default)
                arguments(
                        defaults,
                        singletonMap(FIX_PER_TM_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                        SLOT_SHARED_MANAGED),
                // prioritize fixed-per-slot over fixed-per-tm
                arguments(
                        singletonMap(FIX_PER_SLOT_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                        singletonMap(FIX_PER_TM_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                        SLOT_SHARED_UNMANAGED),
                // prioritize fixed-per-slot over managed
                arguments(
                        map(
                                entry(FIX_PER_SLOT_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                                entry(USE_MANAGED_MEMORY, true)),
                        singletonMap(FIX_PER_TM_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                        SLOT_SHARED_UNMANAGED),
                // use fixed-per-tm - when not managed and not fixed-per-slot
                arguments(
                        singletonMap(USE_MANAGED_MEMORY, false),
                        singletonMap(FIX_PER_TM_MEMORY_SIZE, MemorySize.ofMebiBytes(1)),
                        TM_SHARED_UNMANAGED));
    }

    @ParameterizedTest(name = "jobConfig: {0}, tmConfig: {1}")
    @MethodSource("getSelectionStrategyParams")
    @SuppressWarnings("rawtypes")
    public void testSelectionStrategy(
            Map<ConfigOption, Object> jobOptions,
            Map<ConfigOption, Object> tmOptions,
            RocksDBSharedResourcesFactory expected)
            throws IOException {

        Configuration jobConfig = new Configuration();
        jobOptions.forEach(jobConfig::set);

        Configuration tmConfig = new Configuration();
        tmOptions.forEach(tmConfig::set);

        assertEquals(
                expected,
                RocksDBSharedResourcesFactory.from(
                        RocksDBMemoryConfiguration.fromConfiguration(jobConfig), getEnv(tmConfig)));
    }

    @Test
    public void testTmSharedMemorySize() throws Exception {
        long size = 123L;
        double writeBufferRatio = .5;
        Configuration tmConfig = new Configuration();
        tmConfig.set(FIX_PER_TM_MEMORY_SIZE, new MemorySize(size));
        tmConfig.set(WRITE_BUFFER_RATIO, writeBufferRatio);

        OpaqueMemoryResource<RocksDBSharedResources> resource =
                TM_SHARED_UNMANAGED.create(
                        RocksDBMemoryConfiguration.fromConfiguration(new Configuration()),
                        getEnv(tmConfig),
                        0, // managed memory fraction must be ignored
                        LOG,
                        RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT);

        assertEquals(size, resource.getSize());
        assertEquals(
                calculateWriteBufferManagerCapacity(size, writeBufferRatio),
                resource.getResourceHandle().getWriteBufferManagerCapacity());
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
