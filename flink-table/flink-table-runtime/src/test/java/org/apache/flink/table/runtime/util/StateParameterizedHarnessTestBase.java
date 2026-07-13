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

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public abstract class StateParameterizedHarnessTestBase {

    /**
     * Managed memory for the harness environment. Sized for state backends that reserve managed
     * memory per keyed-state DB (the harness default of 3 MB is too small for them); harmless for
     * the others.
     */
    private static final MemorySize MANAGED_MEMORY = MemorySize.ofMebiBytes(128);

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    public enum StateBackendMode {
        HEAP,
        ROCKSDB
    }

    protected static final StateBackendMode HEAP_BACKEND = StateBackendMode.HEAP;
    protected static final StateBackendMode ROCKSDB_BACKEND = StateBackendMode.ROCKSDB;

    private final StateBackendMode mode;
    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    protected StateParameterizedHarnessTestBase(StateBackendMode mode) {
        this.mode = mode;
        try {
            this.tempFolder.create();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create temporary folder in HarnessTestBase constructor", e);
        }
    }

    @AfterEach
    public void tearDownTemporaryFolder() {
        this.tempFolder.delete();
    }

    protected StateBackend getStateBackend() {
        switch (mode) {
            case HEAP:
                Configuration conf = new Configuration();
                return new HashMapStateBackend().configure(conf, classLoader);
            case ROCKSDB:
                return new EmbeddedRocksDBStateBackend();
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }

    protected CheckpointStorage getCheckpointStorage() {
        switch (mode) {
            case HEAP:
                return new JobManagerCheckpointStorage();
            case ROCKSDB:
                try {
                    return new FileSystemCheckpointStorage(
                            "file://" + tempFolder.getRoot().getAbsolutePath());
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Cannot create folder in temporary directory for checkpoint storage",
                            e);
                }
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }

    /**
     * Builds the {@link MockEnvironment} the test harness is constructed with. The harness would
     * otherwise build its own with only 3 MB of managed memory and no checkpoint storage. We
     * configure it the same way for every backend: enough managed memory, and a filesystem
     * checkpoint storage (so a backend that resolves a task-owned state directory from it works).
     */
    protected MockEnvironment createMockEnvironment() {
        String checkpointPath = "file://" + tempFolder.getRoot().getAbsolutePath();

        Configuration jobConfig = new Configuration();
        jobConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath);

        MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setManagedMemorySize(MANAGED_MEMORY.getBytes())
                        .setJobConfiguration(jobConfig)
                        .build();

        try {
            environment.setCheckpointStorageAccess(
                    new FileSystemCheckpointStorage(checkpointPath)
                            .createCheckpointStorage(new JobID()));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot create checkpoint storage for the test environment", e);
        }
        return environment;
    }

    @Parameters(name = "StateBackend={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[] {HEAP_BACKEND}, new Object[] {ROCKSDB_BACKEND});
    }
}
