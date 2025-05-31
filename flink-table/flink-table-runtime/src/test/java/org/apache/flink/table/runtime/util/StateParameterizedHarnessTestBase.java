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

import org.apache.flink.configuration.Configuration;
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

    @Parameters(name = "StateBackend={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[] {HEAP_BACKEND}, new Object[] {ROCKSDB_BACKEND});
    }
}
