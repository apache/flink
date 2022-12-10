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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/** Tests for the partitioned state part of {@link RocksDBStateBackend}. */
public class RocksDBStateBackendMigrationTest
        extends StateBackendMigrationTestBase<RocksDBStateBackend> {
    // Store it because we need it for the cleanup test.
    public static String dbPath;

    @TempDir static File tmpDbPath;

    @TempDir static File tmpCheckpointPath;

    @Parameter public boolean enableIncrementalCheckpointing;

    @Parameters(name = "Incremental checkpointing: {0}")
    public static List<Object> modes() {
        return Arrays.asList(false, true);
    }

    @Override
    protected RocksDBStateBackend getStateBackend() throws IOException {
        dbPath = tmpDbPath.getAbsolutePath();
        String checkpointPath = tmpCheckpointPath.toURI().toString();
        RocksDBStateBackend backend =
                new RocksDBStateBackend(
                        new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);

        Configuration configuration = new Configuration();
        configuration.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        backend.setDbStoragePath(dbPath);
        return backend;
    }
}
