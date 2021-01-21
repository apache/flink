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

import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/** Tests for the partitioned state part of {@link EmbeddedRocksDBStateBackend}. */
@RunWith(Parameterized.class)
public class EmbeddedRocksDBStateBackendMigrationTest
        extends StateBackendMigrationTestBase<EmbeddedRocksDBStateBackend> {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Parameterized.Parameters
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {
                        true,
                        (SupplierWithException<CheckpointStorage, IOException>)
                                JobManagerCheckpointStorage::new
                    },
                    {
                        false,
                        (SupplierWithException<CheckpointStorage, IOException>)
                                () -> {
                                    String checkpointPath =
                                            TEMP_FOLDER.newFolder().toURI().toString();
                                    return new FileSystemCheckpointStorage(checkpointPath);
                                }
                    }
                });
    }

    @Parameterized.Parameter(value = 0)
    public boolean enableIncrementalCheckpointing;

    @Parameterized.Parameter(value = 1)
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Override
    protected EmbeddedRocksDBStateBackend getStateBackend() throws Exception {
        return new EmbeddedRocksDBStateBackend(enableIncrementalCheckpointing);
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }
}
