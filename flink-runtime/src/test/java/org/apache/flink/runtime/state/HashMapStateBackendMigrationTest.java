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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

/** Tests for the partitioned state part of {@link HashMapStateBackend}. */
@RunWith(Parameterized.class)
public class HashMapStateBackendMigrationTest
        extends StateBackendMigrationTestBase<HashMapStateBackend> {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Parameterized.Parameters
    public static Object[] modes() {
        return new Object[] {
            (SupplierWithException<CheckpointStorage, IOException>)
                    JobManagerCheckpointStorage::new,
            (SupplierWithException<CheckpointStorage, IOException>)
                    () -> {
                        String checkpointPath = TEMP_FOLDER.newFolder().toURI().toString();
                        return new FileSystemCheckpointStorage(checkpointPath);
                    }
        };
    }

    @Parameterized.Parameter
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Override
    protected HashMapStateBackend getStateBackend() throws Exception {
        return new HashMapStateBackend();
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }
}
