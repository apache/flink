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

package org.apache.flink.state.forst;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.v2.StateBackendTestV2Base;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.state.forst.ForStOptions.LOCAL_DIRECTORIES;
import static org.apache.flink.state.forst.ForStOptions.REMOTE_DIRECTORY;

/** Tests for the async keyed state backend part of {@link ForStStateBackend}. */
@ExtendWith(ParameterizedTestExtension.class)
class ForStStateBackendTestV2 extends StateBackendTestV2Base<ForStStateBackend> {

    @TempDir private static java.nio.file.Path tempFolder;
    @TempDir private static java.nio.file.Path tempFolderForForStLocal;
    @TempDir private static java.nio.file.Path tempFolderForForstRemote;

    private static final SupplierWithException<CheckpointStorage, IOException>
            jobManagerCheckpointStorage = JobManagerCheckpointStorage::new;

    private static final SupplierWithException<CheckpointStorage, IOException>
            filesystemCheckpointStorage =
                    () -> {
                        String checkpointPath =
                                TempDirUtils.newFolder(tempFolder).toURI().toString();
                        return new FileSystemCheckpointStorage(new Path(checkpointPath), 0, -1);
                    };

    @Parameters(name = "CheckpointStorage: {0}, hasLocalDir: {1}, hasRemoteDir: {2}")
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {jobManagerCheckpointStorage, true, false},
                    {filesystemCheckpointStorage, true, false},
                    {filesystemCheckpointStorage, false, true},
                    {filesystemCheckpointStorage, true, true}
                });
    }

    @Parameter public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Parameter(1)
    public boolean hasLocalDir;

    @Parameter(2)
    public boolean hasRemoteDir;

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }

    @Override
    protected ConfigurableStateBackend getStateBackend() throws Exception {
        ForStStateBackend backend = new ForStStateBackend();
        Configuration config = new Configuration();
        if (hasLocalDir) {
            config.set(LOCAL_DIRECTORIES, tempFolderForForStLocal.toString());
        }
        if (hasRemoteDir) {
            config.set(REMOTE_DIRECTORY, tempFolderForForstRemote.toString());
        }
        backend.configure(config, Thread.currentThread().getContextClassLoader());
        return new ForStStateBackend();
    }
}
