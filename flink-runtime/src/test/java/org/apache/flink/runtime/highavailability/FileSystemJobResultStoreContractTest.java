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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.core.fs.Path;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

/**
 * Tests for the {@link FileSystemJobResultStore} implementation of the {@link JobResultStore}'s
 * contracts.
 */
public class FileSystemJobResultStoreContractTest implements JobResultStoreContractTest {
    @TempDir File temporaryFolder;

    @Override
    public JobResultStore createJobResultStore() throws IOException {
        Path path = new Path(temporaryFolder.toURI());
        return new FileSystemJobResultStore(
                path.getFileSystem(), path, false, Executors.directExecutor());
    }
}
