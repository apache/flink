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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FsCompletedCheckpointStorageLocation}. */
class FsCompletedCheckpointStorageLocationTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void testDisposeStorageLocationDeletesRecursively() throws Exception {
        // Create a checkpoint directory with nested files
        File checkpointDir = TempDirUtils.newFolder(tempDir, "checkpoint");
        File subDir = new File(checkpointDir, "subdir");
        assertThat(subDir.mkdir()).isTrue();
        File file1 = new File(checkpointDir, "file1.txt");
        File file2 = new File(subDir, "file2.txt");
        assertThat(file1.createNewFile()).isTrue();
        assertThat(file2.createNewFile()).isTrue();

        // Create metadata file
        File metadataFile = new File(checkpointDir, "_metadata");
        assertThat(metadataFile.createNewFile()).isTrue();

        FileSystem fs = LocalFileSystem.getSharedInstance();
        Path checkpointPath = Path.fromLocalFile(checkpointDir);
        FileStateHandle metadataHandle = new FileStateHandle(Path.fromLocalFile(metadataFile), 0);
        String externalPointer = "test-pointer";

        FsCompletedCheckpointStorageLocation location =
                new FsCompletedCheckpointStorageLocation(
                        fs, checkpointPath, metadataHandle, externalPointer);

        // Verify files exist before disposal
        assertThat(checkpointDir.exists()).isTrue();
        assertThat(file1.exists()).isTrue();
        assertThat(file2.exists()).isTrue();
        assertThat(subDir.exists()).isTrue();

        // Execute
        location.disposeStorageLocation();

        // Verify that the entire directory and all its contents are deleted
        assertThat(checkpointDir.exists()).isFalse();
        assertThat(file1.exists()).isFalse();
        assertThat(file2.exists()).isFalse();
        assertThat(subDir.exists()).isFalse();
    }
}
