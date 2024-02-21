/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SnapshotDirectory}. */
class SnapshotDirectoryTest {

    @TempDir private Path temporaryFolder;

    /** Tests if mkdirs for snapshot directories works. */
    @Test
    void mkdirs() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File newFolder = new File(folderRoot, String.valueOf(UUID.randomUUID()));
        File innerNewFolder = new File(newFolder, String.valueOf(UUID.randomUUID()));
        Path path = innerNewFolder.toPath();

        assertThat(newFolder).doesNotExist();
        assertThat(innerNewFolder).doesNotExist();
        SnapshotDirectory snapshotDirectory = SnapshotDirectory.permanent(path);
        assertThat(snapshotDirectory.exists()).isFalse();
        assertThat(newFolder).doesNotExist();
        assertThat(innerNewFolder).doesNotExist();

        assertThat(snapshotDirectory.mkdirs()).isTrue();
        assertThat(newFolder).isDirectory();
        assertThat(innerNewFolder).isDirectory();
        assertThat(snapshotDirectory.exists()).isTrue();
    }

    /** Tests if indication of directory existence works. */
    @Test
    void exists() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));

        assertThat(folderA).doesNotExist();
        Path path = folderA.toPath();
        SnapshotDirectory snapshotDirectory = SnapshotDirectory.permanent(path);
        assertThat(snapshotDirectory.exists()).isFalse();
        assertThat(folderA.mkdirs()).isTrue();
        assertThat(snapshotDirectory.exists()).isTrue();
        assertThat(folderA.delete()).isTrue();
        assertThat(snapshotDirectory.exists()).isFalse();
    }

    /** Tests listing of file statuses works like listing on the path directly. */
    @Test
    void listStatus() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
        File folderB = new File(folderA, String.valueOf(UUID.randomUUID()));
        assertThat(folderB.mkdirs()).isTrue();
        File file = new File(folderA, "test.txt");
        assertThat(file.createNewFile()).isTrue();

        Path path = folderA.toPath();
        SnapshotDirectory snapshotDirectory = SnapshotDirectory.permanent(path);
        assertThat(snapshotDirectory.exists()).isTrue();

        assertThat(Arrays.toString(snapshotDirectory.listDirectory()))
                .isEqualTo(Arrays.toString(snapshotDirectory.listDirectory()));
        assertThat(Arrays.toString(snapshotDirectory.listDirectory()))
                .isEqualTo(Arrays.toString(FileUtils.listDirectory(path)));
    }

    /**
     * Tests that reporting the handle of a completed snapshot works as expected and that the
     * directory for completed snapshot is not deleted by {@link #deleteIfNotCompeltedSnapshot()}.
     */
    @Test
    void completeSnapshotAndGetHandle() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
        assertThat(folderA.mkdirs()).isTrue();
        Path folderAPath = folderA.toPath();

        SnapshotDirectory snapshotDirectory = SnapshotDirectory.permanent(folderAPath);

        // check that completed checkpoint dirs are not deleted as incomplete.
        DirectoryStateHandle handle = snapshotDirectory.completeSnapshotAndGetHandle();
        assertThat(handle).isNotNull();
        assertThat(snapshotDirectory.cleanup()).isTrue();
        assertThat(folderA).isDirectory();
        assertThat(handle.getDirectory()).isEqualTo(folderAPath);
        handle.discardState();

        assertThat(folderA).doesNotExist();
        assertThat(folderA.mkdirs()).isTrue();

        SnapshotDirectory newSnapshotDirectory = SnapshotDirectory.permanent(folderAPath);
        assertThat(newSnapshotDirectory.cleanup()).isTrue();
        assertThatThrownBy(newSnapshotDirectory::completeSnapshotAndGetHandle)
                .isInstanceOf(IOException.class);
    }

    /**
     * Tests that snapshot director behaves correct for delete calls. Completed snapshots should not
     * be deleted, only ongoing snapshots can.
     */
    @Test
    void deleteIfNotCompeltedSnapshot() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
        File folderB = new File(folderA, String.valueOf(UUID.randomUUID()));
        assertThat(folderB.mkdirs()).isTrue();
        File file = new File(folderA, "test.txt");
        assertThat(file.createNewFile()).isTrue();
        Path folderAPath = folderA.toPath();
        SnapshotDirectory snapshotDirectory = SnapshotDirectory.permanent(folderAPath);
        assertThat(snapshotDirectory.cleanup()).isTrue();
        assertThat(folderA).doesNotExist();
        assertThat(folderA.mkdirs()).isTrue();
        assertThat(file.createNewFile()).isTrue();
        snapshotDirectory = SnapshotDirectory.permanent(folderAPath);
        snapshotDirectory.completeSnapshotAndGetHandle();
        assertThat(snapshotDirectory.cleanup()).isTrue();
        assertThat(folderA).isDirectory();
        assertThat(file).exists();
    }

    /**
     * This test checks that completing or deleting the snapshot influence the #isSnapshotOngoing()
     * flag.
     */
    @Test
    void isSnapshotOngoing() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
        assertThat(folderA.mkdirs()).isTrue();
        Path pathA = folderA.toPath();
        SnapshotDirectory snapshotDirectory = SnapshotDirectory.permanent(pathA);
        assertThat(snapshotDirectory.isSnapshotCompleted()).isFalse();
        assertThat(snapshotDirectory.completeSnapshotAndGetHandle()).isNotNull();
        assertThat(snapshotDirectory.isSnapshotCompleted()).isTrue();
        snapshotDirectory = SnapshotDirectory.permanent(pathA);
        assertThat(snapshotDirectory.isSnapshotCompleted()).isFalse();
        snapshotDirectory.cleanup();
        assertThat(snapshotDirectory.isSnapshotCompleted()).isFalse();
    }

    /** Tests that temporary directories have the right behavior on completion and deletion. */
    @Test
    void temporary() throws Exception {
        File folderRoot = temporaryFolder.toFile();
        File folder = new File(folderRoot, String.valueOf(UUID.randomUUID()));
        assertThat(folder.mkdirs()).isTrue();
        SnapshotDirectory tmpSnapshotDirectory = SnapshotDirectory.temporary(folder);
        // temporary snapshot directories should not return a handle, because they will be deleted.
        assertThat(tmpSnapshotDirectory.completeSnapshotAndGetHandle()).isNull();
        // check that the directory is deleted even after we called #completeSnapshotAndGetHandle.
        assertThat(tmpSnapshotDirectory.cleanup()).isTrue();
        assertThat(folder).doesNotExist();
    }
}
