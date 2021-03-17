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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Tests verifying that the FsStateBackend passes the entropy injection option to the FileSystem for
 * state payload files, but not for metadata files.
 */
public class FsStateBackendEntropyTest {

    static final String ENTROPY_MARKER = "__ENTROPY__";
    static final String RESOLVED_MARKER = "+RESOLVED+";

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testEntropyInjection() throws Exception {
        final int fileSizeThreshold = 1024;
        final FileSystem fs = new TestEntropyAwareFs();

        final Path checkpointDir =
                new Path(Path.fromLocalFile(tmp.newFolder()), ENTROPY_MARKER + "/checkpoints");
        final String checkpointDirStr = checkpointDir.toString();

        final FsCheckpointStorageAccess storage =
                new FsCheckpointStorageAccess(
                        fs, checkpointDir, null, new JobID(), fileSizeThreshold, 4096);
        storage.initializeBaseLocations();

        final FsCheckpointStorageLocation location =
                (FsCheckpointStorageLocation) storage.initializeLocationForCheckpoint(96562);

        assertThat(location.getCheckpointDirectory().toString(), startsWith(checkpointDirStr));
        assertThat(location.getSharedStateDirectory().toString(), startsWith(checkpointDirStr));
        assertThat(location.getTaskOwnedStateDirectory().toString(), startsWith(checkpointDirStr));
        assertThat(location.getMetadataFilePath().toString(), not(containsString(ENTROPY_MARKER)));

        // check entropy in task-owned state
        try (CheckpointStateOutputStream stream = storage.createTaskOwnedStateStream()) {
            stream.write(new byte[fileSizeThreshold + 1], 0, fileSizeThreshold + 1);
            FileStateHandle handle = (FileStateHandle) stream.closeAndGetHandle();

            assertNotNull(handle);
            assertThat(handle.getFilePath().toString(), not(containsString(ENTROPY_MARKER)));
            assertThat(handle.getFilePath().toString(), containsString(RESOLVED_MARKER));
        }

        // check entropy in the exclusive/shared state
        try (CheckpointStateOutputStream stream =
                location.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)) {
            stream.write(new byte[fileSizeThreshold + 1], 0, fileSizeThreshold + 1);

            FileStateHandle handle = (FileStateHandle) stream.closeAndGetHandle();

            assertNotNull(handle);
            assertThat(handle.getFilePath().toString(), not(containsString(ENTROPY_MARKER)));
            assertThat(handle.getFilePath().toString(), containsString(RESOLVED_MARKER));
        }

        // check that there is no entropy in the metadata
        // check entropy in the exclusive/shared state
        try (CheckpointMetadataOutputStream stream = location.createMetadataOutputStream()) {
            stream.flush();
            FsCompletedCheckpointStorageLocation handle =
                    (FsCompletedCheckpointStorageLocation) stream.closeAndFinalizeCheckpoint();

            assertNotNull(handle);

            // metadata files have no entropy
            assertThat(
                    handle.getMetadataHandle().getFilePath().toString(),
                    not(containsString(ENTROPY_MARKER)));
            assertThat(
                    handle.getMetadataHandle().getFilePath().toString(),
                    not(containsString(RESOLVED_MARKER)));

            // external location is the same as metadata, without the file name
            assertEquals(
                    handle.getMetadataHandle().getFilePath().getParent().toString(),
                    handle.getExternalPointer());
        }
    }

    static class TestEntropyAwareFs extends LocalFileSystem implements EntropyInjectingFileSystem {

        @Override
        public String getEntropyInjectionKey() {
            return ENTROPY_MARKER;
        }

        @Override
        public String generateEntropy() {
            return RESOLVED_MARKER;
        }
    }
}
