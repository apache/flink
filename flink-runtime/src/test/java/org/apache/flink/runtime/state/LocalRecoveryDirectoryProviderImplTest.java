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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LocalRecoveryDirectoryProvider}. */
class LocalRecoveryDirectoryProviderImplTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;

    @TempDir private java.nio.file.Path tmpFolder;

    private LocalRecoveryDirectoryProviderImpl directoryProvider;
    private File[] allocBaseFolders;

    @BeforeEach
    void setup() throws IOException {
        this.allocBaseFolders =
                new File[] {
                    TempDirUtils.newFolder(tmpFolder),
                    TempDirUtils.newFolder(tmpFolder),
                    TempDirUtils.newFolder(tmpFolder)
                };
        this.directoryProvider =
                new LocalRecoveryDirectoryProviderImpl(
                        allocBaseFolders, JOB_ID, JOB_VERTEX_ID, SUBTASK_INDEX);
    }

    @Test
    void allocationBaseDir() {
        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.allocationBaseDirectory(i))
                    .isEqualTo(allocBaseFolders[i % allocBaseFolders.length]);
        }
    }

    @Test
    void selectAllocationBaseDir() {
        for (int i = 0; i < allocBaseFolders.length; ++i) {
            assertThat(directoryProvider.selectAllocationBaseDirectory(i))
                    .isEqualTo(allocBaseFolders[i]);
        }
    }

    @Test
    void allocationBaseDirectoriesCount() {
        assertThat(directoryProvider.allocationBaseDirsCount()).isEqualTo(allocBaseFolders.length);
    }

    @Test
    void subtaskSpecificDirectory() {
        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.subtaskBaseDirectory(i))
                    .isEqualTo(
                            new File(
                                    directoryProvider.allocationBaseDirectory(i),
                                    directoryProvider.subtaskDirString()));
        }
    }

    @Test
    void subtaskCheckpointSpecificDirectory() {
        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.subtaskSpecificCheckpointDirectory(i))
                    .isEqualTo(
                            new File(
                                    directoryProvider.subtaskBaseDirectory(i),
                                    directoryProvider.checkpointDirString(i)));
        }
    }

    @Test
    void testPathStringConstants() {

        assertThat(directoryProvider.subtaskDirString())
                .isEqualTo(
                        "jid_"
                                + JOB_ID
                                + Path.SEPARATOR
                                + "vtx_"
                                + JOB_VERTEX_ID
                                + "_sti_"
                                + SUBTASK_INDEX);

        final long checkpointId = 42;
        assertThat(directoryProvider.checkpointDirString(checkpointId))
                .isEqualTo("chk_" + checkpointId);
    }

    @Test
    void testPreconditionsNotNullFiles() {
        assertThatThrownBy(
                        () ->
                                new LocalRecoveryDirectoryProviderImpl(
                                        new File[] {null}, JOB_ID, JOB_VERTEX_ID, SUBTASK_INDEX))
                .isInstanceOf(NullPointerException.class);
    }
}
