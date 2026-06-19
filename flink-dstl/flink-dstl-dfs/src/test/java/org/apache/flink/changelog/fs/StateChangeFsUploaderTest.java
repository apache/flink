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

package org.apache.flink.changelog.fs;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.fs.local.LocalFileSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link StateChangeFsUploader} test. */
class StateChangeFsUploaderTest {

    @TempDir java.nio.file.Path tempFolder;

    @Test
    void testBasePath() throws IOException {
        JobID jobID = JobID.generate();
        String rootPath = "/dstl-root-path";
        Path oriBasePath = new Path(rootPath);

        ChangelogStorageMetricGroup metrics =
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup());

        StateChangeFsUploader uploader =
                new StateChangeFsUploader(
                        jobID,
                        oriBasePath,
                        oriBasePath.getFileSystem(),
                        false,
                        4096,
                        metrics,
                        TaskChangelogRegistry.NO_OP);

        assertThat(uploader.getBasePath().getPath())
                .isEqualTo(
                        String.format(
                                "%s/%s/%s",
                                rootPath, jobID.toHexString(), StateChangeFsUploader.PATH_SUB_DIR));
    }

    @Test
    void testCompression() throws IOException {
        AtomicBoolean outputStreamClosed = new AtomicBoolean(false);

        FileSystem wrappedFileSystem =
                new LocalFileSystem() {
                    @Override
                    public FSDataOutputStream create(Path filePath, WriteMode overwrite)
                            throws IOException {
                        checkNotNull(filePath, "filePath");

                        if (exists(filePath) && overwrite == WriteMode.NO_OVERWRITE) {
                            throw new FileAlreadyExistsException(
                                    "File already exists: " + filePath);
                        }

                        final Path parent = filePath.getParent();
                        if (parent != null && !mkdirs(parent)) {
                            throw new IOException("Mkdirs failed to create " + parent);
                        }

                        final File file = pathToFile(filePath);
                        return new LocalDataOutputStream(file) {
                            @Override
                            public void close() throws IOException {
                                super.close();
                                outputStreamClosed.set(true);
                            }
                        };
                    }
                };

        StateChangeFsUploader uploader = createUploader(wrappedFileSystem, false);
        uploader.upload(Collections.emptyList());
        assertThat(outputStreamClosed.get()).isTrue();

        outputStreamClosed.set(false);
        uploader = createUploader(wrappedFileSystem, true);
        uploader.upload(Collections.emptyList());
        assertThat(outputStreamClosed.get()).isTrue();
    }

    private StateChangeFsUploader createUploader(FileSystem fileSystem, boolean compression) {
        return new StateChangeFsUploader(
                JobID.generate(),
                new Path(tempFolder.toUri()),
                fileSystem,
                compression,
                4096,
                new ChangelogStorageMetricGroup(createUnregisteredTaskManagerJobMetricGroup()),
                TaskChangelogRegistry.NO_OP);
    }
}
