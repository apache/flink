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

package org.apache.flink.runtime.rest.handler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileUploads}. */
class FileUploadsTest {

    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testRelativePathRejection() {
        Path relative = Paths.get("root");
        assertThatThrownBy(() -> new FileUploads(relative))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDirectoryScan() throws IOException {
        Path rootDir = Paths.get("root");
        Path rootFile = rootDir.resolve("rootFile");
        Path subDir = rootDir.resolve("sub");
        Path subFile = subDir.resolve("subFile");

        Path tmp = temporaryFolder;
        Files.createDirectory(tmp.resolve(rootDir));
        Files.createDirectory(tmp.resolve(subDir));
        Files.createFile(tmp.resolve(rootFile));
        Files.createFile(tmp.resolve(subFile));

        try (FileUploads fileUploads = new FileUploads(tmp.resolve(rootDir))) {
            Collection<Path> detectedFiles =
                    fileUploads.getUploadedFiles().stream()
                            .map(File::toPath)
                            .collect(Collectors.toList());

            assertThat(detectedFiles).hasSize(2);
            assertThat(detectedFiles).contains(tmp.resolve(rootFile));
            assertThat(detectedFiles).contains(tmp.resolve(subFile));
        }
    }

    @Test
    void testEmptyDirectory() throws IOException {
        Path rootDir = Paths.get("root");

        Path tmp = temporaryFolder;
        Files.createDirectory(tmp.resolve(rootDir));

        try (FileUploads fileUploads = new FileUploads(tmp.resolve(rootDir))) {
            Collection<File> detectedFiles = fileUploads.getUploadedFiles();
            assertThat(detectedFiles).isEmpty();
        }
    }

    @Test
    void testCleanup() throws IOException {
        Path rootDir = Paths.get("root");
        Path rootFile = rootDir.resolve("rootFile");
        Path subDir = rootDir.resolve("sub");
        Path subFile = subDir.resolve("subFile");

        Path tmp = temporaryFolder;
        Files.createDirectory(tmp.resolve(rootDir));
        Files.createDirectory(tmp.resolve(subDir));
        Files.createFile(tmp.resolve(rootFile));
        Files.createFile(tmp.resolve(subFile));

        try (FileUploads fileUploads = new FileUploads(tmp.resolve(rootDir))) {
            assertThat(Files.exists(tmp.resolve(rootDir))).isTrue();
            assertThat(Files.exists(tmp.resolve(subDir))).isTrue();
            assertThat(Files.exists(tmp.resolve(rootFile))).isTrue();
            assertThat(Files.exists(tmp.resolve(subFile))).isTrue();
        }
        assertThat(Files.exists(tmp.resolve(rootDir))).isFalse();
        assertThat(Files.exists(tmp.resolve(subDir))).isFalse();
        assertThat(Files.exists(tmp.resolve(rootFile))).isFalse();
        assertThat(Files.exists(tmp.resolve(subFile))).isFalse();
    }
}
