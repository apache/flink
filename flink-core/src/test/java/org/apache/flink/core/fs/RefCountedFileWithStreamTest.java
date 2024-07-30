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

package org.apache.flink.core.fs;

import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/** Tests for the {@link RefCountedFileWithStream}. */
class RefCountedFileWithStreamTest {

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    void writeShouldSucceed() throws IOException {
        byte[] content = bytesOf("hello world");

        final File newFile =
                new File(
                        TempDirUtils.newFolder(tempFolder).toPath().toFile(),
                        ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        final RefCountedFileWithStream fileUnderTest1 =
                RefCountedFileWithStream.newFile(newFile, out);

        fileUnderTest1.write(content, 0, content.length);

        fileUnderTest1.closeStream();

        assertThat(fileUnderTest1.getLength()).isEqualTo(content.length);
    }

    @Test
    void closeShouldNotReleaseReference() throws IOException {
        Path path = TempDirUtils.newFolder(tempFolder).toPath();
        getClosedRefCountedFileWithContent("hello world", path);
        try (Stream<Path> files = Files.list(path)) {
            assertThat(files).hasSize(1);
        }
    }

    @Test
    void writeAfterCloseShouldThrowException() {
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(
                        () -> {
                            final RefCountedFileWithStream fileUnderTest =
                                    getClosedRefCountedFileWithContent(
                                            "hello world",
                                            TempDirUtils.newFolder(tempFolder).toPath());
                            byte[] content = bytesOf("Hello Again");
                            fileUnderTest.write(content, 0, content.length);
                        });
    }

    @Test
    void flushAfterCloseShouldThrowException() {
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(
                        () -> {
                            final RefCountedFileWithStream fileUnderTest =
                                    getClosedRefCountedFileWithContent(
                                            "hello world",
                                            TempDirUtils.newFolder(tempFolder).toPath());
                            fileUnderTest.flush();
                        });
    }

    // ------------------------------------- Utilities -------------------------------------

    private RefCountedFileWithStream getClosedRefCountedFileWithContent(
            String content, Path tempFolder) throws IOException {

        byte[] content1 = bytesOf(content);
        final File newFile = new File(tempFolder.toFile(), ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        final RefCountedFileWithStream fileUnderTest =
                RefCountedFileWithStream.newFile(newFile, out);

        fileUnderTest.write(content1, 0, content1.length);

        fileUnderTest.closeStream();
        return fileUnderTest;
    }

    private static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
