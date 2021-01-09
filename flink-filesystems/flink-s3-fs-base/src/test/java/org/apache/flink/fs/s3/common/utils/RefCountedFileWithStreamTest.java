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

package org.apache.flink.fs.s3.common.utils;

import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.stream.Stream;

/** Tests for the {@link RefCountedFileWithStream}. */
public class RefCountedFileWithStreamTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void writeShouldSucceed() throws IOException {
        byte[] content = bytesOf("hello world");

        final RefCountedFileWithStream fileUnderTest = getClosedRefCountedFileWithContent(content);
        long fileLength = fileUnderTest.getLength();

        Assertions.assertEquals(content.length, fileLength);
    }

    @Test
    public void closeShouldNotReleaseReference() throws IOException {
        getClosedRefCountedFileWithContent("hello world");
        verifyTheFileIsStillThere();
    }

    @Test
    public void writeAfterCloseShouldThrowException() throws IOException {
        assertThrows(
                IOException.class,
                () -> {
                    final RefCountedFileWithStream fileUnderTest =
                            getClosedRefCountedFileWithContent("hello world");
                    byte[] content = bytesOf("Hello Again");
                    fileUnderTest.write(content, 0, content.length);
                });
    }

    @Test
    public void flushAfterCloseShouldThrowException() throws IOException {
        assertThrows(
                IOException.class,
                () -> {
                    final RefCountedFileWithStream fileUnderTest =
                            getClosedRefCountedFileWithContent("hello world");
                    fileUnderTest.flush();
                });
    }

    // ------------------------------------- Utilities -------------------------------------

    private void verifyTheFileIsStillThere() throws IOException {
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            Assertions.assertEquals(1L, files.count());
        }
    }

    private RefCountedFileWithStream getClosedRefCountedFileWithContent(String content)
            throws IOException {
        return getClosedRefCountedFileWithContent(bytesOf(content));
    }

    private RefCountedFileWithStream getClosedRefCountedFileWithContent(byte[] content)
            throws IOException {
        final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
        final OutputStream out =
                Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

        final RefCountedFileWithStream fileUnderTest =
                RefCountedFileWithStream.newFile(newFile, out);

        fileUnderTest.write(content, 0, content.length);

        fileUnderTest.closeStream();
        return fileUnderTest;
    }

    private static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
