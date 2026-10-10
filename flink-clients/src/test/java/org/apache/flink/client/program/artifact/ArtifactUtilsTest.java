/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program.artifact;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ArtifactUtils}. */
class ArtifactUtilsTest {

    @Test
    void testCreateMissingParents(@TempDir Path tempDir) {
        File targetDir = tempDir.resolve("p1").resolve("p2").resolve("base-dir").toFile();
        assertThat(targetDir.getParentFile().getParentFile()).doesNotExist();

        ArtifactUtils.createMissingParents(targetDir);
        assertThat(targetDir.getParentFile()).isDirectory();
    }

    private static final byte[] ARTIFACT = "not a real jar".getBytes(StandardCharsets.UTF_8);

    @Test
    void testACompleteCopyIsPutUnderTheTargetName(@TempDir Path tempDir) throws Exception {
        final File targetFile = tempDir.resolve("missing").resolve("job.jar").toFile();

        ArtifactUtils.copyToFileWhenComplete(new ByteArrayInputStream(ARTIFACT), targetFile);

        assertThat(targetFile).hasBinaryContent(ARTIFACT);
        assertThat(targetFile.getParentFile().list())
                .as("nothing else is left behind")
                .containsExactly("job.jar");
    }

    @Test
    void testTheTargetNameIsUnusedWhileTheCopyIsUnderWay(@TempDir Path tempDir) throws Exception {
        final File targetFile = tempDir.resolve("job.jar").toFile();
        final InputStream checkingStream =
                new ByteArrayInputStream(ARTIFACT) {
                    @Override
                    public synchronized int read(byte[] b, int off, int len) {
                        assertThat(targetFile)
                                .as("a process ending now must not leave a partial artifact")
                                .doesNotExist();
                        return super.read(b, off, len);
                    }
                };

        ArtifactUtils.copyToFileWhenComplete(checkingStream, targetFile);

        assertThat(targetFile).hasBinaryContent(ARTIFACT);
    }

    @Test
    void testNothingIsLeftWhenTheCopyFails(@TempDir Path tempDir) {
        final File targetFile = tempDir.resolve("job.jar").toFile();
        final IOException failure = new IOException("connection reset");

        assertThatThrownBy(
                        () ->
                                ArtifactUtils.copyToFileWhenComplete(
                                        failingAfterFirstRead(failure), targetFile))
                .isSameAs(failure);

        assertThat(tempDir.toFile().list()).isEmpty();
    }

    @Test
    void testNothingIsLeftWhenTheCopyFailsWithAnUncheckedException(@TempDir Path tempDir) {
        final File targetFile = tempDir.resolve("job.jar").toFile();
        final UncheckedIOException failure =
                new UncheckedIOException(new IOException("plugin failure"));

        assertThatThrownBy(
                        () ->
                                ArtifactUtils.copyToFileWhenComplete(
                                        failingAfterFirstRead(failure), targetFile))
                .isSameAs(failure);

        assertThat(tempDir.toFile().list()).isEmpty();
    }

    /** Returns the artifact from its first read, then fails with the given exception. */
    private static InputStream failingAfterFirstRead(Exception failure) {
        return new InputStream() {
            private boolean read;

            @Override
            public int read() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (!read) {
                    read = true;
                    final int count = Math.min(len, ARTIFACT.length);
                    System.arraycopy(ARTIFACT, 0, b, off, count);
                    return count;
                }
                if (failure instanceof IOException) {
                    throw (IOException) failure;
                }
                throw (RuntimeException) failure;
            }
        };
    }
}
