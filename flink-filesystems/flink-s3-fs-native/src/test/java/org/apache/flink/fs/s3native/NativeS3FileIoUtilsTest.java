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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeS3FileIoUtils}. */
class NativeS3FileIoUtilsTest {

    @ParameterizedTest
    @CsvSource({"1", "7", "1024", "4096"})
    void testCopyStreamPreservesContentAcrossBufferSizes(int bufferSize, @TempDir Path tempDir)
            throws Exception {
        byte[] data = new byte[3000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        Path dest = tempDir.resolve("out-" + bufferSize + ".bin");

        NativeS3FileIoUtils.copyStream(new ByteArrayInputStream(data), dest, bufferSize);

        assertThat(Files.readAllBytes(dest)).isEqualTo(data);
    }

    @Test
    void testCopyStreamOverwritesExistingFile(@TempDir Path tempDir) throws Exception {
        Path dest = tempDir.resolve("out.bin");
        Files.write(dest, new byte[] {9, 9, 9, 9, 9});
        byte[] data = {1, 2, 3};

        NativeS3FileIoUtils.copyStream(new ByteArrayInputStream(data), dest, 256 * 1024);

        assertThat(Files.readAllBytes(dest)).isEqualTo(data);
    }

    @Test
    void testCopyStreamEmptySource(@TempDir Path tempDir) throws Exception {
        Path dest = tempDir.resolve("empty.bin");

        NativeS3FileIoUtils.copyStream(new ByteArrayInputStream(new byte[0]), dest, 1024);

        assertThat(Files.readAllBytes(dest)).isEmpty();
    }

    @Test
    void testCreateTemporaryDownloadFilePadsShortNames(@TempDir Path tempDir) throws Exception {
        Path shortName = tempDir.resolve("x");
        Path temp = NativeS3FileIoUtils.createTemporaryDownloadFile(tempDir, shortName);
        assertThat(temp).exists();
        assertThat(temp.getFileName().toString()).endsWith(".tmp");
    }

    @Test
    void testMoveFileOverwritesDestination(@TempDir Path tempDir) throws Exception {
        Path source = tempDir.resolve("src.bin");
        Path dest = tempDir.resolve("dst.bin");
        Files.write(source, new byte[] {1, 2, 3});
        Files.write(dest, new byte[] {9});

        NativeS3FileIoUtils.moveFile(source, dest);

        assertThat(Files.readAllBytes(dest)).containsExactly(1, 2, 3);
        assertThat(source).doesNotExist();
    }
}
