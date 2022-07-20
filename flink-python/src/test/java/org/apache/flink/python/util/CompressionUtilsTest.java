/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.util;

import org.apache.flink.configuration.ConfigConstants;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CompressionUtils}. */
class CompressionUtilsTest {

    private static final int S_IFLNK = 40960;

    @Test
    void testSymlink(@TempDir Path zipPath, @TempDir Path target) throws IOException {
        File zipFile = Files.createTempFile(zipPath, null, null).toFile();

        try (ZipArchiveOutputStream zipOut =
                new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
            String file1 = "zipFile1";
            ZipArchiveEntry entry = new ZipArchiveEntry(file1);
            entry.setUnixMode(0644);
            zipOut.putArchiveEntry(entry);
            zipOut.write(new byte[] {1, 1, 1, 1, 1});
            zipOut.closeArchiveEntry();

            String file2 = "zipFile2";
            entry = new ZipArchiveEntry(file2);
            entry.setUnixMode(0644);
            zipOut.putArchiveEntry(entry);
            zipOut.write(new byte[] {2, 2, 2, 2, 2});
            zipOut.closeArchiveEntry();

            entry = new ZipArchiveEntry("softlink");
            entry.setUnixMode(S_IFLNK | 0644);
            zipOut.putArchiveEntry(entry);
            zipOut.write(file1.getBytes(ConfigConstants.DEFAULT_CHARSET));
            zipOut.closeArchiveEntry();
        }

        String targetPath = target.toString();
        CompressionUtils.extractZipFileWithPermissions(zipFile.getCanonicalPath(), targetPath);
        Path softLink = new File(targetPath, "softlink").toPath();
        assertThat(Files.isSymbolicLink(softLink)).isTrue();
        assertThat(Files.readSymbolicLink(softLink).toString()).endsWith("zipFile1");

        Path file1Path = new File(targetPath, "zipFile1").toPath();
        assertThat(Files.isRegularFile(file1Path)).isTrue();
        assertThat(Files.readAllBytes(file1Path)).isEqualTo(new byte[] {1, 1, 1, 1, 1});

        Path file2Path = new File(targetPath, "zipFile2").toPath();
        assertThat(Files.isRegularFile(file2Path)).isTrue();
        assertThat(Files.readAllBytes(file2Path)).isEqualTo(new byte[] {2, 2, 2, 2, 2});
    }

    @Test
    void testSymlinkWithoutTargetFile(@TempDir Path zipPath, @TempDir Path target)
            throws IOException {
        File zipFile = Files.createTempFile(zipPath, null, null).toFile();

        try (ZipArchiveOutputStream zipOut =
                new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
            ZipArchiveEntry entry = new ZipArchiveEntry("softlink");
            entry.setUnixMode(S_IFLNK | 0644);
            zipOut.putArchiveEntry(entry);
            zipOut.write("targetFile".getBytes(ConfigConstants.DEFAULT_CHARSET));
            zipOut.closeArchiveEntry();
        }

        String targetPath = target.toString();
        CompressionUtils.extractZipFileWithPermissions(zipFile.getCanonicalPath(), targetPath);
        Path softLink = new File(targetPath, "softlink").toPath();
        assertThat(Files.isSymbolicLink(softLink)).isTrue();
        assertThat(Files.readSymbolicLink(softLink).toString()).endsWith("targetFile");
    }

    @Test
    void testExpandOutOfTargetDir(@TempDir Path zipPath, @TempDir Path target) throws IOException {
        File zipFile = Files.createTempFile(zipPath, null, null).toFile();

        try (ZipArchiveOutputStream zipOut =
                new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
            String file1 = "../zipFile";
            ZipArchiveEntry entry = new ZipArchiveEntry(file1);
            zipOut.putArchiveEntry(entry);
            zipOut.write(new byte[] {1, 1, 1, 1, 1});
            zipOut.closeArchiveEntry();
        }

        String targetPath = target.toString();
        assertThatThrownBy(
                        () ->
                                CompressionUtils.extractZipFileWithPermissions(
                                        zipFile.getCanonicalPath(), targetPath))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Expand ../zipFile would create a file outside of");
    }

    @Test
    void testPermissionRestored(@TempDir Path zipPath, @TempDir Path target) throws IOException {
        File zipFile = Files.createTempFile(zipPath, null, null).toFile();

        try (ZipArchiveOutputStream zipOut =
                new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
            String file1 = "zipFile";
            ZipArchiveEntry entry = new ZipArchiveEntry(file1);
            entry.setUnixMode(0355);
            zipOut.putArchiveEntry(entry);
            zipOut.write(new byte[] {1, 1, 1, 1, 1});
            zipOut.closeArchiveEntry();
        }

        String targetPath = target.toString();
        CompressionUtils.extractZipFileWithPermissions(zipFile.getCanonicalPath(), targetPath);

        Path path = new File(targetPath, "zipFile").toPath();
        assertThat(toUnixMode(Files.getPosixFilePermissions(path))).isEqualTo(0355);
    }

    private int toUnixMode(Set<PosixFilePermission> permission) {
        int mode = 0;
        for (PosixFilePermission action : PosixFilePermission.values()) {
            mode = mode << 1;
            mode += permission.contains(action) ? 1 : 0;
        }
        return mode;
    }
}
