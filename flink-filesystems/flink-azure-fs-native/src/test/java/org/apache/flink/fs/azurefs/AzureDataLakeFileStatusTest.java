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

package org.apache.flink.fs.azurefs;

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureDataLakeFileStatus}. */
class AzureDataLakeFileStatusTest {

    private static final Path TEST_DIR_PATH =
            new Path("abfss://container@account.dfs.core.windows.net/dir");
    private static final Path TEST_FILE_PATH =
            new Path("abfss://container@account.dfs.core.windows.net/dir/file.txt");
    private static final Path TEST_ROOT_FILE_PATH =
            new Path("abfss://container@account.dfs.core.windows.net/root.txt");

    @Test
    void forDirectorySetsExpectedValues() {
        final AzureDataLakeFileStatus status =
                AzureDataLakeFileStatus.forDirectory(TEST_DIR_PATH, 1700000000000L);

        assertThat(status.isDir()).isTrue();
        assertThat(status.getPath()).isEqualTo(TEST_DIR_PATH);
        assertThat(status.getLen()).isZero();
        assertThat(status.getBlockSize()).isZero();
        assertThat(status.getModificationTime()).isEqualTo(1700000000000L);
        assertThat(status.getAccessTime()).isZero();
    }

    static Stream<Arguments> fileCases() {
        return Stream.of(
                Arguments.of(TEST_FILE_PATH, 1024L, 256L, 1700000000000L),
                Arguments.of(TEST_ROOT_FILE_PATH, 0L, 0L, 0L));
    }

    @ParameterizedTest
    @MethodSource("fileCases")
    void forFileSetsExpectedValues(
            final Path path, final long size, final long blockSize, final long modTime) {
        final AzureDataLakeFileStatus status =
                AzureDataLakeFileStatus.forFile(path, size, blockSize, modTime);

        assertThat(status.isDir()).isFalse();
        assertThat(status.getPath()).isEqualTo(path);
        assertThat(status.getLen()).isEqualTo(size);
        assertThat(status.getBlockSize()).isEqualTo(blockSize);
        assertThat(status.getModificationTime()).isEqualTo(modTime);
        assertThat(status.getAccessTime()).isZero();
    }

    @Test
    void forDirectoryWithNullPathThrows() {
        assertThatThrownBy(() -> AzureDataLakeFileStatus.forDirectory(null, 0L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void forFileWithNullPathThrows() {
        assertThatThrownBy(() -> AzureDataLakeFileStatus.forFile(null, 0L, 0L, 0L))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @MethodSource("negativeValueCases")
    void shouldRejectNegativeValues(final Runnable action) {
        assertThatThrownBy(action::run).isInstanceOf(IllegalArgumentException.class);
    }

    private static Stream<Arguments> negativeValueCases() {
        return Stream.of(
                // negative file size
                Arguments.of(
                        (Runnable)
                                () -> AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, -1L, 0L, 0L)),
                // negative block size
                Arguments.of(
                        (Runnable)
                                () -> AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 0L, -1L, 0L)),
                // negative file modification time
                Arguments.of(
                        (Runnable)
                                () -> AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 0L, 0L, -1L)),
                // negative directory modification time
                Arguments.of(
                        (Runnable) () -> AzureDataLakeFileStatus.forDirectory(TEST_DIR_PATH, -1L)));
    }

    @Test
    void replicationAlwaysReturnsOne() {
        final AzureDataLakeFileStatus dir = AzureDataLakeFileStatus.forDirectory(TEST_DIR_PATH, 0L);
        final AzureDataLakeFileStatus file =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 100L, 64L, 0L);

        assertThat(dir.getReplication()).isEqualTo((short) 1);
        assertThat(file.getReplication()).isEqualTo((short) 1);
    }

    @Test
    void toStringContainsAllFields() {
        final AzureDataLakeFileStatus status =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 1024L, 256L, 99L);

        assertThat(status.toString())
                .contains("path=" + TEST_FILE_PATH)
                .contains("length=1024")
                .contains("blockSize=256")
                .contains("isDir=false")
                .contains("modTime=99")
                .contains("accessTime=0");
    }

    @Test
    void equalObjectsHaveEqualHashCodes() {
        final AzureDataLakeFileStatus a =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 1024L, 256L, 100L);
        final AzureDataLakeFileStatus b =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 1024L, 256L, 100L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentLengthIsNotEqual() {
        final AzureDataLakeFileStatus a =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 1024L, 256L, 100L);
        final AzureDataLakeFileStatus b =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 2048L, 256L, 100L);

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void fileAndDirectoryAreNotEqual() {
        final AzureDataLakeFileStatus file =
                AzureDataLakeFileStatus.forFile(TEST_DIR_PATH, 0L, 0L, 100L);
        final AzureDataLakeFileStatus dir =
                AzureDataLakeFileStatus.forDirectory(TEST_DIR_PATH, 100L);

        assertThat(file).isNotEqualTo(dir);
    }

    @Test
    void notEqualToNullOrDifferentType() {
        final AzureDataLakeFileStatus status =
                AzureDataLakeFileStatus.forFile(TEST_FILE_PATH, 1024L, 256L, 100L);

        assertThat(status).isNotEqualTo(null);
        assertThat(status).isNotEqualTo("not a file status");
    }
}
