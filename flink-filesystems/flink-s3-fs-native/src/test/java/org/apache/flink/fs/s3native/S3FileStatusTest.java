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

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link S3FileStatus}. */
class S3FileStatusTest {

    private static final Path PATH = new Path("s3://bucket/key");

    static Stream<Arguments> fileSizeAndModTime() {
        return Stream.of(
                Arguments.of(0L, 0L),
                Arguments.of(1024L, 987654321L),
                Arguments.of(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("fileSizeAndModTime")
    void withFile_variousSizes_allFieldsSetCorrectly(long size, long modTime) {
        S3FileStatus s = S3FileStatus.withFile(size, modTime, PATH);

        assertThat(s.getLen()).isEqualTo(size);
        assertThat(s.getBlockSize()).isEqualTo(size);
        assertThat(s.getModificationTime()).isEqualTo(modTime);
        assertThat(s.getAccessTime()).isEqualTo(0L);
        assertThat(s.isDir()).isFalse();
        assertThat(s.getPath()).isEqualTo(PATH);
        assertThat(s.getReplication()).isEqualTo((short) 1);
    }

    @Test
    void withDirectory_anyPath_allFieldsSetCorrectly() {
        S3FileStatus s = S3FileStatus.withDirectory(PATH);

        assertThat(s.getLen()).isEqualTo(0L);
        assertThat(s.getBlockSize()).isEqualTo(0L);
        assertThat(s.getModificationTime()).isEqualTo(0L);
        assertThat(s.getAccessTime()).isEqualTo(0L);
        assertThat(s.isDir()).isTrue();
        assertThat(s.getPath()).isEqualTo(PATH);
        assertThat(s.getReplication()).isEqualTo((short) 1);
    }

    @Test
    void constructor_nonZeroAccessTime_accessTimeIsPreserved() {
        S3FileStatus s = new S3FileStatus(1024L, 1024L, 100L, 999L, false, PATH);

        assertThat(s.getAccessTime()).isEqualTo(999L);
    }
}
