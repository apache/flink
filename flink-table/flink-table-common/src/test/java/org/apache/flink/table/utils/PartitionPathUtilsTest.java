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

package org.apache.flink.table.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.table.utils.PartitionPathUtils}. */
class PartitionPathUtilsTest {

    @TempDir
    Path tmpDir;

    @Test
    void testEscapeChar() {
        for (char c = 0; c <= 128; c++) {
            String expected = "%" + String.format("%1$02X", (int) c);
            String actual = PartitionPathUtils.escapeChar(c, new StringBuilder()).toString();
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void testEscapePathNameWithHeadControl() {
        String origin = "[00";
        String expected = "%5B00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithTailControl() {
        String origin = "00]";
        String expected = "00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithMidControl() {
        String origin = "00:00";
        String expected = "00%3A00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathName() {
        String origin = "[00:00]";
        String expected = "%5B00%3A00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithoutControl() {
        String origin = "0000";
        String expected = "0000";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithCurlyBraces() {
        String origin = "{partName}";
        String expected = "%7BpartName%7D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    /**
     * FLINK-38774: {@link PartitionPathUtils#searchPartSpecAndPaths} must not descend into hidden
     * directories. A hidden dir such as {@code _temporary} may contain non-hidden children (e.g.
     * {@code job-123}) at partition depth; without skipping hidden dirs those children leak into
     * the result with an empty partition spec, which later surfaces as a {@code TableException:
     * incomplete partition spec}.
     */
    @Test
    void testSearchPartSpecAndPathsSkipsHiddenDirectories() throws IOException {
        org.apache.flink.core.fs.Path basePath =
                new org.apache.flink.core.fs.Path(tmpDir.toString(), "country_page_view");
        FileSystem fs = basePath.getFileSystem();

        // Real partition: date=2019-8-30/country=China
        Files.createDirectories(
                Path.of(
                        tmpDir.toString(),
                        "country_page_view",
                        "date=2019-8-30",
                        "country=China"));
        // Hidden _temporary dir whose non-hidden child sits at partition depth (2).
        Files.createDirectories(
                Path.of(tmpDir.toString(), "country_page_view", "_temporary", "job-123"));

        List<Tuple2<LinkedHashMap<String, String>, org.apache.flink.core.fs.Path>> parts =
                PartitionPathUtils.searchPartSpecAndPaths(fs, basePath, 2);

        // Only the real partition is returned; the _temporary subtree is skipped entirely.
        assertThat(parts).hasSize(1);
        assertThat(parts.get(0).f0)
                .containsEntry("date", "2019-8-30")
                .containsEntry("country", "China");
    }
}
