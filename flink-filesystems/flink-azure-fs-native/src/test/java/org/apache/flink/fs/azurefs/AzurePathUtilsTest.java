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

import com.azure.storage.file.datalake.models.PathItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzurePathUtils}. */
class AzurePathUtilsTest {

    private static final URI FILESYSTEM_URI =
            URI.create("abfss://container@account.dfs.core.windows.net/");

    static Stream<Arguments> pathCases() {
        return Stream.of(
                Arguments.of(
                        "dir/file.txt",
                        "abfss://container@account.dfs.core.windows.net/dir/file.txt"),
                Arguments.of("file.txt", "abfss://container@account.dfs.core.windows.net/file.txt"),
                Arguments.of(
                        "a/b/c/deep.parquet",
                        "abfss://container@account.dfs.core.windows.net/a/b/c/deep.parquet"),
                Arguments.of(
                        "dir/file with spaces.txt",
                        "abfss://container@account.dfs.core.windows.net/dir/file with spaces.txt"),
                Arguments.of(
                        "special-chars/a=1/b=2/part-00000.parquet",
                        "abfss://container@account.dfs.core.windows.net/special-chars/a=1/b=2/part-00000.parquet"));
    }

    @ParameterizedTest
    @MethodSource("pathCases")
    void buildDataLakePathConstructsCorrectPath(final String itemName, final String expectedPath) {
        final PathItem pathItem = createPathItem(itemName);

        final Path result = AzurePathUtils.buildDataLakePath(FILESYSTEM_URI, pathItem);

        assertThat(result.toString()).isEqualTo(expectedPath);
    }

    @Test
    void buildDataLakePathHandlesLeadingSlash() {
        final PathItem pathItem = createPathItem("/already/absolute");

        final Path result = AzurePathUtils.buildDataLakePath(FILESYSTEM_URI, pathItem);

        assertThat(result.toString())
                .isEqualTo("abfss://container@account.dfs.core.windows.net/already/absolute");
    }

    @Test
    void buildDataLakePathHandlesEmptyName() {
        final PathItem pathItem = createPathItem("");

        final Path result = AzurePathUtils.buildDataLakePath(FILESYSTEM_URI, pathItem);

        assertThat(result.toString()).isEqualTo("abfss://container@account.dfs.core.windows.net/");
    }

    @Test
    void buildDataLakePathStripsLeadingAndTrailingSpaces() {
        final PathItem pathItem = createPathItem("  dir/file.txt  ");

        final Path result = AzurePathUtils.buildDataLakePath(FILESYSTEM_URI, pathItem);

        assertThat(result.toString())
                .isEqualTo("abfss://container@account.dfs.core.windows.net/dir/file.txt");
    }

    @Test
    void buildDataLakePathWorksWithoutAuthority() {
        final URI noAuthorityUri = URI.create("abfss:///");
        final PathItem pathItem = createPathItem("root-folder/checkpoints");

        final Path result = AzurePathUtils.buildDataLakePath(noAuthorityUri, pathItem);

        assertThat(result.toString()).isEqualTo("abfss:/root-folder/checkpoints");
    }

    @Test
    void buildDataLakePathThrowsOnNullUri() {
        final PathItem pathItem = createPathItem("file.txt");

        assertThatThrownBy(() -> AzurePathUtils.buildDataLakePath(null, pathItem))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void buildDataLakePathThrowsOnNullPathItem() {
        assertThatThrownBy(() -> AzurePathUtils.buildDataLakePath(FILESYSTEM_URI, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static PathItem createPathItem(final String name) {
        return new PathItem(
                (String) null, // eTag
                (OffsetDateTime) null, // lastModified
                0L, // contentLength
                (String) null, // group
                false, // isDirectory
                name,
                (String) null, // owner
                (String) null); // permissions
    }
}
