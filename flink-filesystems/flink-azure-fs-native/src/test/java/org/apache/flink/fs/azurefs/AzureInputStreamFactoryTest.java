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

import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureInputStreamFactory}. */
class AzureInputStreamFactoryTest {

    // --- buildInputStreamOptions ---

    private static final int BLOCK_SIZE = AzureDataLakeFileSystem.SDK_READ_CHUNK_SIZE;

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L})
    void buildInputStreamOptionsShouldNotSetRangeForNonPositivePosition(final long position) {
        final DataLakeFileInputStreamOptions options =
                AzureInputStreamFactory.buildInputStreamOptions(position, BLOCK_SIZE);
        assertThat(options.getRange()).isNull();
        assertThat(options.getBlockSize()).isEqualTo(BLOCK_SIZE);
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 42L, Long.MAX_VALUE})
    void buildInputStreamOptionsShouldSetRangeForPositivePosition(final long position) {
        final DataLakeFileInputStreamOptions options =
                AzureInputStreamFactory.buildInputStreamOptions(position, BLOCK_SIZE);
        final FileRange range = options.getRange();
        assertThat(range).isNotNull();
        assertThat(range.getOffset()).isEqualTo(position);
        assertThat(options.getBlockSize()).isEqualTo(BLOCK_SIZE);
    }

    @Test
    void buildInputStreamOptionsShouldUseCustomBlockSize() {
        final int customBlockSize = 8 * 1024 * 1024;
        final DataLakeFileInputStreamOptions options =
                AzureInputStreamFactory.buildInputStreamOptions(0L, customBlockSize);
        assertThat(options.getBlockSize()).isEqualTo(customBlockSize);
    }

    // --- createOpener null checks ---

    @Test
    void shouldThrowOnNullClient() {
        assertThatThrownBy(
                        () ->
                                AzureInputStreamFactory.createOpener(
                                        null, "path/to/file", BLOCK_SIZE))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("fsClient");
    }

    @Test
    void shouldThrowOnNullFilePath() {
        assertThatThrownBy(
                        () ->
                                AzureInputStreamFactory.createOpener(
                                        new TestingDataLakeStorageOperations(), null, BLOCK_SIZE))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("filePath");
    }
}
