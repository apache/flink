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

import org.apache.flink.core.fs.FileSystem.WriteMode;

import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureOutputStreamFactory}. */
class AzureOutputStreamFactoryTest {

    private static final long WRITE_REQUEST_SIZE = 8 * 1024 * 1024L;

    // --- buildOutputStreamOptions: content type ---

    @ParameterizedTest
    @EnumSource(WriteMode.class)
    void shouldSetContentTypeToOctetStream(final WriteMode writeMode) {
        final DataLakeFileOutputStreamOptions options =
                AzureOutputStreamFactory.buildOutputStreamOptions(
                        WRITE_REQUEST_SIZE, writeMode, Collections.emptyMap());

        assertThat(options.getHeaders().getContentType()).isEqualTo("application/octet-stream");
    }

    // --- buildOutputStreamOptions: block size and single-upload threshold ---

    @ParameterizedTest
    @EnumSource(WriteMode.class)
    void shouldSetTransferOptionsToWriteRequestSize(final WriteMode writeMode) {
        final DataLakeFileOutputStreamOptions options =
                AzureOutputStreamFactory.buildOutputStreamOptions(
                        WRITE_REQUEST_SIZE, writeMode, Collections.emptyMap());

        assertThat(options.getParallelTransferOptions().getBlockSizeLong())
                .isEqualTo(WRITE_REQUEST_SIZE);
        assertThat(options.getParallelTransferOptions().getMaxSingleUploadSizeLong())
                .isEqualTo(WRITE_REQUEST_SIZE);
    }

    // --- buildOutputStreamOptions: request conditions ---

    @ParameterizedTest
    @MethodSource("requestConditionsCases")
    void shouldSetRequestConditionsBasedOnWriteMode(
            final WriteMode writeMode, final String expectedIfNoneMatch) {
        final DataLakeFileOutputStreamOptions options =
                AzureOutputStreamFactory.buildOutputStreamOptions(
                        WRITE_REQUEST_SIZE, writeMode, Collections.emptyMap());

        final DataLakeRequestConditions conditions = options.getRequestConditions();
        if (expectedIfNoneMatch == null) {
            assertThat(conditions).isNull();
        } else {
            assertThat(conditions).isNotNull();
            assertThat(conditions.getIfNoneMatch()).isEqualTo(expectedIfNoneMatch);
        }
    }

    private static Stream<Arguments> requestConditionsCases() {
        return Stream.of(
                Arguments.of(WriteMode.OVERWRITE, null), Arguments.of(WriteMode.NO_OVERWRITE, "*"));
    }

    // --- buildOutputStreamOptions: metadata ---

    @Test
    void shouldNotSetMetadataWhenEmpty() {
        final DataLakeFileOutputStreamOptions options =
                AzureOutputStreamFactory.buildOutputStreamOptions(
                        WRITE_REQUEST_SIZE, WriteMode.OVERWRITE, Collections.emptyMap());

        assertThat(options.getMetadata()).isNull();
    }

    @Test
    void shouldAttachMetadataWhenPresent() {
        final Map<String, String> metadata = Map.of("x_cse_key_id", "key-123");
        final DataLakeFileOutputStreamOptions options =
                AzureOutputStreamFactory.buildOutputStreamOptions(
                        WRITE_REQUEST_SIZE, WriteMode.OVERWRITE, metadata);

        assertThat(options.getMetadata()).containsEntry("x_cse_key_id", "key-123");
    }

    // --- createOpener: argument validation ---

    static Stream<Arguments> invalidCreateOpenerArgs() {
        final DataLakeStorageOperations ops = new TestingDataLakeStorageOperations();
        return Stream.of(
                Arguments.of(
                        null,
                        "path/file.txt",
                        WRITE_REQUEST_SIZE,
                        WriteMode.OVERWRITE,
                        NullPointerException.class,
                        "fsClient"),
                Arguments.of(
                        ops,
                        null,
                        WRITE_REQUEST_SIZE,
                        WriteMode.OVERWRITE,
                        NullPointerException.class,
                        "filePath"),
                Arguments.of(
                        ops,
                        "path/file.txt",
                        0L,
                        WriteMode.OVERWRITE,
                        IllegalArgumentException.class,
                        "writeRequestSize must be positive"),
                Arguments.of(
                        ops,
                        "path/file.txt",
                        -1L,
                        WriteMode.OVERWRITE,
                        IllegalArgumentException.class,
                        "writeRequestSize must be positive"),
                Arguments.of(
                        ops,
                        "path/file.txt",
                        WRITE_REQUEST_SIZE,
                        null,
                        NullPointerException.class,
                        "writeMode"));
    }

    @ParameterizedTest
    @MethodSource("invalidCreateOpenerArgs")
    void shouldRejectInvalidCreateOpenerArgs(
            final DataLakeStorageOperations fsClient,
            final String filePath,
            final long writeRequestSize,
            final WriteMode writeMode,
            final Class<? extends Throwable> expectedType,
            final String expectedMessage) {
        assertThatThrownBy(
                        () ->
                                AzureOutputStreamFactory.createOpener(
                                        fsClient, filePath, writeRequestSize, writeMode))
                .isInstanceOf(expectedType)
                .hasMessageContaining(expectedMessage);
    }
}
