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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.fs.azurefs;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.cse.TestingKeyProvider;
import org.apache.flink.fs.cse.aes.gcm.AesGcmCseStreamFactory;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip CSE integration tests for {@link AzureDataLakeFileSystem}.
 *
 * <p>Verifies that the FS layer correctly routes writes through {@link
 * AesGcmCseStreamFactory#openEncryptedWrite}, detects encrypted blobs via {@link
 * AesGcmCseStreamFactory#isEncrypted}, and routes reads through {@link
 * AesGcmCseStreamFactory#openEncryptedRead}. Also verifies the plaintext fallback when no
 * encryption metadata is present.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AzureDataLakeFileSystemCseTest {

    private static final URI TEST_URI =
            URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net");

    private static final int READ_BUFFER_SIZE = 256 * 1024;
    private static final long WRITE_REQUEST_SIZE = 8 * 1024 * 1024L;
    private static final String WRITE_KEY_ID = "test-key";

    private TestingDataLakeStorageOperations storageOps;
    private TestingKeyProvider keyProvider;
    private AesGcmCseStreamFactory cseFactory;

    @BeforeEach
    void setUp() {
        storageOps = new TestingDataLakeStorageOperations();
        keyProvider = new TestingKeyProvider();
        cseFactory = new AesGcmCseStreamFactory(keyProvider, WRITE_KEY_ID, Map.of());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] randomBytes(final int size) {
        final byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private AzureDataLakeFileSystem buildFs(
            @Nullable final AesGcmCseStreamFactory factory, final boolean encryptWrites) {
        final AzureDataLakeFileSystem.Builder builder =
                AzureDataLakeFileSystem.builder(storageOps, TEST_URI)
                        .readBufferSize(READ_BUFFER_SIZE)
                        .writeRequestSize(WRITE_REQUEST_SIZE);
        if (factory != null) {
            builder.cseFactory(factory);
        }
        builder.encryptWrites(encryptWrites);
        return builder.build();
    }

    /** Writes content through a CSE-enabled FS so the blob gets encryption metadata. */
    private void writeEncrypted(final String relativePath, final byte[] content) throws Exception {
        final AzureDataLakeFileSystem writeFs = buildFs(cseFactory, true);
        final Path path = new Path(TEST_URI + "/" + relativePath);
        try (FSDataOutputStream out = writeFs.create(path, WriteMode.OVERWRITE)) {
            out.write(content);
        }
    }

    /** Pre-adds a plaintext file directly into storage (no encryption metadata). */
    private void addPlaintext(final String relativePath, final byte[] content) {
        storageOps.addFile(relativePath, content, OffsetDateTime.now());
    }

    private byte[] readFile(final AzureDataLakeFileSystem fileSystem, final String relativePath)
            throws Exception {
        final Path path = new Path(TEST_URI + "/" + relativePath);
        try (FSDataInputStream in = fileSystem.open(path)) {
            return in.readAllBytes();
        }
    }

    // -------------------------------------------------------------------------
    // CSE routing matrix
    // -------------------------------------------------------------------------

    Stream<Arguments> cseRoutingMatrix() {
        final byte[] content = randomBytes(256);
        final byte[] empty = new byte[0];
        final BiConsumerWithException<String, byte[], Exception> encrypted = this::writeEncrypted;
        final BiConsumerWithException<String, byte[], Exception> plaintext = this::addPlaintext;
        return Stream.of(
                // cseFactory=present, encryptWrites=true, blob=encrypted
                Arguments.of("encrypted", content, true, encrypted),
                Arguments.of("encrypted_empty", empty, true, encrypted),
                // cseFactory=present, encryptWrites=true, blob=plaintext
                Arguments.of("plaintext_cse_enabled", content, true, plaintext),
                // cseFactory=present, encryptWrites=false, blob=encrypted
                Arguments.of("encrypted_read_only", content, false, encrypted),
                // cseFactory=present, encryptWrites=false, blob=plaintext
                Arguments.of("plaintext_read_only", content, false, plaintext));
    }

    @ParameterizedTest
    @MethodSource("cseRoutingMatrix")
    void shouldRouteReadCorrectly(
            final String caseName,
            final byte[] content,
            final boolean encryptWrites,
            final BiConsumerWithException<String, byte[], Exception> contentCreator)
            throws Exception {

        final String filePath = caseName + ".bin";
        contentCreator.accept(filePath, content);

        final AzureDataLakeFileSystem readFs = buildFs(cseFactory, encryptWrites);

        assertThat(readFile(readFs, filePath)).as(caseName).isEqualTo(content);
    }

    // -------------------------------------------------------------------------
    // Key version selection from metadata
    // -------------------------------------------------------------------------

    @Test
    void shouldDecryptUsingKeyVersionFromMetadata() throws Exception {
        final byte[] content1 = randomBytes(128);
        final byte[] content2 = randomBytes(128);

        writeEncrypted("file1.bin", content1);
        keyProvider.rotateKey();
        writeEncrypted("file2.bin", content2);

        final AzureDataLakeFileSystem readFs = buildFs(cseFactory, true);

        assertThat(readFile(readFs, "file1.bin")).isEqualTo(content1);
        assertThat(readFile(readFs, "file2.bin")).isEqualTo(content2);
    }
}
