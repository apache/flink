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

package org.apache.flink.fs.cse.aes.gcm;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.fs.cse.EncryptedReadContext;
import org.apache.flink.fs.cse.EncryptedWriteContext;
import org.apache.flink.fs.cse.KeyProviderException;
import org.apache.flink.fs.cse.TestingKeyProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AesGcmCseStreamFactory}. */
class AesGcmCseStreamFactoryTest {

    static final String TEST_FILE = "test-file";

    private static final String KEY_ID = "test-key";
    private static final Map<String, String> ENC_CTX = Map.of("org_id", "org-test");
    private static final int TEST_READ_BUFFER_SIZE = 8192;

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    private static final class WriteResult {
        final byte[] ciphertext;
        final Map<String, String> metadata;

        WriteResult(final byte[] ciphertext, final Map<String, String> metadata) {
            this.ciphertext = ciphertext;
            this.metadata = metadata;
        }
    }

    private static WriteResult writeEncrypted(
            final AesGcmCseStreamFactory factory, final byte[] plaintext) throws IOException {
        final ByteArrayOutputStream ciphertextOut = new ByteArrayOutputStream();
        final AtomicReference<Map<String, String>> capturedMetadata = new AtomicReference<>();
        final OutputStream encryptingStream =
                factory.openEncryptedWrite(
                        ctx -> {
                            capturedMetadata.set(ctx.getMetadata());
                            return ciphertextOut;
                        },
                        EncryptedWriteContext.of(TEST_FILE));
        encryptingStream.write(plaintext);
        encryptingStream.close();
        return new WriteResult(ciphertextOut.toByteArray(), capturedMetadata.get());
    }

    private static FSDataInputStream openRead(
            final AesGcmCseStreamFactory factory,
            final byte[] ciphertext,
            final Map<String, String> metadata)
            throws IOException {
        final EncryptedReadContext readCtx =
                EncryptedReadContext.of(
                        metadata, TEST_FILE, ciphertext.length, TEST_READ_BUFFER_SIZE);
        return factory.openEncryptedRead(ctx -> new ByteArrayInputStream(ciphertext), readCtx);
    }

    private static byte[] randomBytes(final int size) {
        final byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private static AesGcmCseStreamFactory newFactory() {
        return newFactory(new TestingKeyProvider());
    }

    private static AesGcmCseStreamFactory newFactory(final TestingKeyProvider keyProvider) {
        return new AesGcmCseStreamFactory(keyProvider, KEY_ID, ENC_CTX);
    }

    private static byte[] readAllEncrypted(
            final AesGcmCseStreamFactory factory, final WriteResult result) throws IOException {
        try (final FSDataInputStream in = openRead(factory, result.ciphertext, result.metadata)) {
            return in.readAllBytes();
        }
    }

    // -------------------------------------------------------------------------
    // Round-trip with key rotation
    // -------------------------------------------------------------------------

    @Test
    void testKeyRotationIsReflectedInMetadata() throws IOException {
        final TestingKeyProvider keyProvider = new TestingKeyProvider();
        final AesGcmCseStreamFactory factory = newFactory(keyProvider);

        final byte[] plaintext1 = randomBytes(500);
        final byte[] plaintext2 = randomBytes(500);

        final WriteResult result1 = writeEncrypted(factory, plaintext1);
        keyProvider.rotateKey();
        final WriteResult result2 = writeEncrypted(factory, plaintext2);

        assertThat(readAllEncrypted(factory, result1)).isEqualTo(plaintext1);
        assertThat(readAllEncrypted(factory, result2)).isEqualTo(plaintext2);
        assertThat(result1.metadata.get("x_cse_key_version"))
                .isNotEqualTo(result2.metadata.get("x_cse_key_version"));
    }

    // -------------------------------------------------------------------------
    // isEncrypted: parameterized
    // -------------------------------------------------------------------------

    static Stream<Arguments> isEncryptedCases() {
        return Stream.of(
                Arguments.of("encryptedMetadata", Map.of("x_cse_key_id", "some-key-id"), true),
                Arguments.of("emptyMap", Map.of(), false),
                Arguments.of("unrelatedKeys", Map.of("unrelated", "value"), false));
    }

    @ParameterizedTest
    @MethodSource("isEncryptedCases")
    void isEncryptedTests(
            final String caseName, final Map<String, String> inputMap, final boolean expected) {
        final AesGcmCseStreamFactory factory = newFactory();
        assertThat(factory.isEncrypted(inputMap)).as(caseName).isEqualTo(expected);
    }

    // -------------------------------------------------------------------------
    // encryptionContext: parameterized
    // -------------------------------------------------------------------------

    static Stream<Arguments> encryptionContextCases() {
        return Stream.of(
                Arguments.of("withContext", Map.of("org_id", "org-123", "lfcp_id", "lfcp-456")),
                Arguments.of("emptyContext", Map.of()));
    }

    @ParameterizedTest
    @MethodSource("encryptionContextCases")
    void encryptionContextTests(final String caseName, final Map<String, String> encryptionContext)
            throws IOException {
        final AesGcmCseStreamFactory factory =
                new AesGcmCseStreamFactory(new TestingKeyProvider(), KEY_ID, encryptionContext);
        final byte[] plaintext = randomBytes(64);
        final WriteResult result = writeEncrypted(factory, plaintext);

        if (!encryptionContext.isEmpty()) {
            for (final Map.Entry<String, String> entry : encryptionContext.entrySet()) {
                assertThat(result.metadata)
                        .as(caseName + ": ctx key x_cse_ctx_" + entry.getKey())
                        .containsEntry("x_cse_ctx_" + entry.getKey(), entry.getValue());
            }
        } else {
            assertThat(result.metadata.keySet())
                    .as(caseName + ": no ctx keys")
                    .noneMatch(k -> k.startsWith("x_cse_ctx_"));
        }

        assertThat(result.metadata)
                .as(caseName + ": iv length key present")
                .containsKey("x_cse_iv_bytes_length");
        assertThat(result.metadata).as(caseName + ": no raw iv key").doesNotContainKey("x_cse_iv");

        assertThat(readAllEncrypted(factory, result))
                .as(caseName + ": round-trip")
                .isEqualTo(plaintext);
    }

    @Test
    void shouldThrowOnNullEncryptionContext() {
        assertThatThrownBy(() -> new AesGcmCseStreamFactory(new TestingKeyProvider(), KEY_ID, null))
                .isInstanceOf(NullPointerException.class);
    }

    // -------------------------------------------------------------------------
    // Seek: parameterized
    // -------------------------------------------------------------------------

    static Stream<Arguments> seekCases() {
        return Stream.of(
                Arguments.of("seekToStart", 0),
                Arguments.of("seekToMiddle", 5000),
                Arguments.of("seekToEnd", 10_000));
    }

    @ParameterizedTest
    @MethodSource("seekCases")
    void seekTests(final String caseName, final int seekPosition) throws IOException {
        final AesGcmCseStreamFactory factory = newFactory();
        final byte[] plaintext = randomBytes(10_000);
        final WriteResult result = writeEncrypted(factory, plaintext);

        try (final FSDataInputStream in = openRead(factory, result.ciphertext, result.metadata)) {
            in.seek(seekPosition);
            final byte[] read = in.readAllBytes();
            assertThat(read)
                    .as(caseName)
                    .isEqualTo(Arrays.copyOfRange(plaintext, seekPosition, plaintext.length));
        }
    }

    // -------------------------------------------------------------------------
    // Empty-file round-trip
    // -------------------------------------------------------------------------

    @Test
    void shouldRoundTripEmptyFile() throws IOException {
        final AesGcmCseStreamFactory factory = newFactory();
        final WriteResult result = writeEncrypted(factory, new byte[0]);
        assertThat(readAllEncrypted(factory, result)).isEmpty();
    }

    // -------------------------------------------------------------------------
    // Write-path failure: raw stream closed on wrapEncrypting error
    // -------------------------------------------------------------------------

    @Test
    void shouldCloseRawStreamOnWriteFailure() {
        final AesGcmCseStreamFactory factory = newFactory();
        final IOException cause = new IOException("write failure");
        final OutputStream failingStream =
                new OutputStream() {
                    boolean closed;

                    @Override
                    public void write(final int b) throws IOException {
                        throw cause;
                    }

                    @Override
                    public void close() {
                        closed = true;
                    }
                };

        assertThatThrownBy(
                        () ->
                                factory.openEncryptedWrite(
                                        ctx -> failingStream, EncryptedWriteContext.of(TEST_FILE)))
                .isInstanceOf(IOException.class)
                .isSameAs(cause);
        assertThat(failingStream).extracting("closed").isEqualTo(true);
    }

    // -------------------------------------------------------------------------
    // Error: missing metadata
    // -------------------------------------------------------------------------

    @Test
    void shouldThrowOnMissingMetadataForRead() {
        final AesGcmCseStreamFactory factory = newFactory();
        assertThatThrownBy(() -> openRead(factory, new byte[64], Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowOnCiphertextLengthBelowOverhead() throws IOException {
        final AesGcmCseStreamFactory factory = newFactory();
        final WriteResult result = writeEncrypted(factory, randomBytes(64));
        final EncryptedReadContext readCtx =
                EncryptedReadContext.of(
                        result.metadata,
                        TEST_FILE,
                        AesGcmCipherStreams.GCM_OVERHEAD_BYTES - 1,
                        TEST_READ_BUFFER_SIZE);
        assertThatThrownBy(
                        () ->
                                factory.openEncryptedRead(
                                        ctx -> new ByteArrayInputStream(result.ciphertext),
                                        readCtx))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("less than GCM overhead");
    }

    // -------------------------------------------------------------------------
    // Error propagation: KeyProviderException from key provider
    // -------------------------------------------------------------------------

    @Test
    void shouldPropagateKeyProviderException() throws IOException {
        final AesGcmCseStreamFactory factory = newFactory();
        final WriteResult result = writeEncrypted(factory, randomBytes(64));

        final Map<String, String> tamperedMetadata = new HashMap<>(result.metadata);
        tamperedMetadata.put("x_cse_key_version", "unknown-version");

        assertThatThrownBy(() -> openRead(factory, result.ciphertext, tamperedMetadata))
                .isInstanceOf(KeyProviderException.class);
    }
}
