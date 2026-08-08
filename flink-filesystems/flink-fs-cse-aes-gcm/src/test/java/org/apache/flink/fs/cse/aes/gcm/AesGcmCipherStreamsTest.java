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

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.stream.Stream;

import static org.apache.flink.fs.cse.aes.gcm.AesGcmCipherStreams.GCM_IV_LENGTH;
import static org.apache.flink.fs.cse.aes.gcm.AesGcmCipherStreams.GCM_OVERHEAD_BYTES;
import static org.apache.flink.fs.cse.aes.gcm.AesGcmDecryptingInputStream.CIPHERTEXT_CHUNK_SIZE;
import static org.apache.flink.fs.cse.aes.gcm.AesGcmEncryptionMetadata.GCM_TAG_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AesGcmCipherStreams}. */
class AesGcmCipherStreamsTest {

    private static final Provider TEST_PROVIDER = new BouncyCastleFipsProvider();

    /** 32-byte AES-256 test key (not secret — test only). */
    private static final byte[] TEST_KEY = new byte[32];

    /** Alternate 32-byte key for wrong-key tests. */
    private static final byte[] OTHER_KEY = new byte[32];

    static {
        TEST_KEY[0] = 1;
        OTHER_KEY[0] = 2;
    }

    private static final byte[] TEST_IV = new byte[GCM_IV_LENGTH];

    // -------------------------------------------------------------------------
    // Round-trip: parameterized over plaintext sizes
    // -------------------------------------------------------------------------

    static Stream<Integer> plaintextSizes() {
        return Stream.of(
                0,
                1,
                GCM_TAG_LENGTH,
                CIPHERTEXT_CHUNK_SIZE - 1,
                CIPHERTEXT_CHUNK_SIZE,
                CIPHERTEXT_CHUNK_SIZE + 1,
                CIPHERTEXT_CHUNK_SIZE * 3,
                100_000);
    }

    @ParameterizedTest
    @MethodSource("plaintextSizes")
    void shouldRoundTripPlaintext(final int size) throws IOException {
        final byte[] plaintext = randomBytes(size);
        final byte[] decrypted = decrypt(encrypt(plaintext, TEST_KEY, TEST_IV), TEST_KEY);
        assertThat(decrypted).isEqualTo(plaintext);
    }

    // -------------------------------------------------------------------------
    // Single-byte read at EOF
    // -------------------------------------------------------------------------

    @Test
    void singleByteReadShouldReturnMinusOneAtEof() throws IOException {
        final byte[] plaintext = randomBytes(16);
        final byte[] ciphertext = encrypt(plaintext, TEST_KEY, TEST_IV);
        final ByteArrayInputStream bais = new ByteArrayInputStream(ciphertext);
        final byte[] iv = bais.readNBytes(GCM_IV_LENGTH);
        try (InputStream stream =
                AesGcmCipherStreams.wrapDecrypting(bais, TEST_KEY, iv, TEST_PROVIDER)) {
            stream.readAllBytes();
            assertThat(stream.read()).isEqualTo(-1);
        }
    }

    // -------------------------------------------------------------------------
    // Ciphertext size
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("plaintextSizes")
    void ciphertextSizeShouldEqualPlaintextPlusOverhead(final int size) throws IOException {
        final byte[] plaintext = randomBytes(size);
        final byte[] ciphertext = encrypt(plaintext, TEST_KEY, TEST_IV);
        assertThat(ciphertext).hasSize(size + GCM_OVERHEAD_BYTES);
    }

    // -------------------------------------------------------------------------
    // Authentication failures
    // -------------------------------------------------------------------------

    @Test
    void shouldThrowOnWrongKey() throws IOException {
        final byte[] ciphertext = encrypt(randomBytes(64), TEST_KEY, TEST_IV);
        assertThatThrownBy(() -> decrypt(ciphertext, OTHER_KEY)).isInstanceOf(IOException.class);
    }

    static Stream<Arguments> tamperPositions() {
        return Stream.of(
                Arguments.of("IV", 0),
                Arguments.of("body", GCM_IV_LENGTH + 1),
                Arguments.of("GCM tag", -1));
    }

    @ParameterizedTest
    @MethodSource("tamperPositions")
    void shouldThrowOnTamperedCiphertext(final String region, final int tamperOffset)
            throws IOException {
        final byte[] ciphertext = encrypt(randomBytes(64), TEST_KEY, TEST_IV);
        final int pos = tamperOffset >= 0 ? tamperOffset : ciphertext.length - 1;
        ciphertext[pos] ^= 0xFF;
        assertThatThrownBy(() -> decrypt(ciphertext, TEST_KEY)).isInstanceOf(IOException.class);
    }

    // -------------------------------------------------------------------------
    // Incremental reads: caller buffer smaller than internal plaintext buffer
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {1, 7, CIPHERTEXT_CHUNK_SIZE - 1})
    void shouldDecryptCorrectlyWithSmallReadBuffer(final int readSize) throws IOException {
        final byte[] plaintext = randomBytes(CIPHERTEXT_CHUNK_SIZE + 100);
        final byte[] ciphertext = encrypt(plaintext, TEST_KEY, TEST_IV);

        final ByteArrayInputStream bais = new ByteArrayInputStream(ciphertext);
        final byte[] iv = bais.readNBytes(GCM_IV_LENGTH);
        final InputStream decryptingStream =
                AesGcmCipherStreams.wrapDecrypting(bais, TEST_KEY, iv, TEST_PROVIDER);

        final ByteArrayOutputStream result = new ByteArrayOutputStream();
        final byte[] buf = new byte[readSize];
        int n;
        while ((n = decryptingStream.read(buf, 0, readSize)) != -1) {
            result.write(buf, 0, n);
        }
        decryptingStream.close();

        assertThat(result.toByteArray()).isEqualTo(plaintext);
    }

    // -------------------------------------------------------------------------
    // Input validation
    // -------------------------------------------------------------------------

    static Stream<Arguments> invalidWrapArgs() {
        return Stream.of(
                Arguments.of(
                        "null provider",
                        new byte[0],
                        TEST_KEY,
                        TEST_IV,
                        null,
                        NullPointerException.class),
                Arguments.of(
                        "null stream",
                        null,
                        TEST_KEY,
                        TEST_IV,
                        TEST_PROVIDER,
                        NullPointerException.class),
                Arguments.of(
                        "null key",
                        new byte[0],
                        null,
                        TEST_IV,
                        TEST_PROVIDER,
                        NullPointerException.class),
                Arguments.of(
                        "short key",
                        new byte[0],
                        new byte[16],
                        TEST_IV,
                        TEST_PROVIDER,
                        IllegalArgumentException.class),
                Arguments.of(
                        "null IV",
                        new byte[0],
                        TEST_KEY,
                        null,
                        TEST_PROVIDER,
                        NullPointerException.class),
                Arguments.of(
                        "short IV",
                        new byte[0],
                        TEST_KEY,
                        new byte[8],
                        TEST_PROVIDER,
                        IllegalArgumentException.class));
    }

    @ParameterizedTest
    @MethodSource("invalidWrapArgs")
    void wrapEncryptingShouldRejectInvalidArgs(
            final String label,
            final byte[] streamBytes,
            final byte[] key,
            final byte[] iv,
            final Provider provider,
            final Class<? extends Throwable> expected) {
        final OutputStream stream = streamBytes != null ? new ByteArrayOutputStream() : null;
        assertThatThrownBy(() -> AesGcmCipherStreams.wrapEncrypting(stream, key, iv, provider))
                .as(label)
                .isInstanceOf(expected);
    }

    @ParameterizedTest
    @MethodSource("invalidWrapArgs")
    void wrapDecryptingShouldRejectInvalidArgs(
            final String label,
            final byte[] streamBytes,
            final byte[] key,
            final byte[] iv,
            final Provider provider,
            final Class<? extends Throwable> expected) {
        final InputStream stream =
                streamBytes != null ? new ByteArrayInputStream(streamBytes) : null;
        assertThatThrownBy(() -> AesGcmCipherStreams.wrapDecrypting(stream, key, iv, provider))
                .as(label)
                .isInstanceOf(expected);
    }

    // -------------------------------------------------------------------------
    // Close drains and verifies GCM tag
    // -------------------------------------------------------------------------

    static Stream<Arguments> closeTamperPositions() {
        return Stream.of(
                Arguments.of("IV", 0),
                Arguments.of("read body", GCM_IV_LENGTH + CIPHERTEXT_CHUNK_SIZE / 2),
                Arguments.of("unread body", GCM_IV_LENGTH + CIPHERTEXT_CHUNK_SIZE * 2),
                Arguments.of("GCM tag", -1));
    }

    @ParameterizedTest
    @MethodSource("closeTamperPositions")
    void closeShouldThrowOnTamperedCiphertextWithoutFullRead(
            final String region, final int tamperOffset) throws IOException {
        final byte[] ciphertext =
                encrypt(randomBytes(CIPHERTEXT_CHUNK_SIZE * 3), TEST_KEY, TEST_IV);
        final int pos = tamperOffset >= 0 ? tamperOffset : ciphertext.length - 1;
        ciphertext[pos] ^= 0xFF;

        final ByteArrayInputStream bais = new ByteArrayInputStream(ciphertext);
        final byte[] iv = bais.readNBytes(GCM_IV_LENGTH);
        final InputStream stream =
                AesGcmCipherStreams.wrapDecrypting(bais, TEST_KEY, iv, TEST_PROVIDER);
        stream.read();
        assertThatThrownBy(stream::close).as(region).isInstanceOf(IOException.class);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] encrypt(final byte[] plaintext, final byte[] key, final byte[] iv)
            throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final OutputStream encryptingStream =
                AesGcmCipherStreams.wrapEncrypting(baos, key, iv, TEST_PROVIDER);
        encryptingStream.write(plaintext);
        encryptingStream.close();
        return baos.toByteArray();
    }

    private static byte[] decrypt(final byte[] ciphertext, final byte[] key) throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(ciphertext);
        final byte[] iv = bais.readNBytes(GCM_IV_LENGTH);
        final InputStream decryptingStream =
                AesGcmCipherStreams.wrapDecrypting(bais, key, iv, TEST_PROVIDER);
        final byte[] result = decryptingStream.readAllBytes();
        decryptingStream.close();
        return result;
    }

    private static byte[] randomBytes(final int size) {
        final byte[] bytes = new byte[size];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}
