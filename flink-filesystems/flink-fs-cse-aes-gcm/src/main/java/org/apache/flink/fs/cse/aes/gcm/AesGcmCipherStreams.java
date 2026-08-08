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

import org.apache.flink.util.Preconditions;

import org.bouncycastle.jcajce.io.CipherOutputStream;

import javax.annotation.concurrent.Immutable;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Provider;

import static org.apache.flink.fs.cse.aes.gcm.AesGcmEncryptionMetadata.GCM_TAG_LENGTH;

/**
 * Static utilities for AES-256-GCM stream encryption and decryption via the BCFIPS JCA provider.
 */
@Immutable
final class AesGcmCipherStreams {

    /** GCM IV length in bytes (NIST SP 800-38D recommended). */
    static final int GCM_IV_LENGTH = 12;

    /** Total overhead: IV prefix plus GCM authentication tag. */
    static final int GCM_OVERHEAD_BYTES = GCM_IV_LENGTH + GCM_TAG_LENGTH;

    /** Key length in bytes for AES-256. */
    static final int AES_256_KEY_LENGTH = 32;

    /**
     * JCA cipher algorithm identifier, also used as the wire-format algorithm value in metadata.
     */
    static final String ALGORITHM = "AES/GCM/NoPadding";

    /** GCM tag length in bits, as required by {@link GCMParameterSpec}. */
    private static final int GCM_TAG_LENGTH_BITS = GCM_TAG_LENGTH * 8;

    private AesGcmCipherStreams() {}

    /**
     * Wraps {@code rawOut} in an AES-256-GCM encrypting stream.
     *
     * <p>Wire format: {@code [12-byte IV][ciphertext][16-byte GCM tag]}. The IV prefix is written
     * immediately, and the GCM tag is appended automatically when the returned stream is closed.
     *
     * <p>Uses BouncyCastle FIPS via the supplied JCA {@link Provider}. The returned stream is a BC
     * {@link CipherOutputStream} — not the JDK's {@code javax.crypto.CipherOutputStream} — because
     * BC's implementation calls {@code cipher.doFinal()} on close, which is required to finalize
     * the GCM authentication tag. The JDK's version swallows exceptions from {@code doFinal()},
     * which would silently produce truncated ciphertext.
     *
     * @param rawOut the underlying output stream; must not be null
     * @param key 32-byte AES-256 key material; must not be null
     * @param iv 12-byte GCM initialization vector; must not be null
     * @param provider the JCA {@link Provider} to use for cipher instantiation; must not be null
     * @return an encrypting {@link OutputStream} wrapping {@code rawOut}
     * @throws IOException if writing the IV prefix or cipher initialization fails
     * @throws IllegalArgumentException if {@code key} or {@code iv} have wrong length
     */
    static OutputStream wrapEncrypting(
            final OutputStream rawOut, final byte[] key, final byte[] iv, final Provider provider)
            throws IOException {
        Preconditions.checkNotNull(rawOut, "rawOut must not be null");
        final Cipher cipher = initCipher(Cipher.ENCRYPT_MODE, key, iv, provider);
        rawOut.write(iv);
        return new CipherOutputStream(rawOut, cipher);
    }

    /**
     * Wraps {@code ciphertextIn} in an AES-256-GCM decrypting stream.
     *
     * <p>The caller must have already consumed the 12-byte IV prefix from the underlying stream and
     * provide it via {@code iv}. The returned stream decrypts ciphertext in 8192-byte chunks and
     * verifies the GCM authentication tag when EOF is reached or the stream is closed (stream is
     * drained till EOF on close).
     *
     * @param ciphertextIn ciphertext stream positioned after the IV prefix; must not be null
     * @param key 32-byte AES-256 key material; must not be null
     * @param iv 12-byte GCM initialization vector; must not be null
     * @param provider the JCA {@link Provider} to use for cipher instantiation; must not be null
     * @return a decrypting {@link InputStream} wrapping {@code ciphertextIn}
     * @throws IOException if cipher initialization fails
     * @throws IllegalArgumentException if {@code key} or {@code iv} have wrong length
     */
    static InputStream wrapDecrypting(
            final InputStream ciphertextIn,
            final byte[] key,
            final byte[] iv,
            final Provider provider)
            throws IOException {
        Preconditions.checkNotNull(ciphertextIn, "ciphertextIn must not be null");
        final Cipher cipher = initCipher(Cipher.DECRYPT_MODE, key, iv, provider);
        return new AesGcmDecryptingInputStream(ciphertextIn, cipher);
    }

    /**
     * Validates {@code key} and {@code iv}, then creates and initializes an AES-GCM {@link Cipher}.
     *
     * @param mode {@link Cipher#ENCRYPT_MODE} or {@link Cipher#DECRYPT_MODE}
     * @param key 32-byte AES-256 key material; must not be null
     * @param iv 12-byte GCM initialization vector; must not be null
     * @param provider the JCA {@link Provider} to use for cipher instantiation; must not be null
     * @return an initialized {@link Cipher}
     * @throws IOException if cipher instantiation or initialization fails
     */
    private static Cipher initCipher(
            final int mode, final byte[] key, final byte[] iv, final Provider provider)
            throws IOException {
        Preconditions.checkNotNull(provider, "provider must not be null");
        Preconditions.checkNotNull(key, "key must not be null");
        Preconditions.checkArgument(
                key.length == AES_256_KEY_LENGTH,
                "key must be exactly %s bytes, got %s",
                AES_256_KEY_LENGTH,
                key.length);
        Preconditions.checkNotNull(iv, "iv must not be null");
        Preconditions.checkArgument(
                iv.length == GCM_IV_LENGTH,
                "iv must be exactly %s bytes, got %s",
                GCM_IV_LENGTH,
                iv.length);

        try {
            final Cipher cipher = Cipher.getInstance(ALGORITHM, provider);
            cipher.init(
                    mode,
                    new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            return cipher;
        } catch (final GeneralSecurityException e) {
            throw new IOException(
                    String.format(
                            "Failed to initialize AES-GCM cipher (algorithm=%s, provider=%s).",
                            ALGORITHM, provider.getName()),
                    e);
        }
    }
}
