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

import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;

import java.io.IOException;
import java.io.InputStream;

/**
 * Streaming AES-256-GCM decrypting {@link InputStream} that works around two bugs in BC FIPS 2.1.2
 * native GCM on Linux x86_64.
 *
 * <h3>Why a custom stream instead of {@code CipherInputStream}?</h3>
 *
 * <p>GCM decryption of checkpoint/state files must be streaming — files can be hundreds of
 * megabytes and must not be buffered entirely in memory. Three candidate approaches exist, and none
 * of them work:
 *
 * <ol>
 *   <li><b>JDK's {@code javax.crypto.CipherInputStream}</b>: For AEAD ciphers, {@code
 *       Cipher.update()} returns no output until {@code doFinal()} — so CipherInputStream buffers
 *       the <em>entire</em> plaintext in memory before returning a single byte. This is a
 *       fundamental limitation of the JDK's CipherInputStream/Cipher interaction for authenticated
 *       ciphers, not a bug.
 *   <li><b>BouncyCastle's {@code CipherInputStream}</b>: Streams correctly (its internal {@code
 *       nextChunk()} calls BC's low-level {@code processBytes()} which does return intermediate
 *       output). However, its internal buffer size is an implementation detail. If BC ever changes
 *       it to a non-64-byte-aligned size, the native GCM bug (bc-java#2294) would corrupt
 *       decryption silently.
 *   <li><b>BC FIPS low-level API ({@code InputAEADDecryptor.getDecryptingStream()})</b>: Returns an
 *       opaque {@link InputStream} backed by BC's internal {@code CipherInputStream}. Same
 *       alignment concern as option 2, plus we lose control over buffer allocation.
 * </ol>
 *
 * <p>This class takes a fourth approach: it calls {@code cipher.update()} and {@code
 * cipher.doFinal()} directly with explicitly controlled chunk sizes. This gives us:
 *
 * <ul>
 *   <li><b>Streaming</b>: plaintext is produced incrementally as ciphertext chunks are processed —
 *       the file is never fully buffered in memory.
 *   <li><b>Aligned reads</b>: ciphertext is always read in exact {@value #CIPHERTEXT_CHUNK_SIZE}
 *       -byte chunks (64-byte aligned), avoiding the native GCM stale {@code inlen} bug.
 *   <li><b>Correct buffer sizing</b>: output buffers use {@code cipher.getOutputSize()} instead of
 *       {@code cipher.getUpdateOutputSize()}, avoiding the native {@code OutputLengthException}
 *       bug.
 *   <li><b>GCM tag verification at EOF</b>: {@code cipher.doFinal()} is called when the ciphertext
 *       stream reaches EOF, verifying the authentication tag. If verification fails, an {@link
 *       IOException} wrapping {@link AEADBadTagException} is thrown.
 * </ul>
 *
 * <h3>BC FIPS bugs worked around</h3>
 *
 * <ol>
 *   <li><b>Stale {@code inlen} in {@code process_buffer_dec}</b> (bc-java#2294): The native GCM
 *       implementation does not decrement {@code inlen} after the initial alignment copy, causing
 *       over-read and GCM hash corruption on non-64-byte-aligned input. Workaround: always feed
 *       8192-byte (64-byte-aligned) chunks to {@code cipher.update()}.
 *   <li><b>{@code getUpdateOutputSize()} underestimation</b>: The native path returns a buffer size
 *       that is too small, causing {@code OutputLengthException}. Workaround: use {@code
 *       cipher.getOutputSize()} which over-allocates to account for buffered state and tag.
 * </ol>
 *
 * <p>Both bugs affect only the native GCM path on Linux x86_64 (ARM NEON, Intel AVX, Intel
 * AVX-512). macOS and non-x86_64 platforms use the Java GCM fallback ({@code GCMBlockCipher}) which
 * is not affected. Both are fixed in bc-fips 2.1.3 (pending FIPS re-certification).
 *
 * <h3>Threading and lifecycle contract</h3>
 *
 * <p>This stream is designed as an inner layer of {@link
 * org.apache.flink.core.fs.ObjectStorageInputStream}, which provides thread safety (via {@code
 * ReentrantLock}), argument validation, closed-state checks, and {@code available()} computation.
 * This class therefore omits all of those concerns — it trusts its single caller to enforce them.
 * It must not be used as a standalone public {@link InputStream}.
 *
 * @see <a href="https://github.com/bcgit/bc-java/issues/2294">bc-java#2294</a>
 * @see AesGcmCipherStreams#wrapDecrypting
 */
@NotThreadSafe
final class AesGcmDecryptingInputStream extends InputStream {

    /**
     * Ciphertext chunk size in bytes. Must be a multiple of 64 to avoid the BC FIPS native GCM
     * stale {@code inlen} bug. 8192 is chosen for consistency with typical I/O buffer sizes.
     *
     * <p>{@code readNBytes()} is used to guarantee exactly this many bytes are read from the
     * underlying stream (blocking until full or EOF), unlike {@code read()} which may return fewer.
     */
    static final int CIPHERTEXT_CHUNK_SIZE = 8192;

    private final InputStream ciphertextStream;
    private final Cipher cipher;

    private final byte[] ciphertextBuffer;
    private byte[] plaintextBuffer;
    private int plaintextBufferPos;
    private int plaintextBufferLimit;

    private final byte[] singleByte = new byte[1];
    private boolean endOfCiphertext;

    AesGcmDecryptingInputStream(final InputStream ciphertextStream, final Cipher cipher) {
        this.ciphertextStream = ciphertextStream;
        this.cipher = cipher;
        this.ciphertextBuffer = new byte[CIPHERTEXT_CHUNK_SIZE];
        this.plaintextBuffer = new byte[0];
        this.plaintextBufferPos = 0;
        this.plaintextBufferLimit = 0;
        this.endOfCiphertext = false;
    }

    /** Delegates to {@link #read(byte[], int, int)} to reuse the decryption logic. */
    @Override
    public int read() throws IOException {
        // 0xFF mask converts signed byte (-128..127) to unsigned int (0..255) per InputStream
        // contract
        return read(singleByte, 0, 1) == -1 ? -1 : (singleByte[0] & 0xFF);
    }

    /** Returns at most one plaintext buffer worth of data per call; callers must loop for more. */
    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (plaintextBufferPos >= plaintextBufferLimit && !fillPlaintextBuffer()) {
            return -1;
        }

        final int available = plaintextBufferLimit - plaintextBufferPos;
        final int toCopy = Math.min(len, available);
        System.arraycopy(plaintextBuffer, plaintextBufferPos, b, off, toCopy);
        plaintextBufferPos += toCopy;
        return toCopy;
    }

    /**
     * Reads a ciphertext chunk, decrypts it, and fills the internal plaintext buffer.
     *
     * <p>Uses {@code readNBytes()} to guarantee exactly {@link #CIPHERTEXT_CHUNK_SIZE} bytes are
     * read from the underlying stream. This ensures {@code cipher.update()} always receives
     * 64-byte-aligned input, avoiding bc-java#2294. When fewer bytes are returned (EOF), {@code
     * cipher.doFinal()} processes the remaining ciphertext and verifies the GCM authentication tag.
     *
     * @return {@code true} if plaintext was produced, {@code false} if EOF
     */
    private boolean fillPlaintextBuffer() throws IOException {
        if (endOfCiphertext) {
            return false;
        }

        while (true) {
            final int bytesRead =
                    ciphertextStream.readNBytes(ciphertextBuffer, 0, ciphertextBuffer.length);

            if (bytesRead < ciphertextBuffer.length) {
                endOfCiphertext = true;
                return finalizeCipher(bytesRead);
            }

            try {
                // getOutputSize(), not getUpdateOutputSize() — the latter triggers
                // OutputLengthException in BC native GCM.
                final int requiredOutputBufferSize = cipher.getOutputSize(bytesRead);
                if (plaintextBuffer.length < requiredOutputBufferSize) {
                    plaintextBuffer = new byte[requiredOutputBufferSize];
                }
                final int plaintextLen =
                        cipher.update(ciphertextBuffer, 0, bytesRead, plaintextBuffer, 0);
                plaintextBufferPos = 0;
                plaintextBufferLimit = plaintextLen;
                if (plaintextLen > 0) {
                    return true;
                }
            } catch (final Exception e) {
                throw new IOException("Cipher update failed", e);
            }
        }
    }

    /** Calls {@code cipher.doFinal()} on the remaining ciphertext and verifies the GCM tag. */
    private boolean finalizeCipher(final int bytesRead) throws IOException {
        try {
            final byte[] finalPlaintext = cipher.doFinal(ciphertextBuffer, 0, bytesRead);
            if (finalPlaintext != null && finalPlaintext.length > 0) {
                plaintextBuffer = finalPlaintext;
                plaintextBufferPos = 0;
                plaintextBufferLimit = finalPlaintext.length;
                return true;
            }
        } catch (final AEADBadTagException e) {
            throw new IOException("GCM authentication tag verification failed", e);
        } catch (final Exception e) {
            throw new IOException("Cipher finalization failed", e);
        }
        return false;
    }

    /**
     * Drains remaining ciphertext through the cipher to trigger GCM tag verification, then closes
     * the underlying stream.
     *
     * <p>If the stream was not fully consumed, this method feeds the remaining ciphertext to {@code
     * cipher.update()} / {@code cipher.doFinal()} (discarding the plaintext output) so that the GCM
     * authentication tag is always verified. This guarantees integrity even if the caller abandons
     * the stream mid-read.
     *
     * @throws IOException if GCM tag verification fails or an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(this::drainRemainingCiphertext, ciphertextStream);
        } catch (final Exception e) {
            throw e instanceof IOException ? (IOException) e : new IOException(e);
        }
    }

    private void drainRemainingCiphertext() throws IOException {
        while (!endOfCiphertext) {
            fillPlaintextBuffer();
        }
    }
}
