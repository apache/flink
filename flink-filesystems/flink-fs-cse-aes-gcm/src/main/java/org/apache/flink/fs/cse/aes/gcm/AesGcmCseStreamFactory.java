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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.InputStreamOpener;
import org.apache.flink.core.fs.ObjectStorageInputStream;
import org.apache.flink.core.fs.OutputStreamOpener;
import org.apache.flink.core.fs.WriteContext;
import org.apache.flink.fs.cse.CseStreamFactory;
import org.apache.flink.fs.cse.EncryptedReadContext;
import org.apache.flink.fs.cse.EncryptedWriteContext;
import org.apache.flink.fs.cse.KeyMaterial;
import org.apache.flink.fs.cse.KeyProvider;
import org.apache.flink.util.Preconditions;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Map;

/**
 * AES-256-GCM {@link CseStreamFactory} backed by BouncyCastle FIPS.
 *
 * <p>This factory owns all GCM-specific concerns: IV generation via a shared {@link SecureRandom},
 * BCFIPS provider instantiation, {@link AesGcmEncryptionMetadata} construction (write path) and
 * parsing (read path), and {@link KeyProvider} calls. The BCFIPS provider is held as a field and
 * passed explicitly to each cipher operation.
 *
 * <p>Wire format: {@code [12-byte IV][ciphertext][16-byte GCM tag]}. The IV is written as a prefix
 * to the ciphertext stream, and the GCM tag is appended on close.
 *
 * <p>Instances are thread-safe and intended to be shared across the filesystem.
 *
 * @see CseStreamFactory
 * @see AesGcmEncryptionMetadata
 */
@Internal
@Experimental
@ThreadSafe
public final class AesGcmCseStreamFactory implements CseStreamFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AesGcmCseStreamFactory.class);

    /** Shared, thread-safe SecureRandom for IV generation. */
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final KeyProvider keyProvider;
    @Nullable private final String writeKeyId;
    private final Map<String, String> encryptionContext;
    private final Provider bcFipsProvider;

    /**
     * Creates an AES-256-GCM CSE stream factory and instantiates the BCFIPS JCA provider.
     *
     * <p>The provider is held as a field and passed explicitly to each cipher operation.
     *
     * @param keyProvider provider for DEK retrieval; must be thread-safe
     * @param writeKeyId the key identifier passed to the key provider on every write, or {@code
     *     null} for read-only decryption mode
     * @param encryptionContext encryption context for authorization and audit; (pass {@link
     *     Map#of()} when no context is needed)
     */
    public AesGcmCseStreamFactory(
            final KeyProvider keyProvider,
            @Nullable final String writeKeyId,
            final Map<String, String> encryptionContext) {
        this.keyProvider = Preconditions.checkNotNull(keyProvider, "keyProvider");
        this.writeKeyId = writeKeyId;
        Preconditions.checkNotNull(encryptionContext, "encryptionContext");
        this.encryptionContext = Map.copyOf(encryptionContext);
        this.bcFipsProvider = new BouncyCastleFipsProvider();
    }

    @Override
    public OutputStream openEncryptedWrite(
            final OutputStreamOpener opener, final EncryptedWriteContext ctx) throws IOException {
        Preconditions.checkState(
                writeKeyId != null,
                "Cannot write encrypted data without a key-id (read-only decryption mode)");
        LOG.debug("Opening encrypted write for '{}'", ctx.getFilePath());
        final KeyMaterial keyMat = keyProvider.getActiveKey(writeKeyId, encryptionContext);
        final AesGcmEncryptionMetadata encMeta =
                new AesGcmEncryptionMetadata(
                        keyMat.getKeyId(),
                        keyMat.getVersion(),
                        AesGcmCipherStreams.GCM_IV_LENGTH,
                        encryptionContext);
        final OutputStream raw = opener.open(WriteContext.of(encMeta.toMetadata()));
        try {
            return AesGcmCipherStreams.wrapEncrypting(
                    raw, keyMat.getMaterial(), generateIv(), bcFipsProvider);
        } catch (final IOException | RuntimeException e) {
            try {
                raw.close();
            } catch (final Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Override
    public boolean isEncrypted(final Map<String, String> blobMetadata) {
        return blobMetadata.containsKey(AesGcmEncryptionMetadata.META_KEY_ID);
    }

    @Override
    public FSDataInputStream openEncryptedRead(
            final InputStreamOpener opener, final EncryptedReadContext ctx) throws IOException {
        final AesGcmEncryptionMetadata encMeta =
                AesGcmEncryptionMetadata.fromMetadata(ctx.getBlobMetadata());
        final KeyMaterial keyMat = keyProvider.getKeyForMetadata(encMeta);
        final long ciphertextLength = ctx.getCiphertextLength();
        Preconditions.checkArgument(
                ciphertextLength >= AesGcmCipherStreams.GCM_OVERHEAD_BYTES,
                "Ciphertext length %s is less than GCM overhead (%s bytes)",
                ciphertextLength,
                AesGcmCipherStreams.GCM_OVERHEAD_BYTES);
        final long plaintextLength = ciphertextLength - AesGcmCipherStreams.GCM_OVERHEAD_BYTES;
        // skipOnSeekThreshold = plaintextLength: always read-and-discard on forward seek,
        // never reopen. GCM requires decryption from byte 0 regardless, so reopening
        // would just add extra HTTP request(s) for the same full re-read.
        return new ObjectStorageInputStream(
                plaintextLength,
                plaintextLength,
                new AesGcmCseInputStreamExtension(
                        opener,
                        ctx.getReadBufferSize(),
                        buffered -> readIvAndWrapDecrypting(buffered, keyMat, bcFipsProvider)));
    }

    /**
     * Reads the {@link AesGcmCipherStreams#GCM_IV_LENGTH}-byte IV prefix from {@code buffered} and
     * returns an AES-GCM decrypting stream.
     *
     * @param buffered the buffered ciphertext stream positioned at the start of the IV prefix
     * @param keyMaterial the {@link AesGcmCipherStreams#AES_256_KEY_LENGTH}-byte AES-256 data
     *     encryption key
     * @param provider the JCA {@link Provider} to use for cipher instantiation
     * @return a decrypting {@link InputStream}
     * @throws IOException if the IV prefix is truncated or cipher initialization fails
     */
    private static InputStream readIvAndWrapDecrypting(
            final InputStream buffered, final KeyMaterial keyMaterial, final Provider provider)
            throws IOException {
        final byte[] iv = buffered.readNBytes(AesGcmCipherStreams.GCM_IV_LENGTH);
        if (iv.length != AesGcmCipherStreams.GCM_IV_LENGTH) {
            throw new IOException("Unexpected EOF reading GCM IV prefix");
        }
        return AesGcmCipherStreams.wrapDecrypting(
                buffered, keyMaterial.getMaterial(), iv, provider);
    }

    @Override
    public void close() throws IOException {
        keyProvider.close();
    }

    private static byte[] generateIv() {
        final byte[] iv = new byte[AesGcmCipherStreams.GCM_IV_LENGTH];
        SECURE_RANDOM.nextBytes(iv);
        return iv;
    }
}
