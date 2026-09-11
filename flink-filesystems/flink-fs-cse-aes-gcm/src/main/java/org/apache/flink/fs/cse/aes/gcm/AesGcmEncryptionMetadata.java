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
import org.apache.flink.fs.cse.EncryptionMetadata;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.flink.fs.cse.aes.gcm.AesGcmCipherStreams.ALGORITHM;

/** AES-256-GCM {@link EncryptionMetadata}: key ID, key version, IV length, encryption context. */
@Internal
@Experimental
@Immutable
public final class AesGcmEncryptionMetadata implements EncryptionMetadata {

    /** GCM authentication tag length in bytes. */
    public static final int GCM_TAG_LENGTH = 16;

    static final String META_KEY_ID = "x_cse_key_id";
    private static final String META_KEY_VERSION = "x_cse_key_version";
    private static final String META_IV_LENGTH = "x_cse_iv_bytes_length";
    private static final String META_ALGORITHM = "x_cse_algorithm";
    private static final String META_TAG_LENGTH = "x_cse_gcm_tag_length";
    private static final String META_CTX_PREFIX = "x_cse_ctx_";
    private static final String META_CHUNK_SIZE = "x_cse_chunk_size";
    private static final String WHOLE_FILE_CHUNK_SIZE = "-1";

    /**
     * Allowed charset for an encryption-context key, applied to the {@code x_cse_ctx_} suffix. This
     * is the strictest common denominator across cloud object stores: Azure blob metadata keys must
     * be C# identifiers, which is a subset of the HTTP-header tokens AWS S3 and GCS accept. A key
     * valid here is therefore portable to all three.
     */
    private static final Pattern CONTEXT_KEY_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final String keyId;
    private final String keyVersion;
    private final int ivByteLength;
    private final Map<String, String> encryptionContext;

    /**
     * @param keyId the encryption key identifier
     * @param keyVersion the key version used for this file
     * @param ivByteLength the IV length in bytes; must be positive
     * @param encryptionContext key-provider authorization context; may be empty
     */
    public AesGcmEncryptionMetadata(
            final String keyId,
            final String keyVersion,
            final int ivByteLength,
            final Map<String, String> encryptionContext) {
        this.keyId = Preconditions.checkNotNull(keyId, "keyId must not be null");
        this.keyVersion = Preconditions.checkNotNull(keyVersion, "keyVersion must not be null");
        Preconditions.checkArgument(
                ivByteLength > 0, "ivByteLength must be positive, got %s", ivByteLength);
        Preconditions.checkNotNull(encryptionContext, "encryptionContext must not be null");
        encryptionContext
                .keySet()
                .forEach(
                        k ->
                                Preconditions.checkArgument(
                                        CONTEXT_KEY_PATTERN.matcher(k).matches(),
                                        "Encryption context key '%s' is not a valid object metadata"
                                                + " key (must match %s)",
                                        k,
                                        CONTEXT_KEY_PATTERN.pattern()));
        this.ivByteLength = ivByteLength;
        this.encryptionContext = Map.copyOf(encryptionContext);
    }

    @Override
    public String keyId() {
        return keyId;
    }

    @Override
    public String getKeyVersion() {
        return keyVersion;
    }

    /** Returns the IV length in bytes. */
    public int getIvByteLength() {
        return ivByteLength;
    }

    @Override
    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }

    @Override
    public Map<String, String> toMetadata() {
        final Map<String, String> map = new HashMap<>();
        map.put(META_KEY_ID, keyId);
        map.put(META_KEY_VERSION, keyVersion);
        map.put(META_IV_LENGTH, Integer.toString(ivByteLength));
        map.put(META_ALGORITHM, ALGORITHM);
        map.put(META_TAG_LENGTH, Integer.toString(GCM_TAG_LENGTH));
        map.put(META_CHUNK_SIZE, WHOLE_FILE_CHUNK_SIZE);
        encryptionContext.forEach((k, v) -> map.put(META_CTX_PREFIX + k, v));
        return Collections.unmodifiableMap(map);
    }

    /**
     * Parses an {@link AesGcmEncryptionMetadata} from a flat map (e.g. file metadata).
     *
     * @param map the flat metadata map from cloud object metadata
     * @return parsed metadata
     * @throws IllegalArgumentException if any required key is missing or malformed
     */
    public static AesGcmEncryptionMetadata fromMetadata(final Map<String, String> map) {
        final String keyId = requireEntry(map, META_KEY_ID);
        final String keyVersion = requireEntry(map, META_KEY_VERSION);
        final String ivByteLengthStr = requireEntry(map, META_IV_LENGTH);
        final String algorithm = requireEntry(map, META_ALGORITHM);
        final String tagLengthStr = requireEntry(map, META_TAG_LENGTH);

        Preconditions.checkArgument(
                ALGORITHM.equals(algorithm),
                "Unsupported algorithm '%s', expected '%s'",
                algorithm,
                ALGORITHM);

        final int tagLength = ensureInt(tagLengthStr, META_TAG_LENGTH);
        Preconditions.checkArgument(
                tagLength == GCM_TAG_LENGTH,
                "Unsupported GCM tag length %s, expected %s",
                tagLength,
                GCM_TAG_LENGTH);

        final String chunkSize = requireEntry(map, META_CHUNK_SIZE);
        Preconditions.checkArgument(
                WHOLE_FILE_CHUNK_SIZE.equals(chunkSize),
                "Chunked encryption is not supported, got %s=%s",
                META_CHUNK_SIZE,
                chunkSize);

        final int ivByteLength = ensureInt(ivByteLengthStr, META_IV_LENGTH);
        Preconditions.checkArgument(
                ivByteLength > 0, "IV byte length must be positive, got %s", ivByteLength);

        final Map<String, String> context = new HashMap<>();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(META_CTX_PREFIX)) {
                context.put(entry.getKey().substring(META_CTX_PREFIX.length()), entry.getValue());
            }
        }

        return new AesGcmEncryptionMetadata(keyId, keyVersion, ivByteLength, context);
    }

    private static String requireEntry(final Map<String, String> map, final String key) {
        final String value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required metadata entry '" + key + "'");
        }
        return value;
    }

    private static int ensureInt(final String value, final String key) {
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Malformed value for '" + key + "': " + value, e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AesGcmEncryptionMetadata)) {
            return false;
        }
        final AesGcmEncryptionMetadata that = (AesGcmEncryptionMetadata) o;
        return ivByteLength == that.ivByteLength
                && Objects.equals(keyId, that.keyId)
                && Objects.equals(keyVersion, that.keyVersion)
                && Objects.equals(encryptionContext, that.encryptionContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyId, keyVersion, ivByteLength, encryptionContext);
    }

    @Override
    public String toString() {
        return "AesGcmEncryptionMetadata{"
                + "keyId="
                + keyId
                + ", keyVersion="
                + keyVersion
                + ", ivByteLength="
                + ivByteLength
                + ", encryptionContext="
                + encryptionContext
                + "}";
    }
}
