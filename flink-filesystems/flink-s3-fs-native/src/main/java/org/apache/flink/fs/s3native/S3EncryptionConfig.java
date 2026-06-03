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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for S3 server-side encryption (SSE).
 *
 * <p>Supported encryption types:
 *
 * <ul>
 *   <li><b>SSE-S3</b>: Server-side encryption with Amazon S3-managed keys
 *   <li><b>SSE-KMS</b>: Server-side encryption with AWS KMS-managed keys
 * </ul>
 *
 * <p><b>Encryption Context (SSE-KMS only):</b> For SSE-KMS, you can optionally provide an
 * encryption context - a set of key-value pairs that provide additional authenticated data (AAD).
 * This allows for more granular permission control in IAM policies. See <a
 * href="https://docs.aws.amazon.com/kms/latest/developerguide/encrypt_context.html">AWS KMS
 * Encryption Context</a> for details.
 */
@Internal
public class S3EncryptionConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    /** Encryption types supported by this configuration. */
    public enum EncryptionType {
        /** No encryption. */
        NONE,
        /** Server-side encryption with Amazon S3-managed keys (SSE-S3). */
        SSE_S3,
        /** Server-side encryption with AWS KMS-managed keys (SSE-KMS). */
        SSE_KMS
    }

    private final EncryptionType encryptionType;
    @Nullable private final String kmsKeyId;

    /**
     * Optional encryption context for SSE-KMS. Provides additional authenticated data for KMS
     * encrypt/decrypt operations and can be used for fine-grained IAM policy conditions.
     */
    private final Map<String, String> encryptionContext;

    private S3EncryptionConfig(EncryptionType encryptionType, @Nullable String kmsKeyId) {
        this(encryptionType, kmsKeyId, Collections.emptyMap());
    }

    private S3EncryptionConfig(
            EncryptionType encryptionType,
            @Nullable String kmsKeyId,
            @Nullable Map<String, String> encryptionContext) {
        this.encryptionType =
                Objects.requireNonNull(encryptionType, "encryptionType must not be null");
        this.kmsKeyId = kmsKeyId;
        this.encryptionContext =
                encryptionContext != null
                        ? Collections.unmodifiableMap(new HashMap<>(encryptionContext))
                        : Collections.emptyMap();
    }

    /** Creates a config with no encryption. */
    public static S3EncryptionConfig none() {
        return new S3EncryptionConfig(EncryptionType.NONE, null);
    }

    /** Creates a config for SSE-S3 encryption (S3-managed keys). */
    public static S3EncryptionConfig sseS3() {
        return new S3EncryptionConfig(EncryptionType.SSE_S3, null);
    }

    /**
     * Creates a config for SSE-KMS encryption.
     *
     * @param kmsKeyId The KMS key ID, ARN, or alias; null uses the AWS-managed default key
     * @param encryptionContext Optional key-value pairs for IAM policy conditions and CloudTrail
     *     auditing; null or empty means no context
     * @see <a href="https://docs.aws.amazon.com/kms/latest/developerguide/encrypt_context.html">AWS
     *     KMS Encryption Context</a>
     */
    public static S3EncryptionConfig sseKms(
            @Nullable String kmsKeyId, @Nullable Map<String, String> encryptionContext) {
        return new S3EncryptionConfig(
                EncryptionType.SSE_KMS,
                StringUtils.isNullOrWhitespaceOnly(kmsKeyId) ? null : kmsKeyId,
                encryptionContext);
    }

    /**
     * Creates an encryption config from configuration strings.
     *
     * @param encryptionTypeStr The encryption type: "none", "sse-s3", "sse-kms", "aws:kms",
     *     "aes256" (case-insensitive)
     * @param kmsKeyId The KMS key ID (required for SSE-KMS, ignored for other types)
     * @return The encryption configuration
     * @throws IllegalArgumentException if the encryption type is invalid
     */
    public static S3EncryptionConfig fromConfig(
            @Nullable String encryptionTypeStr,
            @Nullable String kmsKeyId,
            Map<String, String> encryptionContext) {
        if (StringUtils.isNullOrWhitespaceOnly(encryptionTypeStr)
                || "none".equalsIgnoreCase(encryptionTypeStr)) {
            return none();
        }

        String normalizedType = encryptionTypeStr.toLowerCase(Locale.ROOT);

        switch (normalizedType) {
            case "sse-s3":
            case "aes256":
                return sseS3();
            case "sse-kms":
            case "aws:kms":
                return sseKms(kmsKeyId, encryptionContext);
            default:
                throw new IllegalArgumentException(
                        "Unknown encryption type: "
                                + encryptionTypeStr
                                + ". Supported values: none, sse-s3, sse-kms");
        }
    }

    public EncryptionType getEncryptionType() {
        return encryptionType;
    }

    @Nullable
    public String getKmsKeyId() {
        return kmsKeyId;
    }

    /**
     * Gets the encryption context for SSE-KMS.
     *
     * @return The encryption context map (never null, may be empty)
     */
    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }

    /**
     * Checks if encryption context is configured.
     *
     * @return true if encryption context has entries
     */
    public boolean hasEncryptionContext() {
        return !encryptionContext.isEmpty();
    }

    public boolean isEnabled() {
        return encryptionType != EncryptionType.NONE;
    }

    /**
     * Gets the AWS SDK ServerSideEncryption enum value for this configuration.
     *
     * @return The ServerSideEncryption value, or null if encryption is disabled
     */
    @Nullable
    public ServerSideEncryption getServerSideEncryption() {
        switch (encryptionType) {
            case SSE_S3:
                return ServerSideEncryption.AES256;
            case SSE_KMS:
                return ServerSideEncryption.AWS_KMS;
            default:
                return null;
        }
    }

    public String serializeEncryptionContext() {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : encryptionContext.entrySet()) {
            if (!first) {
                json.append(",");
            }
            json.append("\"")
                    .append(escapeJson(entry.getKey()))
                    .append("\":\"")
                    .append(escapeJson(entry.getValue()))
                    .append("\"");
            first = false;
        }
        json.append("}");
        return Base64.getEncoder().encodeToString(json.toString().getBytes(StandardCharsets.UTF_8));
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("S3EncryptionConfig{type=").append(encryptionType);
        if (kmsKeyId != null) {
            sb.append(", kmsKeyId=").append(kmsKeyId);
        }
        if (!encryptionContext.isEmpty()) {
            sb.append(", encryptionContext=").append(encryptionContext.keySet());
        }
        sb.append("}");
        return sb.toString();
    }
}
