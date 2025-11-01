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

import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
            Map<String, String> encryptionContext) {
        this.encryptionType = encryptionType;
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
     * Creates a config for SSE-KMS encryption with the default KMS key.
     *
     * <p>Uses the AWS-managed KMS key (aws/s3) for the S3 bucket.
     */
    public static S3EncryptionConfig sseKms() {
        return new S3EncryptionConfig(EncryptionType.SSE_KMS, null);
    }

    /**
     * Creates a config for SSE-KMS encryption with a specific KMS key.
     *
     * @param kmsKeyId The KMS key ID, ARN, or alias (e.g., "arn:aws:kms:region:account:key/key-id"
     *     or "alias/my-key")
     */
    public static S3EncryptionConfig sseKms(String kmsKeyId) {
        return new S3EncryptionConfig(EncryptionType.SSE_KMS, kmsKeyId);
    }

    /**
     * Creates a config for SSE-KMS encryption with a specific KMS key and encryption context.
     *
     * <p>The encryption context is a set of key-value pairs that:
     *
     * <ul>
     *   <li>Provides additional authenticated data (AAD) for encryption
     *   <li>Can be used in IAM policy conditions for fine-grained access control
     *   <li>Is logged in AWS CloudTrail for auditing
     * </ul>
     *
     * <p>Example: You might include context like {"department": "finance", "project": "budget"} to
     * restrict which principals can encrypt/decrypt based on these values.
     *
     * @param kmsKeyId The KMS key ID, ARN, or alias
     * @param encryptionContext The encryption context key-value pairs
     * @see <a href="https://docs.aws.amazon.com/kms/latest/developerguide/encrypt_context.html">AWS
     *     KMS Encryption Context</a>
     */
    public static S3EncryptionConfig sseKms(
            String kmsKeyId, Map<String, String> encryptionContext) {
        return new S3EncryptionConfig(EncryptionType.SSE_KMS, kmsKeyId, encryptionContext);
    }

    /**
     * Creates an encryption config from configuration strings.
     *
     * @param encryptionTypeStr The encryption type: "none", "sse-s3", "sse-kms", or "SSE_S3",
     *     "SSE_KMS"
     * @param kmsKeyId The KMS key ID (required for SSE-KMS, ignored for other types)
     * @return The encryption configuration
     * @throws IllegalArgumentException if the encryption type is invalid
     */
    public static S3EncryptionConfig fromConfig(
            @Nullable String encryptionTypeStr, @Nullable String kmsKeyId) {
        if (encryptionTypeStr == null
                || encryptionTypeStr.isEmpty()
                || "none".equalsIgnoreCase(encryptionTypeStr)) {
            return none();
        }

        String normalizedType = encryptionTypeStr.toUpperCase().replace("-", "_");

        switch (normalizedType) {
            case "SSE_S3":
            case "AES256":
                return sseS3();
            case "SSE_KMS":
            case "AWS_KMS":
                return kmsKeyId != null && !kmsKeyId.isEmpty() ? sseKms(kmsKeyId) : sseKms();
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
