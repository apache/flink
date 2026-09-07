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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * The per-part checksums an S3 {@link UploadPartResponse} can carry, together with the stable wire
 * tags used by {@link S3RecoverableSerializer}.
 *
 * <p>This enum is the single source of truth both for propagating checksums from {@link
 * UploadPartResponse} to {@link CompletedPart} and for (de)serializing them in recoverable state,
 * so the two sets can never drift apart. Adding a checksum field introduced by a newer AWS SDK only
 * requires a new constant. Wire tags are persisted in checkpointed state and must never be
 * renumbered or reused.
 */
@Internal
enum S3PartChecksum {
    CRC32(
            (byte) 1,
            UploadPartResponse::checksumCRC32,
            CompletedPart::checksumCRC32,
            CompletedPart.Builder::checksumCRC32),
    CRC32C(
            (byte) 2,
            UploadPartResponse::checksumCRC32C,
            CompletedPart::checksumCRC32C,
            CompletedPart.Builder::checksumCRC32C),
    CRC64NVME(
            (byte) 3,
            UploadPartResponse::checksumCRC64NVME,
            CompletedPart::checksumCRC64NVME,
            CompletedPart.Builder::checksumCRC64NVME),
    SHA1(
            (byte) 4,
            UploadPartResponse::checksumSHA1,
            CompletedPart::checksumSHA1,
            CompletedPart.Builder::checksumSHA1),
    SHA256(
            (byte) 5,
            UploadPartResponse::checksumSHA256,
            CompletedPart::checksumSHA256,
            CompletedPart.Builder::checksumSHA256),
    SHA512(
            (byte) 6,
            UploadPartResponse::checksumSHA512,
            CompletedPart::checksumSHA512,
            CompletedPart.Builder::checksumSHA512),
    MD5(
            (byte) 7,
            UploadPartResponse::checksumMD5,
            CompletedPart::checksumMD5,
            CompletedPart.Builder::checksumMD5),
    XXHASH64(
            (byte) 8,
            UploadPartResponse::checksumXXHASH64,
            CompletedPart::checksumXXHASH64,
            CompletedPart.Builder::checksumXXHASH64),
    XXHASH3(
            (byte) 9,
            UploadPartResponse::checksumXXHASH3,
            CompletedPart::checksumXXHASH3,
            CompletedPart.Builder::checksumXXHASH3),
    XXHASH128(
            (byte) 10,
            UploadPartResponse::checksumXXHASH128,
            CompletedPart::checksumXXHASH128,
            CompletedPart.Builder::checksumXXHASH128);

    private final byte wireTag;

    private final Function<UploadPartResponse, String> responseGetter;

    private final Function<CompletedPart, String> partGetter;

    private final BiConsumer<CompletedPart.Builder, String> builderSetter;

    S3PartChecksum(
            byte wireTag,
            Function<UploadPartResponse, String> responseGetter,
            Function<CompletedPart, String> partGetter,
            BiConsumer<CompletedPart.Builder, String> builderSetter) {
        this.wireTag = wireTag;
        this.responseGetter = responseGetter;
        this.partGetter = partGetter;
        this.builderSetter = builderSetter;
    }

    byte getWireTag() {
        return wireTag;
    }

    @Nullable
    String valueOf(UploadPartResponse response) {
        return responseGetter.apply(response);
    }

    @Nullable
    String valueOf(CompletedPart part) {
        return partGetter.apply(part);
    }

    void applyTo(CompletedPart.Builder builder, String value) {
        builderSetter.accept(builder, value);
    }

    static S3PartChecksum fromWireTag(byte wireTag) throws IOException {
        for (S3PartChecksum checksum : values()) {
            if (checksum.wireTag == wireTag) {
                return checksum;
            }
        }
        throw new IOException("Corrupt data: Unknown part checksum tag " + wireTag);
    }
}
