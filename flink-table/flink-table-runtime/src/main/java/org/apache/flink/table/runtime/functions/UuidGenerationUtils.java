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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.UuidType;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Runtime helpers for generating {@code UUID} values, stored as their 16-byte big-endian encoding.
 */
@Internal
public final class UuidGenerationUtils {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private UuidGenerationUtils() {}

    /**
     * Generates a random RFC 9562 version 4 {@code UUID}.
     *
     * <p>Fills the bytes directly rather than via {@link UUID#randomUUID()}. The internal
     * representation is already a {@code byte[]}, so this avoids an extra round-trip through a
     * {@link UUID} object.
     */
    public static byte[] generateV4() {
        final byte[] bytes = new byte[UuidType.BYTE_LENGTH];
        SECURE_RANDOM.nextBytes(bytes);

        // set the version to 4
        bytes[6] &= 0x0F;
        bytes[6] |= 0x40;

        // set the variant to IETF
        bytes[8] &= 0x3F;
        bytes[8] |= (byte) 0x80;

        return bytes;
    }

    /**
     * Generates a time-ordered RFC 9562 version 7 {@code UUID}, consisting of a 48-bit big-endian
     * Unix epoch millisecond timestamp followed by 74 bits of randomness, with the version and
     * variant bits set accordingly.
     *
     * <p>Implements the layout from the RFC directly rather than delegating to the JDK, since a
     * built-in generator for this ({@code UUID.ofEpochMillis}) only exists starting with JDK 26,
     * newer than what Flink's minimum supported Java version provides.
     */
    public static byte[] generateV7() {
        final long timestamp = System.currentTimeMillis();
        final byte[] bytes = new byte[UuidType.BYTE_LENGTH];
        SECURE_RANDOM.nextBytes(bytes);

        // embed the timestamp into the first 6 bytes
        bytes[0] = (byte) (timestamp >>> 40);
        bytes[1] = (byte) (timestamp >>> 32);
        bytes[2] = (byte) (timestamp >>> 24);
        bytes[3] = (byte) (timestamp >>> 16);
        bytes[4] = (byte) (timestamp >>> 8);
        bytes[5] = (byte) timestamp;

        // set the version to 7
        bytes[6] &= 0x0F;
        bytes[6] |= 0x70;

        // set the variant to IETF
        bytes[8] &= 0x3F;
        bytes[8] |= (byte) 0x80;

        return bytes;
    }
}
