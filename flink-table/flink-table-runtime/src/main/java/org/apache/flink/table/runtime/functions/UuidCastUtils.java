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
import org.apache.flink.table.api.TableRuntimeException;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Runtime helpers for casting to and from a {@code UUID} value.
 *
 * <p>A {@code UUID} is stored as the canonical 16-byte big-endian encoding, so casting to and from
 * {@code BINARY(16)}/{@code BYTES} is a raw byte copy, while casting to and from a character string
 * renders and parses the 8-4-4-4-12 form.
 */
@Internal
public final class UuidCastUtils {

    private static final int UUID_BYTES = 16;

    private UuidCastUtils() {}

    /**
     * Renders a {@code UUID} as its canonical lower-case 8-4-4-4-12 string. The input always holds
     * the 16-byte big-endian encoding, so this never fails.
     */
    public static String toStringValue(byte[] uuidBytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        return new UUID(buffer.getLong(), buffer.getLong()).toString();
    }

    /**
     * Parses a character string into the 16-byte big-endian encoding of a {@code UUID}, leniently,
     * matching PostgreSQL's {@code uuid} input ({@code string_to_uuid} in {@code adt/uuid.c}):
     *
     * <ul>
     *   <li>hexadecimal digits may be upper or lower case;
     *   <li>the value may be wrapped in a single pair of braces;
     *   <li>a single hyphen may follow any group of four digits, so the canonical 8-4-4-4-12
     *       hyphens may be kept, omitted, or placed between other four-digit groups.
     * </ul>
     *
     * <p>The value must contain exactly 32 hexadecimal digits. A hyphen anywhere else (leading,
     * trailing, doubled, or inside a group) fails the cast.
     */
    public static byte[] toUuidBytes(String input) {
        String value = input;
        // A value may be wrapped in a pair of braces; a leading brace requires a trailing one.
        final boolean braces = !value.isEmpty() && value.charAt(0) == '{';
        if (braces) {
            value = value.substring(1);
        }

        final byte[] result = new byte[UUID_BYTES];
        int pos = 0;
        for (int i = 0; i < UUID_BYTES; i++) {
            if (pos + 2 > value.length()) {
                throw invalidString(input);
            }
            final int high = hexDigit(value.charAt(pos));
            final int low = hexDigit(value.charAt(pos + 1));
            if (high < 0 || low < 0) {
                throw invalidString(input);
            }
            result[i] = (byte) ((high << 4) | low);
            pos += 2;
            // A single hyphen may follow any group of four digits, i.e. after every second byte
            // except the last one.
            if (i % 2 == 1
                    && i < UUID_BYTES - 1
                    && pos < value.length()
                    && value.charAt(pos) == '-') {
                pos++;
            }
        }
        if (braces && (pos >= value.length() || value.charAt(pos) != '}')) {
            throw invalidString(input);
        }
        if (pos + (braces ? 1 : 0) != value.length()) {
            throw invalidString(input);
        }
        return result;
    }

    private static int hexDigit(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    /**
     * Reinterprets a binary value as the 16-byte big-endian encoding of a {@code UUID}. The value
     * has to be exactly 16 bytes long, otherwise the cast fails.
     */
    public static byte[] fromBytes(byte[] bytes) {
        if (bytes.length != UUID_BYTES) {
            throw new TableRuntimeException(
                    String.format(
                            "Cannot cast a binary value of length %d to UUID because a UUID requires "
                                    + "exactly %d bytes.",
                            bytes.length, UUID_BYTES));
        }
        return bytes;
    }

    private static TableRuntimeException invalidString(String input) {
        return new TableRuntimeException(
                String.format(
                        "Cannot cast the string '%s' to UUID. Provide the 32 hexadecimal digits of a "
                                + "UUID, optionally using the 8-4-4-4-12 hyphenation and wrapping the "
                                + "value in braces.",
                        input));
    }
}
