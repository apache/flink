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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.UuidType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Helpers to convert between a {@link UUID} and the canonical 16-byte big-endian encoding used to
 * store a {@link UuidType} value: the most significant 64 bits followed by the least significant 64
 * bits.
 */
@Internal
public final class UuidUtils {

    private UuidUtils() {}

    /** Encodes a {@link UUID} into its canonical 16-byte big-endian representation. */
    public static byte[] toBytes(UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.allocate(UuidType.BYTE_LENGTH);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

    /** Decodes the canonical 16-byte big-endian representation into a {@link UUID}. */
    public static UUID fromBytes(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.BIG_ENDIAN);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
