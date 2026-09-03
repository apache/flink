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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.UuidType;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Converter for {@link UuidType} of {@link UUID} external type.
 *
 * <p>The internal representation is the canonical 16-byte big-endian encoding: the most significant
 * 64 bits followed by the least significant 64 bits.
 */
@Internal
public class UuidUuidConverter implements DataStructureConverter<byte[], UUID> {

    private static final long serialVersionUID = 1L;

    @Override
    public byte[] toInternal(UUID external) {
        return ByteBuffer.allocate(16)
                .putLong(external.getMostSignificantBits())
                .putLong(external.getLeastSignificantBits())
                .array();
    }

    @Override
    public UUID toExternal(byte[] internal) {
        final ByteBuffer buffer = ByteBuffer.wrap(internal);
        final long mostSignificantBits = buffer.getLong();
        final long leastSignificantBits = buffer.getLong();
        return new UUID(mostSignificantBits, leastSignificantBits);
    }
}
