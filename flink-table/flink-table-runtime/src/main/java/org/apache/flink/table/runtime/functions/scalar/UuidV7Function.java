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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.logical.UuidType;

import java.security.SecureRandom;

/** Implementation of {@link BuiltInFunctionDefinitions#UUID_V7}. */
@Internal
public class UuidV7Function extends BuiltInScalarFunction {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    public UuidV7Function(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.UUID_V7, context);
    }

    public byte[] eval() {
        return generate();
    }

    @VisibleForTesting
    static byte[] generate() {
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
