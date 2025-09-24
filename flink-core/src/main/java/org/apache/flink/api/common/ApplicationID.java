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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.nio.ByteBuffer;

/** Unique (at least statistically unique) identifier for a Flink Application. */
@PublicEvolving
public final class ApplicationID extends AbstractID {

    private static final long serialVersionUID = 1L;

    /** Creates a new (statistically) random ApplicationID. */
    public ApplicationID() {
        super();
    }

    /**
     * Creates a new ApplicationID, using the given lower and upper parts.
     *
     * @param lowerPart The lower 8 bytes of the ID.
     * @param upperPart The upper 8 bytes of the ID.
     */
    public ApplicationID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    /**
     * Creates a new ApplicationID from the given byte sequence. The byte sequence must be exactly
     * 16 bytes long. The first eight bytes make up the lower part of the ID, while the next 8 bytes
     * make up the upper part of the ID.
     *
     * @param bytes The byte sequence.
     */
    public ApplicationID(byte[] bytes) {
        super(bytes);
    }

    // ------------------------------------------------------------------------
    //  Static factory methods
    // ------------------------------------------------------------------------

    /**
     * Creates a new (statistically) random ApplicationID.
     *
     * @return A new random ApplicationID.
     */
    public static ApplicationID generate() {
        return new ApplicationID();
    }

    /**
     * Creates a new ApplicationID from the given byte sequence. The byte sequence must be exactly
     * 16 bytes long. The first eight bytes make up the lower part of the ID, while the next 8 bytes
     * make up the upper part of the ID.
     *
     * @param bytes The byte sequence.
     * @return A new ApplicationID corresponding to the ID encoded in the bytes.
     */
    public static ApplicationID fromByteArray(byte[] bytes) {
        return new ApplicationID(bytes);
    }

    public static ApplicationID fromByteBuffer(ByteBuffer buf) {
        long lower = buf.getLong();
        long upper = buf.getLong();
        return new ApplicationID(lower, upper);
    }

    /**
     * Parses an ApplicationID from the given string.
     *
     * @param hexString string representation of an ApplicationID
     * @return Parsed ApplicationID
     * @throws IllegalArgumentException if the ApplicationID could not be parsed from the given
     *     string
     */
    public static ApplicationID fromHexString(String hexString) {
        try {
            return new ApplicationID(StringUtils.hexStringToByte(hexString));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Cannot parse ApplicationID from \""
                            + hexString
                            + "\". The expected format is "
                            + "[0-9a-fA-F]{32}, e.g. fd72014d4c864993a2e5a9287b4a9c5d.",
                    e);
        }
    }
}
