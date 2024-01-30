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

package org.apache.flink.fs.gs.utils;

import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

/** Utility class related to checksums. */
public class ChecksumUtils {

    /** THe crc hash function used by Google storage. */
    public static final HashFunction CRC_HASH_FUNCTION = Hashing.crc32c();

    /** The encoder use to construct string checksums, as done by Google storage. */
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    /**
     * Converts an int crc32 checksum to the string format used by Google storage, which is the
     * base64 string for the int in big-endian format.
     *
     * @param checksum The int checksum
     * @return The string checksum
     */
    public static String convertChecksumToString(int checksum) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(checksum);
        return BASE64_ENCODER.encodeToString(buffer.array());
    }
}
