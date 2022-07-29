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

package org.apache.flink.runtime.io.compression;

/** Utils for {@link BlockCompressor}. */
public class CompressorUtils {
    /**
     * We put two integers before each compressed block, the first integer represents the compressed
     * length of the block, and the second one represents the original length of the block.
     */
    public static final int HEADER_LENGTH = 8;

    public static void writeIntLE(int i, byte[] buf, int offset) {
        buf[offset++] = (byte) i;
        buf[offset++] = (byte) (i >>> 8);
        buf[offset++] = (byte) (i >>> 16);
        buf[offset] = (byte) (i >>> 24);
    }

    public static int readIntLE(byte[] buf, int i) {
        return (buf[i] & 0xFF)
                | ((buf[i + 1] & 0xFF) << 8)
                | ((buf[i + 2] & 0xFF) << 16)
                | ((buf[i + 3] & 0xFF) << 24);
    }

    public static void validateLength(int compressedLen, int originalLen)
            throws BufferDecompressionException {
        if (originalLen < 0
                || compressedLen < 0
                || (originalLen == 0 && compressedLen != 0)
                || (originalLen != 0 && compressedLen == 0)) {
            throw new BufferDecompressionException("Input is corrupted, invalid length.");
        }
    }
}
