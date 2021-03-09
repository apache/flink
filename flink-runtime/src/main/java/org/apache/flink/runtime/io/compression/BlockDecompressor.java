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

import java.nio.ByteBuffer;

/** A decompressor which decompresses a block each time. */
public interface BlockDecompressor {

    /**
     * Decompress source data read from ({@link ByteBuffer#position()} + {@code srcOff}), and write
     * the decompressed data to dst.
     *
     * @param src Compressed data to read from
     * @param srcOff The start offset of compressed data
     * @param srcLen The length of data which want to be decompressed
     * @param dst The target to write decompressed data
     * @param dstOff The start offset to write the decompressed data
     * @return Length of decompressed data
     * @throws DataCorruptionException if data corruption found when decompressing
     * @throws InsufficientBufferException if the target does not have sufficient space
     */
    int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff)
            throws DataCorruptionException, InsufficientBufferException;

    /**
     * Decompress source data read from src and write the decompressed data to dst.
     *
     * @param src Compressed data to read from
     * @param srcOff The start offset of compressed data
     * @param srcLen The length of data which want to be decompressed
     * @param dst The target to write decompressed data
     * @param dstOff The start offset to write the decompressed data
     * @return Length of decompressed data
     * @throws DataCorruptionException if data corruption found when decompressing
     * @throws InsufficientBufferException if the target does not have sufficient space
     */
    int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws DataCorruptionException, InsufficientBufferException;
}
