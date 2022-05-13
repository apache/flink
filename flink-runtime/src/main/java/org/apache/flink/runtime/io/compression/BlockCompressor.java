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

/**
 * A compressor which compresses a whole byte array each time. It will read from and write to byte
 * arrays given from the outside, reducing copy time.
 */
public interface BlockCompressor {

    /** Get the max compressed size for a given original size. */
    int getMaxCompressedSize(int srcSize);

    /**
     * Compress source data read from ({@link ByteBuffer#position()} + {@code srcOff}), and write
     * the compressed data to dst.
     *
     * @param src Uncompressed data to read from
     * @param srcOff The start offset of uncompressed data
     * @param srcLen The length of data which want to be compressed
     * @param dst The target to write compressed data
     * @param dstOff The start offset to write the compressed data
     * @return Length of compressed data
     * @throws InsufficientBufferException if the target does not have sufficient space
     */
    int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff)
            throws InsufficientBufferException;

    /**
     * Compress data read from src, and write the compressed data to dst.
     *
     * @param src Uncompressed data to read from
     * @param srcOff The start offset of uncompressed data
     * @param srcLen The length of data which want to be compressed
     * @param dst The target to write compressed data
     * @param dstOff The start offset to write the compressed data
     * @return Length of compressed data
     * @throws InsufficientBufferException if the target does not have sufficient space
     */
    int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws InsufficientBufferException;
}
