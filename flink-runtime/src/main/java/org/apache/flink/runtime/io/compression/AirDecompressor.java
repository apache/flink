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

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.runtime.io.compression.CompressorUtils.readIntLE;
import static org.apache.flink.runtime.io.compression.CompressorUtils.validateLength;
import static org.apache.flink.runtime.io.compression.Lz4BlockCompressionFactory.HEADER_LENGTH;

/** Flink decompressor that wrap {@link io.airlift.compress.Decompressor}. */
public class AirDecompressor implements BlockDecompressor {
    Decompressor internalDecompressor;

    public AirDecompressor(Decompressor internalDecompressor) {
        this.internalDecompressor = internalDecompressor;
    }

    @Override
    public int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff)
            throws DataCorruptionException, InsufficientBufferException {
        final int prevSrcOff = src.position() + srcOff;
        final int prevDstOff = dst.position() + dstOff;

        src.position(prevSrcOff);
        dst.position(prevDstOff);
        src.order(ByteOrder.LITTLE_ENDIAN);
        final int compressedLen = src.getInt();
        final int originalLen = src.getInt();
        validateLength(compressedLen, originalLen);

        if (dst.capacity() - prevDstOff < originalLen) {
            throw new InsufficientBufferException("Buffer length too small");
        }

        if (src.limit() - prevSrcOff - HEADER_LENGTH < compressedLen) {
            throw new DataCorruptionException("Source data is not integral for decompression.");
        }
        src.limit(prevSrcOff + compressedLen + HEADER_LENGTH);
        try {
            internalDecompressor.decompress(src, dst);
            int originalLen2 = dst.position() - prevDstOff;
            if (originalLen != originalLen2) {
                throw new DataCorruptionException(
                        "Input is corrupted, unexpected original length.");
            }
        } catch (MalformedInputException e) {
            throw new DataCorruptionException("Input is corrupted", e);
        }

        return originalLen;
    }

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws DataCorruptionException, InsufficientBufferException {
        int compressedLen = readIntLE(src, srcOff);
        int originalLen = readIntLE(src, srcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.length - dstOff < originalLen) {
            throw new InsufficientBufferException("Buffer length too small");
        }

        if (src.length - srcOff - HEADER_LENGTH < compressedLen) {
            throw new DataCorruptionException("Source data is not integral for decompression.");
        }

        try {
            final int originalLen2 =
                    internalDecompressor.decompress(
                            src, srcOff + HEADER_LENGTH, compressedLen, dst, dstOff, originalLen);
            if (originalLen != originalLen2) {
                throw new DataCorruptionException("Input is corrupted");
            }
        } catch (MalformedInputException e) {
            throw new DataCorruptionException("Input is corrupted", e);
        }

        return originalLen;
    }
}
