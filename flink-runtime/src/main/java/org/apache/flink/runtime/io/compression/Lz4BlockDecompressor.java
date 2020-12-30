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

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.SafeUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.runtime.io.compression.Lz4BlockCompressionFactory.HEADER_LENGTH;

/**
 * Decode data written with {@link Lz4BlockCompressor}. It reads from and writes to byte arrays
 * provided from the outside, thus reducing copy time.
 *
 * <p>This class is copied and modified from {@link net.jpountz.lz4.LZ4BlockInputStream}.
 */
public class Lz4BlockDecompressor implements BlockDecompressor {

    private final LZ4FastDecompressor decompressor;

    public Lz4BlockDecompressor() {
        this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff)
            throws DataCorruptionException {
        final int prevSrcOff = src.position() + srcOff;
        final int prevDstOff = dst.position() + dstOff;

        src.order(ByteOrder.LITTLE_ENDIAN);
        final int compressedLen = src.getInt(prevSrcOff);
        final int originalLen = src.getInt(prevSrcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.capacity() - prevDstOff < originalLen) {
            throw new InsufficientBufferException("Buffer length too small");
        }

        if (src.limit() - prevSrcOff - HEADER_LENGTH < compressedLen) {
            throw new DataCorruptionException("Source data is not integral for decompression.");
        }

        try {
            final int compressedLen2 =
                    decompressor.decompress(
                            src, prevSrcOff + HEADER_LENGTH, dst, prevDstOff, originalLen);
            if (compressedLen != compressedLen2) {
                throw new DataCorruptionException(
                        "Input is corrupted, unexpected compressed length.");
            }
            src.position(prevSrcOff + compressedLen + HEADER_LENGTH);
            dst.position(prevDstOff + originalLen);
        } catch (LZ4Exception e) {
            throw new DataCorruptionException("Input is corrupted", e);
        }

        return originalLen;
    }

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws InsufficientBufferException, DataCorruptionException {
        final int compressedLen = SafeUtils.readIntLE(src, srcOff);
        final int originalLen = SafeUtils.readIntLE(src, srcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.length - dstOff < originalLen) {
            throw new InsufficientBufferException("Buffer length too small");
        }

        if (src.length - srcOff - HEADER_LENGTH < compressedLen) {
            throw new DataCorruptionException("Source data is not integral for decompression.");
        }

        try {
            final int compressedLen2 =
                    decompressor.decompress(src, srcOff + HEADER_LENGTH, dst, dstOff, originalLen);
            if (compressedLen != compressedLen2) {
                throw new DataCorruptionException("Input is corrupted");
            }
        } catch (LZ4Exception e) {
            throw new DataCorruptionException("Input is corrupted", e);
        }

        return originalLen;
    }

    private void validateLength(int compressedLen, int originalLen) throws DataCorruptionException {
        if (originalLen < 0
                || compressedLen < 0
                || (originalLen == 0 && compressedLen != 0)
                || (originalLen != 0 && compressedLen == 0)) {
            throw new DataCorruptionException("Input is corrupted, invalid length.");
        }
    }
}
