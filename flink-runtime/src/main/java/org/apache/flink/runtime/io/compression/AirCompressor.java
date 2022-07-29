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

import io.airlift.compress.Compressor;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.runtime.io.compression.CompressorUtils.writeIntLE;
import static org.apache.flink.runtime.io.compression.Lz4BlockCompressionFactory.HEADER_LENGTH;

/** Flink compressor that wrap {@link Compressor}. */
public class AirCompressor implements BlockCompressor {
    Compressor internalCompressor;

    public AirCompressor(Compressor internalCompressor) {
        this.internalCompressor = internalCompressor;
    }

    @Override
    public int getMaxCompressedSize(int srcSize) {
        return AirCompressorFactory.HEADER_LENGTH + internalCompressor.maxCompressedLength(srcSize);
    }

    @Override
    public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff)
            throws InsufficientBufferException {
        try {
            final int prevSrcOff = src.position() + srcOff;
            final int prevDstOff = dst.position() + dstOff;

            src.position(prevSrcOff);
            dst.position(prevDstOff + HEADER_LENGTH);

            internalCompressor.compress(src, dst);

            int compressedLength = dst.position() - prevDstOff - HEADER_LENGTH;

            dst.position(prevDstOff);
            dst.order(ByteOrder.LITTLE_ENDIAN);
            dst.putInt(compressedLength);
            dst.putInt(srcLen);
            dst.position(prevDstOff + compressedLength + HEADER_LENGTH);

            return HEADER_LENGTH + compressedLength;
        } catch (IllegalArgumentException
                | ArrayIndexOutOfBoundsException
                | BufferOverflowException e) {
            if (e instanceof IllegalArgumentException
                    && !isInsufficientBuffer((IllegalArgumentException) e)) {
                throw e;
            }
            throw new InsufficientBufferException(e);
        }
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws InsufficientBufferException {
        try {
            int maxCompressedLength = internalCompressor.maxCompressedLength(srcLen);

            if (dst.length < dstOff + maxCompressedLength - 1) {
                throw new ArrayIndexOutOfBoundsException();
            }

            int compressedLength =
                    internalCompressor.compress(
                            src,
                            srcOff,
                            srcLen,
                            dst,
                            dstOff + HEADER_LENGTH,
                            internalCompressor.maxCompressedLength(srcLen));
            writeIntLE(compressedLength, dst, dstOff);
            writeIntLE(srcLen, dst, dstOff + 4);
            return HEADER_LENGTH + compressedLength;
        } catch (IllegalArgumentException
                | BufferOverflowException
                | ArrayIndexOutOfBoundsException e) {
            if (e instanceof IllegalArgumentException
                    && !isInsufficientBuffer((IllegalArgumentException) e)) {
                throw e;
            }
            throw new InsufficientBufferException(e);
        }
    }

    public static boolean isInsufficientBuffer(IllegalArgumentException e) {
        String message = e.getMessage();
        String zStdMessage = "Output buffer too small";
        String snappyMessage = "Output buffer must be at least";
        String lzMessage = "Max output length must be larger than";

        return message.contains(zStdMessage)
                || message.contains(snappyMessage)
                || message.contains(lzMessage);
    }
}
