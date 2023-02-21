/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableIntVector;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Run length decoder for data and dictionary ids. See
 * https://github.com/apache/parquet-format/blob/master/Encodings.md See {@link
 * RunLengthBitPackingHybridDecoder}.
 */
final class RunLengthDecoder {

    /**
     * If true, the bit width is fixed. This decoder is used in different places and this also
     * controls if we need to read the bitwidth from the beginning of the data stream.
     */
    private final boolean fixedWidth;

    private final boolean readLength;

    // Encoded data.
    private ByteBufferInputStream in;

    // bit/byte width of decoded data and utility to batch unpack them.
    private int bitWidth;
    private int bytesWidth;
    private BytePacker packer;

    // Current decoding mode and values
    MODE mode;
    int currentCount;
    int currentValue;

    // Buffer of decoded values if the values are PACKED.
    int[] currentBuffer = new int[16];
    int currentBufferIdx = 0;

    RunLengthDecoder() {
        this.fixedWidth = false;
        this.readLength = false;
    }

    RunLengthDecoder(int bitWidth) {
        this.fixedWidth = true;
        this.readLength = bitWidth != 0;
        initWidthAndPacker(bitWidth);
    }

    RunLengthDecoder(int bitWidth, boolean readLength) {
        this.fixedWidth = true;
        this.readLength = readLength;
        initWidthAndPacker(bitWidth);
    }

    /** Init from input stream. */
    void initFromStream(int valueCount, ByteBufferInputStream in) throws IOException {
        this.in = in;
        if (fixedWidth) {
            // initialize for repetition and definition levels
            if (readLength) {
                int length = readIntLittleEndian();
                this.in = in.sliceStream(length);
            }
        } else {
            // initialize for values
            if (in.available() > 0) {
                initWidthAndPacker(in.read());
            }
        }
        if (bitWidth == 0) {
            // 0 bit width, treat this as an RLE run of valueCount number of 0's.
            this.mode = MODE.RLE;
            this.currentCount = valueCount;
            this.currentValue = 0;
        } else {
            this.currentCount = 0;
        }
    }

    /** Initializes the internal state for decoding ints of `bitWidth`. */
    private void initWidthAndPacker(int bitWidth) {
        Preconditions.checkArgument(
                bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
        this.bitWidth = bitWidth;
        this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
        this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    }

    int readInteger() {
        if (this.currentCount == 0) {
            this.readNextGroup();
        }

        this.currentCount--;
        switch (mode) {
            case RLE:
                return this.currentValue;
            case PACKED:
                return this.currentBuffer[currentBufferIdx++];
        }
        throw new RuntimeException("Unreachable");
    }

    /**
     * Decoding for dictionary ids. The IDs are populated into `values` and the nullability is
     * populated into `nulls`.
     */
    void readDictionaryIds(
            int total,
            WritableIntVector values,
            WritableColumnVector nulls,
            int rowId,
            int level,
            RunLengthDecoder data) {
        int left = total;
        while (left > 0) {
            if (this.currentCount == 0) {
                this.readNextGroup();
            }
            int n = Math.min(left, this.currentCount);
            switch (mode) {
                case RLE:
                    if (currentValue == level) {
                        data.readDictionaryIdData(n, values, rowId);
                    } else {
                        nulls.setNulls(rowId, n);
                    }
                    break;
                case PACKED:
                    for (int i = 0; i < n; ++i) {
                        if (currentBuffer[currentBufferIdx++] == level) {
                            values.setInt(rowId + i, data.readInteger());
                        } else {
                            nulls.setNullAt(rowId + i);
                        }
                    }
                    break;
            }
            rowId += n;
            left -= n;
            currentCount -= n;
        }
    }

    /** It is used to decode dictionary IDs. */
    private void readDictionaryIdData(int total, WritableIntVector c, int rowId) {
        int left = total;
        while (left > 0) {
            if (this.currentCount == 0) {
                this.readNextGroup();
            }
            int n = Math.min(left, this.currentCount);
            switch (mode) {
                case RLE:
                    c.setInts(rowId, n, currentValue);
                    break;
                case PACKED:
                    c.setInts(rowId, n, currentBuffer, currentBufferIdx);
                    currentBufferIdx += n;
                    break;
            }
            rowId += n;
            left -= n;
            currentCount -= n;
        }
    }

    /** Reads the next varint encoded int. */
    private int readUnsignedVarInt() throws IOException {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = in.read();
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    /** Reads the next 4 byte little endian int. */
    private int readIntLittleEndian() throws IOException {
        int ch4 = in.read();
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    /** Reads the next byteWidth little endian int. */
    private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
        switch (bytesWidth) {
            case 0:
                return 0;
            case 1:
                return in.read();
            case 2:
                {
                    int ch2 = in.read();
                    int ch1 = in.read();
                    return (ch1 << 8) + ch2;
                }
            case 3:
                {
                    int ch3 = in.read();
                    int ch2 = in.read();
                    int ch1 = in.read();
                    return (ch1 << 16) + (ch2 << 8) + ch3;
                }
            case 4:
                {
                    return readIntLittleEndian();
                }
        }
        throw new RuntimeException("Unreachable");
    }

    /** Reads the next group. */
    void readNextGroup() {
        try {
            int header = readUnsignedVarInt();
            this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
            switch (mode) {
                case RLE:
                    this.currentCount = header >>> 1;
                    this.currentValue = readIntLittleEndianPaddedOnBitWidth();
                    return;
                case PACKED:
                    int numGroups = header >>> 1;
                    this.currentCount = numGroups * 8;

                    if (this.currentBuffer.length < this.currentCount) {
                        this.currentBuffer = new int[this.currentCount];
                    }
                    currentBufferIdx = 0;
                    int valueIndex = 0;
                    while (valueIndex < this.currentCount) {
                        // values are bit packed 8 at a time, so reading bitWidth will always work
                        ByteBuffer buffer = in.slice(bitWidth);
                        if (buffer.hasArray()) {
                            // byte array has better performance than ByteBuffer
                            this.packer.unpack8Values(
                                    buffer.array(),
                                    buffer.arrayOffset() + buffer.position(),
                                    this.currentBuffer,
                                    valueIndex);
                        } else {
                            this.packer.unpack8Values(
                                    buffer, buffer.position(), this.currentBuffer, valueIndex);
                        }
                        valueIndex += 8;
                    }
                    return;
                default:
                    throw new ParquetDecodingException("not a valid mode " + this.mode);
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to read from input stream", e);
        }
    }

    enum MODE {
        RLE,
        PACKED
    }
}
