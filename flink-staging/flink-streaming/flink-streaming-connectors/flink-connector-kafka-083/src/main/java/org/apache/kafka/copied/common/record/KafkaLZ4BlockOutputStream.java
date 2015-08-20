/**
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

package org.apache.kafka.copied.common.record;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.kafka.copied.common.utils.Utils;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A partial implementation of the v1.4.1 LZ4 Frame format.
 * 
 * @see <a href="https://docs.google.com/document/d/1Tdxmn5_2e5p1y4PtXkatLndWVb0R8QARJFe6JI4Keuo/edit">LZ4 Framing
 *      Format Spec</a>
 */
public final class KafkaLZ4BlockOutputStream extends FilterOutputStream {

    public static final int MAGIC = 0x184D2204;
    public static final int LZ4_MAX_HEADER_LENGTH = 19;
    public static final int LZ4_FRAME_INCOMPRESSIBLE_MASK = 0x80000000;

    public static final String CLOSED_STREAM = "The stream is already closed";

    public static final int BLOCKSIZE_64KB = 4;
    public static final int BLOCKSIZE_256KB = 5;
    public static final int BLOCKSIZE_1MB = 6;
    public static final int BLOCKSIZE_4MB = 7;

    private final LZ4Compressor compressor;
    private final XXHash32 checksum;
    private final FLG flg;
    private final BD bd;
    private final byte[] buffer;
    private final byte[] compressedBuffer;
    private final int maxBlockSize;
    private int bufferOffset;
    private boolean finished;

    /**
     * Create a new {@link OutputStream} that will compress data using the LZ4 algorithm.
     * 
     * @param out The output stream to compress
     * @param blockSize Default: 4. The block size used during compression. 4=64kb, 5=256kb, 6=1mb, 7=4mb. All other
     *            values will generate an exception
     * @param blockChecksum Default: false. When true, a XXHash32 checksum is computed and appended to the stream for
     *            every block of data
     * @throws IOException
     */
    public KafkaLZ4BlockOutputStream(OutputStream out, int blockSize, boolean blockChecksum) throws IOException {
        super(out);
        compressor = LZ4Factory.fastestInstance().fastCompressor();
        checksum = XXHashFactory.fastestInstance().hash32();
        bd = new BD(blockSize);
        flg = new FLG(blockChecksum);
        bufferOffset = 0;
        maxBlockSize = bd.getBlockMaximumSize();
        buffer = new byte[maxBlockSize];
        compressedBuffer = new byte[compressor.maxCompressedLength(maxBlockSize)];
        finished = false;
        writeHeader();
    }

    /**
     * Create a new {@link OutputStream} that will compress data using the LZ4 algorithm.
     * 
     * @param out The stream to compress
     * @param blockSize Default: 4. The block size used during compression. 4=64kb, 5=256kb, 6=1mb, 7=4mb. All other
     *            values will generate an exception
     * @throws IOException
     */
    public KafkaLZ4BlockOutputStream(OutputStream out, int blockSize) throws IOException {
        this(out, blockSize, false);
    }

    /**
     * Create a new {@link OutputStream} that will compress data using the LZ4 algorithm.
     * 
     * @param out The output stream to compress
     * @throws IOException
     */
    public KafkaLZ4BlockOutputStream(OutputStream out) throws IOException {
        this(out, BLOCKSIZE_64KB);
    }

    /**
     * Writes the magic number and frame descriptor to the underlying {@link OutputStream}.
     * 
     * @throws IOException
     */
    private void writeHeader() throws IOException {
        Utils.writeUnsignedIntLE(buffer, 0, MAGIC);
        bufferOffset = 4;
        buffer[bufferOffset++] = flg.toByte();
        buffer[bufferOffset++] = bd.toByte();
        // TODO write uncompressed content size, update flg.validate()
        // TODO write dictionary id, update flg.validate()
        // compute checksum on all descriptor fields
        int hash = (checksum.hash(buffer, 0, bufferOffset, 0) >> 8) & 0xFF;
        buffer[bufferOffset++] = (byte) hash;
        // write out frame descriptor
        out.write(buffer, 0, bufferOffset);
        bufferOffset = 0;
    }

    /**
     * Compresses buffered data, optionally computes an XXHash32 checksum, and writes the result to the underlying
     * {@link OutputStream}.
     * 
     * @throws IOException
     */
    private void writeBlock() throws IOException {
        if (bufferOffset == 0) {
            return;
        }

        int compressedLength = compressor.compress(buffer, 0, bufferOffset, compressedBuffer, 0);
        byte[] bufferToWrite = compressedBuffer;
        int compressMethod = 0;

        // Store block uncompressed if compressed length is greater (incompressible)
        if (compressedLength >= bufferOffset) {
            bufferToWrite = buffer;
            compressedLength = bufferOffset;
            compressMethod = LZ4_FRAME_INCOMPRESSIBLE_MASK;
        }

        // Write content
        Utils.writeUnsignedIntLE(out, compressedLength | compressMethod);
        out.write(bufferToWrite, 0, compressedLength);

        // Calculate and write block checksum
        if (flg.isBlockChecksumSet()) {
            int hash = checksum.hash(bufferToWrite, 0, compressedLength, 0);
            Utils.writeUnsignedIntLE(out, hash);
        }
        bufferOffset = 0;
    }

    /**
     * Similar to the {@link #writeBlock()} method. Writes a 0-length block (without block checksum) to signal the end
     * of the block stream.
     * 
     * @throws IOException
     */
    private void writeEndMark() throws IOException {
        Utils.writeUnsignedIntLE(out, 0);
        // TODO implement content checksum, update flg.validate()
        finished = true;
    }

    @Override
    public void write(int b) throws IOException {
        ensureNotFinished();
        if (bufferOffset == maxBlockSize) {
            writeBlock();
        }
        buffer[bufferOffset++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        net.jpountz.util.Utils.checkRange(b, off, len);
        ensureNotFinished();

        int bufferRemainingLength = maxBlockSize - bufferOffset;
        // while b will fill the buffer
        while (len > bufferRemainingLength) {
            // fill remaining space in buffer
            System.arraycopy(b, off, buffer, bufferOffset, bufferRemainingLength);
            bufferOffset = maxBlockSize;
            writeBlock();
            // compute new offset and length
            off += bufferRemainingLength;
            len -= bufferRemainingLength;
            bufferRemainingLength = maxBlockSize;
        }

        System.arraycopy(b, off, buffer, bufferOffset, len);
        bufferOffset += len;
    }

    @Override
    public void flush() throws IOException {
        if (!finished) {
            writeBlock();
        }
        if (out != null) {
            out.flush();
        }
    }

    /**
     * A simple state check to ensure the stream is still open.
     */
    private void ensureNotFinished() {
        if (finished) {
            throw new IllegalStateException(CLOSED_STREAM);
        }
    }

    @Override
    public void close() throws IOException {
        if (!finished) {
            writeEndMark();
            flush();
            finished = true;
        }
        if (out != null) {
            out.close();
            out = null;
        }
    }

    public static class FLG {

        private static final int VERSION = 1;

        private final int presetDictionary;
        private final int reserved1;
        private final int contentChecksum;
        private final int contentSize;
        private final int blockChecksum;
        private final int blockIndependence;
        private final int version;

        public FLG() {
            this(false);
        }

        public FLG(boolean blockChecksum) {
            this(0, 0, 0, 0, blockChecksum ? 1 : 0, 1, VERSION);
        }

        private FLG(int presetDictionary,
                    int reserved1,
                    int contentChecksum,
                    int contentSize,
                    int blockChecksum,
                    int blockIndependence,
                    int version) {
            this.presetDictionary = presetDictionary;
            this.reserved1 = reserved1;
            this.contentChecksum = contentChecksum;
            this.contentSize = contentSize;
            this.blockChecksum = blockChecksum;
            this.blockIndependence = blockIndependence;
            this.version = version;
            validate();
        }

        public static FLG fromByte(byte flg) {
            int presetDictionary = (flg >>> 0) & 1;
            int reserved1 = (flg >>> 1) & 1;
            int contentChecksum = (flg >>> 2) & 1;
            int contentSize = (flg >>> 3) & 1;
            int blockChecksum = (flg >>> 4) & 1;
            int blockIndependence = (flg >>> 5) & 1;
            int version = (flg >>> 6) & 3;

            return new FLG(presetDictionary,
                           reserved1,
                           contentChecksum,
                           contentSize,
                           blockChecksum,
                           blockIndependence,
                           version);
        }

        public byte toByte() {
            return (byte) (((presetDictionary & 1) << 0) | ((reserved1 & 1) << 1) | ((contentChecksum & 1) << 2)
                    | ((contentSize & 1) << 3) | ((blockChecksum & 1) << 4) | ((blockIndependence & 1) << 5) | ((version & 3) << 6));
        }

        private void validate() {
            if (presetDictionary != 0) {
                throw new RuntimeException("Preset dictionary is unsupported");
            }
            if (reserved1 != 0) {
                throw new RuntimeException("Reserved1 field must be 0");
            }
            if (contentChecksum != 0) {
                throw new RuntimeException("Content checksum is unsupported");
            }
            if (contentSize != 0) {
                throw new RuntimeException("Content size is unsupported");
            }
            if (blockIndependence != 1) {
                throw new RuntimeException("Dependent block stream is unsupported");
            }
            if (version != VERSION) {
                throw new RuntimeException(String.format("Version %d is unsupported", version));
            }
        }

        public boolean isPresetDictionarySet() {
            return presetDictionary == 1;
        }

        public boolean isContentChecksumSet() {
            return contentChecksum == 1;
        }

        public boolean isContentSizeSet() {
            return contentSize == 1;
        }

        public boolean isBlockChecksumSet() {
            return blockChecksum == 1;
        }

        public boolean isBlockIndependenceSet() {
            return blockIndependence == 1;
        }

        public int getVersion() {
            return version;
        }
    }

    public static class BD {

        private final int reserved2;
        private final int blockSizeValue;
        private final int reserved3;

        public BD() {
            this(0, BLOCKSIZE_64KB, 0);
        }

        public BD(int blockSizeValue) {
            this(0, blockSizeValue, 0);
        }

        private BD(int reserved2, int blockSizeValue, int reserved3) {
            this.reserved2 = reserved2;
            this.blockSizeValue = blockSizeValue;
            this.reserved3 = reserved3;
            validate();
        }

        public static BD fromByte(byte bd) {
            int reserved2 = (bd >>> 0) & 15;
            int blockMaximumSize = (bd >>> 4) & 7;
            int reserved3 = (bd >>> 7) & 1;

            return new BD(reserved2, blockMaximumSize, reserved3);
        }

        private void validate() {
            if (reserved2 != 0) {
                throw new RuntimeException("Reserved2 field must be 0");
            }
            if (blockSizeValue < 4 || blockSizeValue > 7) {
                throw new RuntimeException("Block size value must be between 4 and 7");
            }
            if (reserved3 != 0) {
                throw new RuntimeException("Reserved3 field must be 0");
            }
        }

        // 2^(2n+8)
        public int getBlockMaximumSize() {
            return 1 << ((2 * blockSizeValue) + 8);
        }

        public byte toByte() {
            return (byte) (((reserved2 & 15) << 0) | ((blockSizeValue & 7) << 4) | ((reserved3 & 1) << 7));
        }
    }

}
