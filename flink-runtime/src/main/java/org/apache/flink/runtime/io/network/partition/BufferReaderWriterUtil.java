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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FileRegionBuffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Putting and getting of a sequence of buffers to/from a FileChannel or a ByteBuffer. This class
 * handles the headers, length encoding, memory slicing.
 *
 * <p>The encoding is the same across FileChannel and ByteBuffer, so this class can write to a file
 * and read from the byte buffer that results from mapping this file to memory.
 */
public final class BufferReaderWriterUtil {

    public static final int HEADER_LENGTH = 8;

    private static final short HEADER_VALUE_IS_BUFFER = 0;

    private static final short HEADER_VALUE_IS_EVENT = 1;

    private static final short HEADER_VALUE_IS_SEGMENT_EVENT = 2;

    private static final short BUFFER_IS_COMPRESSED = 1;

    private static final short BUFFER_IS_NOT_COMPRESSED = 0;

    // ------------------------------------------------------------------------
    //  ByteBuffer read / write
    // ------------------------------------------------------------------------

    static boolean writeBuffer(Buffer buffer, ByteBuffer memory) {
        final int bufferSize = buffer.getSize();

        if (memory.remaining() < bufferSize + HEADER_LENGTH) {
            return false;
        }

        memory.putShort(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
        memory.putShort(buffer.isCompressed() ? BUFFER_IS_COMPRESSED : BUFFER_IS_NOT_COMPRESSED);
        memory.putInt(bufferSize);
        memory.put(buffer.getNioBufferReadable());
        return true;
    }

    @Nullable
    static Buffer sliceNextBuffer(ByteBuffer memory) {
        final int remaining = memory.remaining();

        // we only check the correct case where data is exhausted
        // all other cases can only occur if our write logic is wrong and will already throw
        // buffer underflow exceptions which will cause the read to fail.
        if (remaining == 0) {
            return null;
        }

        final BufferHeader header = parseBufferHeader(memory);

        memory.limit(memory.position() + header.getLength());
        ByteBuffer buf = memory.slice();
        memory.position(memory.limit());
        memory.limit(memory.capacity());

        MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(buf);
        return new NetworkBuffer(
                memorySegment,
                FreeingBufferRecycler.INSTANCE,
                header.getDataType(),
                header.isCompressed(),
                header.getLength());
    }

    // ------------------------------------------------------------------------
    //  ByteChannel read / write
    // ------------------------------------------------------------------------

    static long writeToByteChannel(
            FileChannel channel, Buffer buffer, ByteBuffer[] arrayWithHeaderBuffer)
            throws IOException {

        final ByteBuffer headerBuffer = arrayWithHeaderBuffer[0];
        setByteChannelBufferHeader(buffer, headerBuffer);

        final ByteBuffer dataBuffer = buffer.getNioBufferReadable();
        arrayWithHeaderBuffer[1] = dataBuffer;

        final long bytesExpected = HEADER_LENGTH + dataBuffer.remaining();

        writeBuffers(channel, bytesExpected, arrayWithHeaderBuffer);
        return bytesExpected;
    }

    static long writeToByteChannelIfBelowSize(
            FileChannel channel, Buffer buffer, ByteBuffer[] arrayWithHeaderBuffer, long bytesLeft)
            throws IOException {

        if (bytesLeft >= HEADER_LENGTH + buffer.getSize()) {
            return writeToByteChannel(channel, buffer, arrayWithHeaderBuffer);
        }

        return -1L;
    }

    public static void setByteChannelBufferHeader(Buffer buffer, ByteBuffer header) {
        header.clear();
        header.putShort(generateDataTypeHeader(buffer));
        header.putShort(buffer.isCompressed() ? BUFFER_IS_COMPRESSED : BUFFER_IS_NOT_COMPRESSED);
        header.putInt(buffer.getSize());
        header.flip();
    }

    private static short generateDataTypeHeader(Buffer buffer) {
        Buffer.DataType dataType = buffer.getDataType();
        if (dataType == Buffer.DataType.DATA_BUFFER) {
            return HEADER_VALUE_IS_BUFFER;
        } else if (dataType == Buffer.DataType.END_OF_SEGMENT) {
            return HEADER_VALUE_IS_SEGMENT_EVENT;
        } else if (dataType == Buffer.DataType.EVENT_BUFFER) {
            return HEADER_VALUE_IS_EVENT;
        } else {
            throw new RuntimeException("Generating DataType failed, DataType is: " + dataType);
        }
    }

    @Nullable
    static Buffer readFileRegionFromByteChannel(FileChannel channel, ByteBuffer headerBuffer)
            throws IOException {
        headerBuffer.clear();
        if (!tryReadByteBuffer(channel, headerBuffer)) {
            return null;
        }
        headerBuffer.flip();

        final BufferHeader header = parseBufferHeader(headerBuffer);

        // the file region does not advance position. it must not, because it gets written
        // interleaved with these calls, which would completely mess up the reading.
        // so we advance the positions always and only here.
        final long position = channel.position();
        channel.position(position + header.getLength());

        return new FileRegionBuffer(
                channel, position, header.getLength(), header.getDataType(), header.isCompressed());
    }

    @Nullable
    public static Buffer readFromByteChannel(
            FileChannel channel,
            ByteBuffer headerBuffer,
            MemorySegment memorySegment,
            BufferRecycler bufferRecycler)
            throws IOException {

        headerBuffer.clear();
        if (!tryReadByteBuffer(channel, headerBuffer)) {
            return null;
        }
        headerBuffer.flip();

        final ByteBuffer targetBuf;
        final BufferHeader header;

        try {
            header = parseBufferHeader(headerBuffer);
            targetBuf = memorySegment.wrap(0, header.getLength());
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            // buffer underflow if header buffer is undersized
            // IllegalArgumentException if size is outside memory segment size
            throwCorruptDataException();
            return null; // silence compiler
        }

        readByteBufferFully(channel, targetBuf);

        Buffer.DataType dataType = header.getDataType();
        return new NetworkBuffer(
                memorySegment, bufferRecycler, dataType, header.isCompressed(), header.getLength());
    }

    public static ByteBuffer allocatedHeaderBuffer() {
        ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
        configureByteBuffer(bb);
        return bb;
    }

    /** Skip one data buffer from the channel's current position by headerBuffer. */
    public static void positionToNextBuffer(FileChannel channel, ByteBuffer headerBuffer)
            throws IOException {
        headerBuffer.clear();
        if (!tryReadByteBuffer(channel, headerBuffer)) {
            throwCorruptDataException();
        }
        headerBuffer.flip();

        try {
            headerBuffer.getShort();
            headerBuffer.getShort();
            long bufferSize = headerBuffer.getInt();
            channel.position(channel.position() + bufferSize);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            // buffer underflow if header buffer is undersized
            // IllegalArgumentException if size is outside memory segment size
            throwCorruptDataException();
        }
    }

    static ByteBuffer[] allocatedWriteBufferArray() {
        return new ByteBuffer[] {allocatedHeaderBuffer(), null};
    }

    private static boolean tryReadByteBuffer(FileChannel channel, ByteBuffer b) throws IOException {
        if (channel.read(b) == -1) {
            return false;
        } else {
            while (b.hasRemaining()) {
                if (channel.read(b) == -1) {
                    throwPrematureEndOfFile();
                }
            }
            return true;
        }
    }

    public static void readByteBufferFully(FileChannel channel, ByteBuffer b) throws IOException {
        // the post-checked loop here gets away with one less check in the normal case
        do {
            if (channel.read(b) == -1) {
                throwPrematureEndOfFile();
            }
        } while (b.hasRemaining());
    }

    public static void readByteBufferFully(
            final FileChannel channel, final ByteBuffer b, long position) throws IOException {

        // the post-checked loop here gets away with one less check in the normal case
        do {
            final int numRead = channel.read(b, position);
            if (numRead == -1) {
                throwPrematureEndOfFile();
            }
            position += numRead;
        } while (b.hasRemaining());
    }

    static void writeBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    public static void writeBuffers(FileChannel channel, long bytesExpected, ByteBuffer... buffers)
            throws IOException {
        // The FileChannel#write method relies on the writev system call for data writing on linux.
        // The writev system call has a limit on the maximum number of buffers can be written in one
        // invoke whose advertised value is 1024 (see writev man page for more information), which
        // means if more than 1024 buffers is written in one invoke, it is not guaranteed that all
        // bytes can be written, so we build this safety net.
        if (bytesExpected > channel.write(buffers)) {
            for (ByteBuffer buffer : buffers) {
                writeBuffer(channel, buffer);
            }
        }
    }

    public static BufferHeader parseBufferHeader(ByteBuffer headerBuffer) {
        configureByteBuffer(headerBuffer);

        short dataTypeIndex = headerBuffer.getShort();
        boolean isCompressed = headerBuffer.getShort() == BUFFER_IS_COMPRESSED;
        int length = headerBuffer.getInt();
        return new BufferHeader(isCompressed, length, parseDataTypeHeader(dataTypeIndex));
    }

    private static Buffer.DataType parseDataTypeHeader(Short dataTypeIndex) {
        if (dataTypeIndex == HEADER_VALUE_IS_BUFFER) {
            return Buffer.DataType.DATA_BUFFER;
        } else if (dataTypeIndex == HEADER_VALUE_IS_SEGMENT_EVENT) {
            return Buffer.DataType.END_OF_SEGMENT;
        } else if (dataTypeIndex == HEADER_VALUE_IS_EVENT) {
            return Buffer.DataType.EVENT_BUFFER;
        } else {
            throw new RuntimeException("Parsing DataType failed, dataTypeIndex: " + dataTypeIndex);
        }
    }

    private static void throwPrematureEndOfFile() throws IOException {
        throw new IOException("The spill file is corrupt: premature end of file");
    }

    private static void throwCorruptDataException() throws IOException {
        throw new IOException("The spill file is corrupt: buffer size and boundaries invalid");
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    static void configureByteBuffer(ByteBuffer buffer) {
        buffer.order(ByteOrder.nativeOrder());
    }
}
