/*
 * Copyright 2012 The Netty Project
 * Copy from netty 4.1.32.Final
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.testutils.CheckedThread;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufUtil;
import org.apache.flink.shaded.netty4.io.netty.util.ByteProcessor;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;
import org.apache.flink.shaded.netty4.io.netty.util.IllegalReferenceCountException;
import org.apache.flink.shaded.netty4.io.netty.util.internal.PlatformDependent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.LITTLE_ENDIAN;
import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.buffer;
import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.copiedBuffer;
import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.directBuffer;
import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.unreleasableBuffer;
import static org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.wrappedBuffer;
import static org.apache.flink.shaded.netty4.io.netty.util.internal.EmptyArrays.EMPTY_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * An abstract test class for channel buffers.
 *
 * <p>Copy from netty 4.1.32.Final.
 */
abstract class AbstractByteBufTest {

    private static final int CAPACITY = 4096; // Must be even
    private static final int BLOCK_SIZE = 128;
    private static final int JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS = 100;

    private long seed;
    private Random random;
    private ByteBuf buffer;

    protected final ByteBuf newBuffer(int capacity) {
        return newBuffer(capacity, Integer.MAX_VALUE);
    }

    protected abstract ByteBuf newBuffer(int capacity, int maxCapacity);

    protected boolean discardReadBytesDoesNotMoveWritableBytes() {
        return true;
    }

    @BeforeEach
    void init() {
        buffer = newBuffer(CAPACITY);
        seed = System.currentTimeMillis();
        random = new Random(seed);
    }

    @AfterEach
    void dispose() {
        if (buffer != null) {
            assertThat(buffer.release()).isTrue();
            assertThat(buffer.refCnt()).isZero();

            try {
                buffer.release();
            } catch (Exception e) {
                // Ignore.
            }
            buffer = null;
        }
    }

    @Test
    void comparableInterfaceNotViolated() {
        assumeThat(buffer.isReadOnly()).isFalse();
        buffer.writerIndex(buffer.readerIndex());
        assumeThat(buffer.writableBytes() >= 4).isTrue();

        buffer.writeLong(0);
        ByteBuf buffer2 = newBuffer(CAPACITY);
        assumeThat(buffer2.isReadOnly()).isFalse();
        buffer2.writerIndex(buffer2.readerIndex());
        // Write an unsigned integer that will cause buffer.getUnsignedInt() -
        // buffer2.getUnsignedInt() to underflow the
        // int type and wrap around on the negative side.
        buffer2.writeLong(0xF0000000L);
        assumeThat(buffer).isLessThan(buffer2);
        assumeThat(buffer2).isGreaterThan(buffer);
        buffer2.release();
    }

    @Test
    void initialState() {
        assertThat(buffer.capacity()).isEqualTo(CAPACITY);
        assertThat(buffer.readerIndex()).isZero();
    }

    @Test
    void readerIndexBoundaryCheck1() {
        buffer.writerIndex(0);
        assertThatThrownBy(() -> buffer.readerIndex(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void readerIndexBoundaryCheck2() {
        buffer.writerIndex(buffer.capacity());
        assertThatThrownBy(() -> buffer.readerIndex(buffer.capacity() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void readerIndexBoundaryCheck3() {
        buffer.writerIndex(CAPACITY / 2);
        assertThatThrownBy(() -> buffer.readerIndex(CAPACITY * 3 / 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void readerIndexBoundaryCheck4() {
        buffer.writerIndex(0);
        buffer.readerIndex(0);
        buffer.writerIndex(buffer.capacity());
        buffer.readerIndex(buffer.capacity());
    }

    @Test
    void writerIndexBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.writerIndex(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void writerIndexBoundaryCheck2() {
        buffer.writerIndex(CAPACITY);
        buffer.readerIndex(CAPACITY);
        assertThatThrownBy(() -> buffer.writerIndex(buffer.capacity() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void writerIndexBoundaryCheck3() {
        buffer.writerIndex(CAPACITY);
        buffer.readerIndex(CAPACITY / 2);
        assertThatThrownBy(() -> buffer.writerIndex(CAPACITY / 4))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void writerIndexBoundaryCheck4() {
        buffer.writerIndex(0);
        buffer.readerIndex(0);
        buffer.writerIndex(CAPACITY);

        buffer.writeBytes(ByteBuffer.wrap(EMPTY_BYTES));
    }

    @Test
    void getBooleanBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getBoolean(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getBooleanBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getBoolean(buffer.capacity()))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getByteBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getByte(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getByteBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getByte(buffer.capacity()))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getShortBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getShort(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getShortBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getShort(buffer.capacity() - 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getMediumBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getMedium(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getMediumBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getMedium(buffer.capacity() - 2))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getIntBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getInt(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getIntBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getInt(buffer.capacity() - 3))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getLongBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getLong(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getLongBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getLong(buffer.capacity() - 7))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getByteArrayBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.getBytes(-1, EMPTY_BYTES))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getByteArrayBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.getBytes(-1, EMPTY_BYTES, 0, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getByteArrayBoundaryCheck3() {
        byte[] dst = new byte[4];
        buffer.setInt(0, 0x01020304);

        assertThatThrownBy(() -> buffer.getBytes(0, dst, -1, 4))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // No partial copy is expected.
        assertThat(dst[0]).isZero();
        assertThat(dst[1]).isZero();
        assertThat(dst[2]).isZero();
        assertThat(dst[3]).isZero();
    }

    @Test
    void getByteArrayBoundaryCheck4() {
        byte[] dst = new byte[4];
        buffer.setInt(0, 0x01020304);

        assertThatThrownBy(() -> buffer.getBytes(0, dst, 1, 4))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // No partial copy is expected.
        assertThat(dst[0]).isZero();
        assertThat(dst[1]).isZero();
        assertThat(dst[2]).isZero();
        assertThat(dst[3]).isZero();
    }

    @Test
    void getByteBufferBoundaryCheck() {
        assertThatThrownBy(() -> buffer.getBytes(-1, ByteBuffer.allocate(0)))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void copyBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.copy(-1, 0)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void copyBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.copy(0, buffer.capacity() + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void copyBoundaryCheck3() {
        assertThatThrownBy(() -> buffer.copy(buffer.capacity() + 1, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void copyBoundaryCheck4() {
        assertThatThrownBy(() -> buffer.copy(buffer.capacity(), 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void setIndexBoundaryCheck1() {
        assertThatThrownBy(() -> buffer.setIndex(-1, CAPACITY))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void setIndexBoundaryCheck2() {
        assertThatThrownBy(() -> buffer.setIndex(CAPACITY / 2, CAPACITY / 4))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void setIndexBoundaryCheck3() {
        assertThatThrownBy(() -> buffer.setIndex(0, CAPACITY + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getByteBufferState() {
        ByteBuffer dst = ByteBuffer.allocate(4);
        dst.position(1);
        dst.limit(3);

        buffer.setByte(0, 1);
        buffer.setByte(1, 2);
        buffer.setByte(2, 3);
        buffer.setByte(3, 4);
        buffer.getBytes(1, dst);

        assertThat(dst.position()).isEqualTo(3);
        assertThat(dst.limit()).isEqualTo(3);

        dst.clear();
        assertThat(dst.get(0)).isZero();
        assertThat(dst.get(1)).isEqualTo((byte) 2);
        assertThat(dst.get(2)).isEqualTo((byte) 3);
        assertThat(dst.get(3)).isZero();
    }

    @Test
    void getDirectByteBufferBoundaryCheck() {
        assertThatThrownBy(() -> buffer.getBytes(-1, ByteBuffer.allocateDirect(0)))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getDirectByteBufferState() {
        ByteBuffer dst = ByteBuffer.allocateDirect(4);
        dst.position(1);
        dst.limit(3);

        buffer.setByte(0, 1);
        buffer.setByte(1, 2);
        buffer.setByte(2, 3);
        buffer.setByte(3, 4);
        buffer.getBytes(1, dst);

        assertThat(dst.position()).isEqualTo(3);
        assertThat(dst.limit()).isEqualTo(3);

        dst.clear();
        assertThat(dst.get(0)).isZero();
        assertThat(dst.get(1)).isEqualTo((byte) 2);
        assertThat(dst.get(2)).isEqualTo((byte) 3);
        assertThat(dst.get(3)).isZero();
    }

    @Test
    void testRandomByteAccess() {
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            assertThat(buffer.getByte(i)).isEqualTo(value);
        }
    }

    @Test
    void testRandomUnsignedByteAccess() {
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i++) {
            short value = (short) (random.nextInt() & 0xFF);
            assertThat(buffer.getUnsignedByte(i)).isEqualTo(value);
        }
    }

    @Test
    void testRandomShortAccess() {
        testRandomShortAccess(true);
    }

    @Test
    void testRandomShortLEAccess() {
        testRandomShortAccess(false);
    }

    private void testRandomShortAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            if (testBigEndian) {
                buffer.setShort(i, value);
            } else {
                buffer.setShortLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            if (testBigEndian) {
                assertThat(buffer.getShort(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getShortLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testShortConsistentWithByteBuffer() {
        testShortConsistentWithByteBuffer(true, true);
        testShortConsistentWithByteBuffer(true, false);
        testShortConsistentWithByteBuffer(false, true);
        testShortConsistentWithByteBuffer(false, false);
    }

    private void testShortConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
        for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
            ByteBuffer javaBuffer =
                    direct
                            ? ByteBuffer.allocateDirect(buffer.capacity())
                            : ByteBuffer.allocate(buffer.capacity());
            if (!testBigEndian) {
                javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            short expected = (short) (random.nextInt() & 0xFFFF);
            javaBuffer.putShort(expected);

            final int bufferIndex = buffer.capacity() - 2;
            if (testBigEndian) {
                buffer.setShort(bufferIndex, expected);
            } else {
                buffer.setShortLE(bufferIndex, expected);
            }
            javaBuffer.flip();

            short javaActual = javaBuffer.getShort();
            assertThat(javaActual)
                    .isEqualTo(expected)
                    .isEqualTo(
                            testBigEndian
                                    ? buffer.getShort(bufferIndex)
                                    : buffer.getShortLE(bufferIndex));
        }
    }

    @Test
    void testRandomUnsignedShortAccess() {
        testRandomUnsignedShortAccess(true);
    }

    @Test
    void testRandomUnsignedShortLEAccess() {
        testRandomUnsignedShortAccess(false);
    }

    private void testRandomUnsignedShortAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 1; i += 2) {
            short value = (short) random.nextInt();
            if (testBigEndian) {
                buffer.setShort(i, value);
            } else {
                buffer.setShortLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 1; i += 2) {
            int value = random.nextInt() & 0xFFFF;
            if (testBigEndian) {
                assertThat(buffer.getUnsignedShort(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getUnsignedShortLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testRandomMediumAccess() {
        testRandomMediumAccess(true);
    }

    @Test
    void testRandomMediumLEAccess() {
        testRandomMediumAccess(false);
    }

    private void testRandomMediumAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 2; i += 3) {
            int value = random.nextInt();
            if (testBigEndian) {
                buffer.setMedium(i, value);
            } else {
                buffer.setMediumLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 2; i += 3) {
            int value = random.nextInt() << 8 >> 8;
            if (testBigEndian) {
                assertThat(buffer.getMedium(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getMediumLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testRandomUnsignedMediumAccess() {
        testRandomUnsignedMediumAccess(true);
    }

    @Test
    void testRandomUnsignedMediumLEAccess() {
        testRandomUnsignedMediumAccess(false);
    }

    private void testRandomUnsignedMediumAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 2; i += 3) {
            int value = random.nextInt();
            if (testBigEndian) {
                buffer.setMedium(i, value);
            } else {
                buffer.setMediumLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 2; i += 3) {
            int value = random.nextInt() & 0x00FFFFFF;
            if (testBigEndian) {
                assertThat(buffer.getUnsignedMedium(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getUnsignedMediumLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testMediumConsistentWithByteBuffer() {
        testMediumConsistentWithByteBuffer(true, true);
        testMediumConsistentWithByteBuffer(true, false);
        testMediumConsistentWithByteBuffer(false, true);
        testMediumConsistentWithByteBuffer(false, false);
    }

    private void testMediumConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
        for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
            ByteBuffer javaBuffer =
                    direct
                            ? ByteBuffer.allocateDirect(buffer.capacity())
                            : ByteBuffer.allocate(buffer.capacity());
            if (!testBigEndian) {
                javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            int expected = random.nextInt() & 0x00FFFFFF;
            javaBuffer.putInt(expected);

            final int bufferIndex = buffer.capacity() - 3;
            if (testBigEndian) {
                buffer.setMedium(bufferIndex, expected);
            } else {
                buffer.setMediumLE(bufferIndex, expected);
            }
            javaBuffer.flip();

            int javaActual = javaBuffer.getInt();
            assertThat(javaActual).isEqualTo(expected);
            assertThat(javaActual)
                    .isEqualTo(
                            testBigEndian
                                    ? buffer.getUnsignedMedium(bufferIndex)
                                    : buffer.getUnsignedMediumLE(bufferIndex));
        }
    }

    @Test
    void testRandomIntAccess() {
        testRandomIntAccess(true);
    }

    @Test
    void testRandomIntLEAccess() {
        testRandomIntAccess(false);
    }

    private void testRandomIntAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 3; i += 4) {
            int value = random.nextInt();
            if (testBigEndian) {
                buffer.setInt(i, value);
            } else {
                buffer.setIntLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 3; i += 4) {
            int value = random.nextInt();
            if (testBigEndian) {
                assertThat(buffer.getInt(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getIntLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testIntConsistentWithByteBuffer() {
        testIntConsistentWithByteBuffer(true, true);
        testIntConsistentWithByteBuffer(true, false);
        testIntConsistentWithByteBuffer(false, true);
        testIntConsistentWithByteBuffer(false, false);
    }

    private void testIntConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
        for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
            ByteBuffer javaBuffer =
                    direct
                            ? ByteBuffer.allocateDirect(buffer.capacity())
                            : ByteBuffer.allocate(buffer.capacity());
            if (!testBigEndian) {
                javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            int expected = random.nextInt();
            javaBuffer.putInt(expected);

            final int bufferIndex = buffer.capacity() - 4;
            if (testBigEndian) {
                buffer.setInt(bufferIndex, expected);
            } else {
                buffer.setIntLE(bufferIndex, expected);
            }
            javaBuffer.flip();

            int javaActual = javaBuffer.getInt();
            assertThat(javaActual)
                    .isEqualTo(expected)
                    .isEqualTo(
                            testBigEndian
                                    ? buffer.getInt(bufferIndex)
                                    : buffer.getIntLE(bufferIndex));
        }
    }

    @Test
    void testRandomUnsignedIntAccess() {
        testRandomUnsignedIntAccess(true);
    }

    @Test
    void testRandomUnsignedIntLEAccess() {
        testRandomUnsignedIntAccess(false);
    }

    private void testRandomUnsignedIntAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 3; i += 4) {
            int value = random.nextInt();
            if (testBigEndian) {
                buffer.setInt(i, value);
            } else {
                buffer.setIntLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 3; i += 4) {
            long value = random.nextInt() & 0xFFFFFFFFL;
            if (testBigEndian) {
                assertThat(buffer.getUnsignedInt(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getUnsignedIntLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testRandomLongAccess() {
        testRandomLongAccess(true);
    }

    @Test
    void testRandomLongLEAccess() {
        testRandomLongAccess(false);
    }

    private void testRandomLongAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            long value = random.nextLong();
            if (testBigEndian) {
                buffer.setLong(i, value);
            } else {
                buffer.setLongLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            long value = random.nextLong();
            if (testBigEndian) {
                assertThat(buffer.getLong(i)).isEqualTo(value);
            } else {
                assertThat(buffer.getLongLE(i)).isEqualTo(value);
            }
        }
    }

    @Test
    void testLongConsistentWithByteBuffer() {
        testLongConsistentWithByteBuffer(true, true);
        testLongConsistentWithByteBuffer(true, false);
        testLongConsistentWithByteBuffer(false, true);
        testLongConsistentWithByteBuffer(false, false);
    }

    private void testLongConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
        for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
            ByteBuffer javaBuffer =
                    direct
                            ? ByteBuffer.allocateDirect(buffer.capacity())
                            : ByteBuffer.allocate(buffer.capacity());
            if (!testBigEndian) {
                javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            long expected = random.nextLong();
            javaBuffer.putLong(expected);

            final int bufferIndex = buffer.capacity() - 8;
            if (testBigEndian) {
                buffer.setLong(bufferIndex, expected);
            } else {
                buffer.setLongLE(bufferIndex, expected);
            }
            javaBuffer.flip();

            long javaActual = javaBuffer.getLong();
            assertThat(javaActual)
                    .isEqualTo(expected)
                    .isEqualTo(
                            testBigEndian
                                    ? buffer.getLong(bufferIndex)
                                    : buffer.getLongLE(bufferIndex));
        }
    }

    @Test
    void testRandomFloatAccess() {
        testRandomFloatAccess(true);
    }

    @Test
    void testRandomFloatLEAccess() {
        testRandomFloatAccess(false);
    }

    private void testRandomFloatAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            float value = random.nextFloat();
            if (testBigEndian) {
                buffer.setFloat(i, value);
            } else {
                buffer.setFloatLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            float expected = random.nextFloat();
            float actual = testBigEndian ? buffer.getFloat(i) : buffer.getFloatLE(i);
            assertThat(actual).isCloseTo(expected, within(0.01f));
        }
    }

    @Test
    void testRandomDoubleAccess() {
        testRandomDoubleAccess(true);
    }

    @Test
    void testRandomDoubleLEAccess() {
        testRandomDoubleAccess(false);
    }

    private void testRandomDoubleAccess(boolean testBigEndian) {
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            double value = random.nextDouble();
            if (testBigEndian) {
                buffer.setDouble(i, value);
            } else {
                buffer.setDoubleLE(i, value);
            }
        }

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() - 7; i += 8) {
            double expected = random.nextDouble();
            double actual = testBigEndian ? buffer.getDouble(i) : buffer.getDoubleLE(i);
            assertThat(actual).isCloseTo(expected, within(0.01));
        }
    }

    @Test
    void testSetZero() {
        buffer.clear();
        while (buffer.isWritable()) {
            buffer.writeByte((byte) 0xFF);
        }

        for (int i = 0; i < buffer.capacity(); ) {
            int length = Math.min(buffer.capacity() - i, random.nextInt(32));
            buffer.setZero(i, length);
            i += length;
        }

        for (int i = 0; i < buffer.capacity(); i++) {
            assertThat(buffer.getByte(i)).isZero();
        }
    }

    @Test
    void testSequentialByteAccess() {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            buffer.writeByte(value);
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            assertThat(buffer.readByte()).isEqualTo(value);
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testSequentialUnsignedByteAccess() {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            buffer.writeByte(value);
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i++) {
            int value = random.nextInt() & 0xFF;
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            assertThat(buffer.readUnsignedByte()).isEqualTo((short) value);
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testSequentialShortAccess() {
        testSequentialShortAccess(true);
    }

    @Test
    void testSequentialShortLEAccess() {
        testSequentialShortAccess(false);
    }

    private void testSequentialShortAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 2) {
            short value = (short) random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeShort(value);
            } else {
                buffer.writeShortLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 2) {
            short value = (short) random.nextInt();
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readShort()).isEqualTo(value);
            } else {
                assertThat(buffer.readShortLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testSequentialUnsignedShortAccess() {
        testSequentialUnsignedShortAccess(true);
    }

    @Test
    void testSequentialUnsignedShortLEAccess() {
        testSequentialUnsignedShortAccess(true);
    }

    private void testSequentialUnsignedShortAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 2) {
            short value = (short) random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeShort(value);
            } else {
                buffer.writeShortLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 2) {
            int value = random.nextInt() & 0xFFFF;
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readUnsignedShort()).isEqualTo(value);
            } else {
                assertThat(buffer.readUnsignedShortLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testSequentialMediumAccess() {
        testSequentialMediumAccess(true);
    }

    @Test
    void testSequentialMediumLEAccess() {
        testSequentialMediumAccess(false);
    }

    private void testSequentialMediumAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
            int value = random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeMedium(value);
            } else {
                buffer.writeMediumLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity() / 3 * 3);
        assertThat(buffer.writableBytes()).isEqualTo(buffer.capacity() % 3);

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
            int value = random.nextInt() << 8 >> 8;
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readMedium()).isEqualTo(value);
            } else {
                assertThat(buffer.readMediumLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity() / 3 * 3);
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity() / 3 * 3);
        assertThat(buffer.readableBytes()).isZero();
        assertThat(buffer.writableBytes()).isEqualTo(buffer.capacity() % 3);
    }

    @Test
    void testSequentialUnsignedMediumAccess() {
        testSequentialUnsignedMediumAccess(true);
    }

    @Test
    void testSequentialUnsignedMediumLEAccess() {
        testSequentialUnsignedMediumAccess(false);
    }

    private void testSequentialUnsignedMediumAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
            int value = random.nextInt() & 0x00FFFFFF;
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeMedium(value);
            } else {
                buffer.writeMediumLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity() / 3 * 3);
        assertThat(buffer.writableBytes()).isEqualTo(buffer.capacity() % 3);

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
            int value = random.nextInt() & 0x00FFFFFF;
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readUnsignedMedium()).isEqualTo(value);
            } else {
                assertThat(buffer.readUnsignedMediumLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity() / 3 * 3);
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity() / 3 * 3);
        assertThat(buffer.readableBytes()).isZero();
        assertThat(buffer.writableBytes()).isEqualTo(buffer.capacity() % 3);
    }

    @Test
    void testSequentialIntAccess() {
        testSequentialIntAccess(true);
    }

    @Test
    void testSequentialIntLEAccess() {
        testSequentialIntAccess(false);
    }

    private void testSequentialIntAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            int value = random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeInt(value);
            } else {
                buffer.writeIntLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            int value = random.nextInt();
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readInt()).isEqualTo(value);
            } else {
                assertThat(buffer.readIntLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testSequentialUnsignedIntAccess() {
        testSequentialUnsignedIntAccess(true);
    }

    @Test
    void testSequentialUnsignedIntLEAccess() {
        testSequentialUnsignedIntAccess(false);
    }

    private void testSequentialUnsignedIntAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            int value = random.nextInt();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeInt(value);
            } else {
                buffer.writeIntLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            long value = random.nextInt() & 0xFFFFFFFFL;
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readUnsignedInt()).isEqualTo(value);
            } else {
                assertThat(buffer.readUnsignedIntLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testSequentialLongAccess() {
        testSequentialLongAccess(true);
    }

    @Test
    void testSequentialLongLEAccess() {
        testSequentialLongAccess(false);
    }

    private void testSequentialLongAccess(boolean testBigEndian) {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 8) {
            long value = random.nextLong();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            assertThat(buffer.isWritable()).isTrue();
            if (testBigEndian) {
                buffer.writeLong(value);
            } else {
                buffer.writeLongLE(value);
            }
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isWritable()).isFalse();

        random.setSeed(seed);
        for (int i = 0; i < buffer.capacity(); i += 8) {
            long value = random.nextLong();
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.isReadable()).isTrue();
            if (testBigEndian) {
                assertThat(buffer.readLong()).isEqualTo(value);
            } else {
                assertThat(buffer.readLongLE()).isEqualTo(value);
            }
        }

        assertThat(buffer.readerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.isReadable()).isFalse();
        assertThat(buffer.isWritable()).isFalse();
    }

    @Test
    void testByteArrayTransfer() {
        byte[] value = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value[j]).isEqualTo(expectedValue[j]);
            }
        }
    }

    @Test
    void testRandomByteArrayTransfer1() {
        byte[] value = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            buffer.getBytes(i, value);
            for (int j = 0; j < BLOCK_SIZE; j++) {
                assertThat(value[j]).isEqualTo(expectedValue.getByte(j));
            }
        }
    }

    @Test
    void testRandomByteArrayTransfer2() {
        byte[] value = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value[j]).isEqualTo(expectedValue.getByte(j));
            }
        }
    }

    @Test
    void testRandomHeapBufferTransfer1() {
        byte[] valueContent = new byte[BLOCK_SIZE];
        ByteBuf value = wrappedBuffer(valueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.setIndex(0, BLOCK_SIZE);
            buffer.setBytes(i, value);
            assertThat(value.readerIndex()).isEqualTo(BLOCK_SIZE);
            assertThat(value.writerIndex()).isEqualTo(BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            value.clear();
            buffer.getBytes(i, value);
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isEqualTo(BLOCK_SIZE);
            for (int j = 0; j < BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
        }
    }

    @Test
    void testRandomHeapBufferTransfer2() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = wrappedBuffer(valueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
        }
    }

    @Test
    void testRandomDirectBufferTransfer() {
        byte[] tmp = new byte[BLOCK_SIZE * 2];
        ByteBuf value = directBuffer(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(tmp);
            value.setBytes(0, tmp, 0, value.capacity());
            buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
        }

        random.setSeed(seed);
        ByteBuf expectedValue = directBuffer(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(tmp);
            expectedValue.setBytes(0, tmp, 0, expectedValue.capacity());
            int valueOffset = random.nextInt(BLOCK_SIZE);
            buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
        }
        value.release();
        expectedValue.release();
    }

    @Test
    void testRandomByteBufferTransfer() {
        ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value.array());
            value.clear().position(random.nextInt(BLOCK_SIZE));
            value.limit(value.position() + BLOCK_SIZE);
            buffer.setBytes(i, value);
        }

        random.setSeed(seed);
        ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue.array());
            int valueOffset = random.nextInt(BLOCK_SIZE);
            value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
            buffer.getBytes(i, value);
            assertThat(value.position()).isEqualTo(valueOffset + BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.get(j)).isEqualTo(expectedValue.get(j));
            }
        }
    }

    @Test
    void testSequentialByteArrayTransfer1() {
        byte[] value = new byte[BLOCK_SIZE];
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            buffer.readBytes(value);
            for (int j = 0; j < BLOCK_SIZE; j++) {
                assertThat(value[j]).isEqualTo(expectedValue[j]);
            }
        }
    }

    @Test
    void testSequentialByteArrayTransfer2() {
        byte[] value = new byte[BLOCK_SIZE * 2];
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            int readerIndex = random.nextInt(BLOCK_SIZE);
            buffer.writeBytes(value, readerIndex, BLOCK_SIZE);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE * 2];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            buffer.readBytes(value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value[j]).isEqualTo(expectedValue[j]);
            }
        }
    }

    @Test
    void testSequentialHeapBufferTransfer1() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = wrappedBuffer(valueContent);
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isEqualTo(valueContent.length);
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            buffer.readBytes(value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isEqualTo(valueContent.length);
        }
    }

    @Test
    void testSequentialHeapBufferTransfer2() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = wrappedBuffer(valueContent);
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            int readerIndex = random.nextInt(BLOCK_SIZE);
            value.readerIndex(readerIndex);
            value.writerIndex(readerIndex + BLOCK_SIZE);
            buffer.writeBytes(value);
            assertThat(value.writerIndex()).isEqualTo(readerIndex + BLOCK_SIZE);
            assertThat(value.readerIndex()).isEqualTo(value.writerIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            value.readerIndex(valueOffset);
            value.writerIndex(valueOffset);
            buffer.readBytes(value, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
            assertThat(value.readerIndex()).isEqualTo(valueOffset);
            assertThat(value.writerIndex()).isEqualTo(valueOffset + BLOCK_SIZE);
        }
    }

    @Test
    void testSequentialDirectBufferTransfer1() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = directBuffer(BLOCK_SIZE * 2);
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.setBytes(0, valueContent);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isZero();
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            value.setBytes(0, valueContent);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            buffer.readBytes(value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isZero();
        }
        value.release();
        expectedValue.release();
    }

    @Test
    void testSequentialDirectBufferTransfer2() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = directBuffer(BLOCK_SIZE * 2);
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.setBytes(0, valueContent);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            int readerIndex = random.nextInt(BLOCK_SIZE);
            value.readerIndex(0);
            value.writerIndex(readerIndex + BLOCK_SIZE);
            value.readerIndex(readerIndex);
            buffer.writeBytes(value);
            assertThat(value.writerIndex()).isEqualTo(readerIndex + BLOCK_SIZE);
            assertThat(value.readerIndex()).isEqualTo(value.writerIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            value.setBytes(0, valueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            value.readerIndex(valueOffset);
            value.writerIndex(valueOffset);
            buffer.readBytes(value, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
            assertThat(value.readerIndex()).isEqualTo(valueOffset);
            assertThat(value.writerIndex()).isEqualTo(valueOffset + BLOCK_SIZE);
        }
        value.release();
        expectedValue.release();
    }

    @Test
    void testSequentialByteBufferBackedHeapBufferTransfer1() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2));
        value.writerIndex(0);
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.setBytes(0, valueContent);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isZero();
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            value.setBytes(0, valueContent);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            buffer.readBytes(value, valueOffset, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
            assertThat(value.readerIndex()).isZero();
            assertThat(value.writerIndex()).isZero();
        }
    }

    @Test
    void testSequentialByteBufferBackedHeapBufferTransfer2() {
        byte[] valueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf value = wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2));
        value.writerIndex(0);
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(valueContent);
            value.setBytes(0, valueContent);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            int readerIndex = random.nextInt(BLOCK_SIZE);
            value.readerIndex(0);
            value.writerIndex(readerIndex + BLOCK_SIZE);
            value.readerIndex(readerIndex);
            buffer.writeBytes(value);
            assertThat(value.writerIndex()).isEqualTo(readerIndex + BLOCK_SIZE);
            assertThat(value.readerIndex()).isEqualTo(value.writerIndex());
        }

        random.setSeed(seed);
        byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
        ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValueContent);
            value.setBytes(0, valueContent);
            int valueOffset = random.nextInt(BLOCK_SIZE);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            value.readerIndex(valueOffset);
            value.writerIndex(valueOffset);
            buffer.readBytes(value, BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.getByte(j)).isEqualTo(expectedValue.getByte(j));
            }
            assertThat(value.readerIndex()).isEqualTo(valueOffset);
            assertThat(value.writerIndex()).isEqualTo(valueOffset + BLOCK_SIZE);
        }
    }

    @Test
    void testSequentialByteBufferTransfer() {
        buffer.writerIndex(0);
        ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(value.array());
            value.clear().position(random.nextInt(BLOCK_SIZE));
            value.limit(value.position() + BLOCK_SIZE);
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue.array());
            int valueOffset = random.nextInt(BLOCK_SIZE);
            value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
            buffer.readBytes(value);
            assertThat(value.position()).isEqualTo(valueOffset + BLOCK_SIZE);
            for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
                assertThat(value.get(j)).isEqualTo(expectedValue.get(j));
            }
        }
    }

    @Test
    void testSequentialCopiedBufferTransfer1() {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            byte[] value = new byte[BLOCK_SIZE];
            random.nextBytes(value);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            ByteBuf actualValue = buffer.readBytes(BLOCK_SIZE);
            assertThat(actualValue).isEqualTo(wrappedBuffer(expectedValue));

            // Make sure if it is a copied buffer.
            actualValue.setByte(0, (byte) (actualValue.getByte(0) + 1));
            assertThat(buffer.getByte(i) == actualValue.getByte(0)).isFalse();
            actualValue.release();
        }
    }

    @Test
    void testSequentialSlice1() {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            byte[] value = new byte[BLOCK_SIZE];
            random.nextBytes(value);
            assertThat(buffer.readerIndex()).isZero();
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(value);
        }

        random.setSeed(seed);
        byte[] expectedValue = new byte[BLOCK_SIZE];
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            random.nextBytes(expectedValue);
            assertThat(buffer.readerIndex()).isEqualTo(i);
            assertThat(buffer.writerIndex()).isEqualTo(CAPACITY);
            ByteBuf actualValue = buffer.readSlice(BLOCK_SIZE);
            assertThat(actualValue.order()).isEqualTo(buffer.order());
            assertThat(actualValue).isEqualTo(wrappedBuffer(expectedValue));

            // Make sure if it is a sliced buffer.
            actualValue.setByte(0, (byte) (actualValue.getByte(0) + 1));
            assertThat(actualValue.getByte(0)).isEqualTo(buffer.getByte(i));
        }
    }

    @Test
    void testWriteZero() {
        assertThatThrownBy(() -> buffer.writeZero(-1)).isInstanceOf(IllegalArgumentException.class);

        buffer.clear();
        while (buffer.isWritable()) {
            buffer.writeByte((byte) 0xFF);
        }

        buffer.clear();
        for (int i = 0; i < buffer.capacity(); ) {
            int length = Math.min(buffer.capacity() - i, random.nextInt(32));
            buffer.writeZero(length);
            i += length;
        }

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(buffer.capacity());

        for (int i = 0; i < buffer.capacity(); i++) {
            assertThat(buffer.getByte(i)).isZero();
        }
    }

    @Test
    void testDiscardReadBytes() {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i += 4) {
            buffer.writeInt(i);
        }
        ByteBuf copy = copiedBuffer(buffer);

        // Make sure there's no effect if called when readerIndex is 0.
        buffer.readerIndex(CAPACITY / 4);
        buffer.markReaderIndex();
        buffer.writerIndex(CAPACITY / 3);
        buffer.markWriterIndex();
        buffer.readerIndex(0);
        buffer.writerIndex(CAPACITY / 2);
        buffer.discardReadBytes();

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(CAPACITY / 2);
        assertThat(buffer.slice(0, CAPACITY / 2)).isEqualTo(copy.slice(0, CAPACITY / 2));
        buffer.resetReaderIndex();
        assertThat(buffer.readerIndex()).isEqualTo(CAPACITY / 4);
        buffer.resetWriterIndex();
        assertThat(buffer.writerIndex()).isEqualTo(CAPACITY / 3);

        // Make sure bytes after writerIndex is not copied.
        buffer.readerIndex(1);
        buffer.writerIndex(CAPACITY / 2);
        buffer.discardReadBytes();

        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(CAPACITY / 2 - 1);
        assertThat(buffer.slice(0, CAPACITY / 2 - 1)).isEqualTo(copy.slice(1, CAPACITY / 2 - 1));

        if (discardReadBytesDoesNotMoveWritableBytes()) {
            // If writable bytes were copied, the test should fail to avoid unnecessary memory
            // bandwidth consumption.
            assertThat(
                            copy.slice(CAPACITY / 2, CAPACITY / 2)
                                    .equals(buffer.slice(CAPACITY / 2 - 1, CAPACITY / 2)))
                    .isFalse();
        } else {
            assertThat(buffer.slice(CAPACITY / 2 - 1, CAPACITY / 2))
                    .isEqualTo(copy.slice(CAPACITY / 2, CAPACITY / 2));
        }

        // Marks also should be relocated.
        buffer.resetReaderIndex();
        assertThat(buffer.readerIndex()).isEqualTo(CAPACITY / 4 - 1);
        buffer.resetWriterIndex();
        assertThat(buffer.writerIndex()).isEqualTo(CAPACITY / 3 - 1);
        copy.release();
    }

    /**
     * The similar test case with {@link #testDiscardReadBytes()} but this one discards a large
     * chunk at once.
     */
    @Test
    void testDiscardReadBytes2() {
        buffer.writerIndex(0);
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.writeByte((byte) i);
        }
        ByteBuf copy = copiedBuffer(buffer);

        // Discard the first (CAPACITY / 2 - 1) bytes.
        buffer.setIndex(CAPACITY / 2 - 1, CAPACITY - 1);
        buffer.discardReadBytes();
        assertThat(buffer.readerIndex()).isZero();
        assertThat(buffer.writerIndex()).isEqualTo(CAPACITY / 2);
        for (int i = 0; i < CAPACITY / 2; i++) {
            assertThat(buffer.slice(i, CAPACITY / 2 - i))
                    .isEqualTo(copy.slice(CAPACITY / 2 - 1 + i, CAPACITY / 2 - i));
        }
        copy.release();
    }

    @Test
    void testStreamTransfer1() throws Exception {
        byte[] expected = new byte[buffer.capacity()];
        random.nextBytes(expected);

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            ByteArrayInputStream in = new ByteArrayInputStream(expected, i, BLOCK_SIZE);
            assertThat(buffer.setBytes(i, in, BLOCK_SIZE)).isEqualTo(BLOCK_SIZE);
            assertThat(buffer.setBytes(i, in, 0)).isEqualTo(-1);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            buffer.getBytes(i, out, BLOCK_SIZE);
        }

        assertThat(Arrays.equals(expected, out.toByteArray())).isTrue();
    }

    @Test
    void testStreamTransfer2() throws Exception {
        byte[] expected = new byte[buffer.capacity()];
        random.nextBytes(expected);
        buffer.clear();

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            ByteArrayInputStream in = new ByteArrayInputStream(expected, i, BLOCK_SIZE);
            assertThat(buffer.writerIndex()).isEqualTo(i);
            buffer.writeBytes(in, BLOCK_SIZE);
            assertThat(buffer.writerIndex()).isEqualTo(i + BLOCK_SIZE);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            assertThat(buffer.readerIndex()).isEqualTo(i);
            buffer.readBytes(out, BLOCK_SIZE);
            assertThat(buffer.readerIndex()).isEqualTo(i + BLOCK_SIZE);
        }

        assertThat(Arrays.equals(expected, out.toByteArray())).isTrue();
    }

    @Test
    void testCopy() {
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        final int readerIndex = CAPACITY / 3;
        final int writerIndex = CAPACITY * 2 / 3;
        buffer.setIndex(readerIndex, writerIndex);

        // Make sure all properties are copied.
        ByteBuf copy = buffer.copy();
        assertThat(copy.readerIndex()).isZero();
        assertThat(copy.writerIndex()).isEqualTo(buffer.readableBytes());
        assertThat(copy.capacity()).isEqualTo(buffer.readableBytes());
        assertThat(copy.order()).isSameAs(buffer.order());
        for (int i = 0; i < copy.capacity(); i++) {
            assertThat(copy.getByte(i)).isEqualTo(buffer.getByte(i + readerIndex));
        }

        // Make sure the buffer content is independent from each other.
        buffer.setByte(readerIndex, (byte) (buffer.getByte(readerIndex) + 1));
        assertThat(buffer.getByte(readerIndex) != copy.getByte(0)).isTrue();
        copy.setByte(1, (byte) (copy.getByte(1) + 1));
        assertThat(buffer.getByte(readerIndex + 1) != copy.getByte(1)).isTrue();
        copy.release();
    }

    @Test
    void testDuplicate() {
        for (int i = 0; i < buffer.capacity(); i++) {
            byte value = (byte) random.nextInt();
            buffer.setByte(i, value);
        }

        final int readerIndex = CAPACITY / 3;
        final int writerIndex = CAPACITY * 2 / 3;
        buffer.setIndex(readerIndex, writerIndex);

        // Make sure all properties are copied.
        ByteBuf duplicate = buffer.duplicate();
        assertThat(duplicate.order()).isSameAs(buffer.order());
        assertThat(duplicate.readableBytes()).isEqualTo(buffer.readableBytes());
        assertThat(buffer).isEqualTo(duplicate);

        // Make sure the buffer content is shared.
        buffer.setByte(readerIndex, (byte) (buffer.getByte(readerIndex) + 1));
        assertThat(duplicate.getByte(duplicate.readerIndex()))
                .isEqualTo(buffer.getByte(readerIndex));
        duplicate.setByte(
                duplicate.readerIndex(), (byte) (duplicate.getByte(duplicate.readerIndex()) + 1));
        assertThat(duplicate.getByte(duplicate.readerIndex()))
                .isEqualTo(buffer.getByte(readerIndex));
    }

    @Test
    void testSliceEndianness() {
        assertThat(buffer.slice(0, buffer.capacity()).order()).isEqualTo(buffer.order());
        assertThat(buffer.slice(0, buffer.capacity() - 1).order()).isEqualTo(buffer.order());
        assertThat(buffer.slice(1, buffer.capacity() - 1).order()).isEqualTo(buffer.order());
        assertThat(buffer.slice(1, buffer.capacity() - 2).order()).isEqualTo(buffer.order());
    }

    @Test
    void testSliceIndex() {
        assertThat(buffer.slice(0, buffer.capacity()).readerIndex()).isZero();
        assertThat(buffer.slice(0, buffer.capacity() - 1).readerIndex()).isZero();
        assertThat(buffer.slice(1, buffer.capacity() - 1).readerIndex()).isZero();
        assertThat(buffer.slice(1, buffer.capacity() - 2).readerIndex()).isZero();

        assertThat(buffer.slice(0, buffer.capacity()).writerIndex()).isEqualTo(buffer.capacity());
        assertThat(buffer.slice(0, buffer.capacity() - 1).writerIndex())
                .isEqualTo(buffer.capacity() - 1);
        assertThat(buffer.slice(1, buffer.capacity() - 1).writerIndex())
                .isEqualTo(buffer.capacity() - 1);
        assertThat(buffer.slice(1, buffer.capacity() - 2).writerIndex())
                .isEqualTo(buffer.capacity() - 2);
    }

    @Test
    void testRetainedSliceIndex() {
        ByteBuf retainedSlice = buffer.retainedSlice(0, buffer.capacity());
        assertThat(retainedSlice.readerIndex()).isZero();
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(0, buffer.capacity() - 1);
        assertThat(retainedSlice.readerIndex()).isZero();
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 1);
        assertThat(retainedSlice.readerIndex()).isZero();
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 2);
        assertThat(retainedSlice.readerIndex()).isZero();
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(0, buffer.capacity());
        assertThat(retainedSlice.writerIndex()).isEqualTo(buffer.capacity());
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(0, buffer.capacity() - 1);
        assertThat(retainedSlice.writerIndex()).isEqualTo(buffer.capacity() - 1);
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 1);
        assertThat(retainedSlice.writerIndex()).isEqualTo(buffer.capacity() - 1);
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 2);
        assertThat(retainedSlice.writerIndex()).isEqualTo(buffer.capacity() - 2);
        retainedSlice.release();
    }

    @Test
    void testEquals() {
        assertThat(buffer).isNotEqualTo(null).isNotEqualTo(new Object());

        byte[] value = new byte[32];
        buffer.setIndex(0, value.length);
        random.nextBytes(value);
        buffer.setBytes(0, value);

        assertThat(wrappedBuffer(value))
                .isEqualTo(wrappedBuffer(value).order(LITTLE_ENDIAN))
                .isEqualTo(buffer);

        value[0]++;
        assertThat(buffer)
                .isNotEqualTo(wrappedBuffer(value))
                .isNotEqualTo(wrappedBuffer(value).order(LITTLE_ENDIAN));
    }

    @Test
    void testCompareTo() {
        assertThatThrownBy(() -> buffer.compareTo(null)).isInstanceOf(NullPointerException.class);

        // Fill the random stuff
        byte[] value = new byte[32];
        random.nextBytes(value);
        // Prevent overflow / underflow
        if (value[0] == 0) {
            value[0]++;
        } else if (value[0] == -1) {
            value[0]--;
        }

        buffer.setIndex(0, value.length);
        buffer.setBytes(0, value);

        assertThat(buffer)
                .isEqualTo(wrappedBuffer(value))
                .isEqualTo(wrappedBuffer(value).order(LITTLE_ENDIAN));

        value[0]++;
        assertThat(buffer)
                .isLessThan(wrappedBuffer(value))
                .isLessThan(wrappedBuffer(value).order(LITTLE_ENDIAN));
        value[0] -= 2;
        assertThat(buffer)
                .isGreaterThan(wrappedBuffer(value))
                .isGreaterThan(wrappedBuffer(value).order(LITTLE_ENDIAN));
        value[0]++;

        assertThat(buffer)
                .isGreaterThan(wrappedBuffer(value, 0, 31))
                .isGreaterThan(wrappedBuffer(value, 0, 31).order(LITTLE_ENDIAN));

        assertThat(buffer.slice(0, 31))
                .isLessThan(wrappedBuffer(value))
                .isLessThan(wrappedBuffer(value).order(LITTLE_ENDIAN));

        ByteBuf retainedSlice = buffer.retainedSlice(0, 31);
        assertThat(retainedSlice).isLessThan(wrappedBuffer(value));
        retainedSlice.release();

        retainedSlice = buffer.retainedSlice(0, 31);
        assertThat(retainedSlice).isLessThan(wrappedBuffer(value).order(LITTLE_ENDIAN));
        retainedSlice.release();
    }

    @Test
    void testCompareTo2() {
        byte[] bytes = {1, 2, 3, 4};
        byte[] bytesReversed = {4, 3, 2, 1};

        ByteBuf buf1 = newBuffer(4).clear().writeBytes(bytes).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuf buf2 =
                newBuffer(4).clear().writeBytes(bytesReversed).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuf buf3 = newBuffer(4).clear().writeBytes(bytes).order(ByteOrder.BIG_ENDIAN);
        ByteBuf buf4 = newBuffer(4).clear().writeBytes(bytesReversed).order(ByteOrder.BIG_ENDIAN);
        try {
            assertThat(buf3.compareTo(buf4)).isEqualTo(buf1.compareTo(buf2));
            assertThat(buf4.compareTo(buf3)).isEqualTo(buf2.compareTo(buf1));
            assertThat(buf2.compareTo(buf4)).isEqualTo(buf1.compareTo(buf3));
            assertThat(buf4.compareTo(buf2)).isEqualTo(buf3.compareTo(buf1));
        } finally {
            buf1.release();
            buf2.release();
            buf3.release();
            buf4.release();
        }
    }

    @Test
    void testToString() {
        ByteBuf copied = copiedBuffer("Hello, World!", CharsetUtil.ISO_8859_1);
        buffer.clear();
        buffer.writeBytes(copied);
        assertThat(buffer.toString(CharsetUtil.ISO_8859_1)).isEqualTo("Hello, World!");
        copied.release();
    }

    @Test
    @Timeout(value = 10)
    void testToStringMultipleThreads() throws Throwable {
        buffer.clear();
        buffer.writeBytes("Hello, World!".getBytes(CharsetUtil.ISO_8859_1));

        final AtomicInteger counter = new AtomicInteger(30000);
        final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>();
        List<CheckedThread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CheckedThread thread =
                    new CheckedThread() {
                        @Override
                        public void go() {
                            while (errorRef.get() == null && counter.decrementAndGet() > 0) {
                                assertThat(buffer.toString(CharsetUtil.ISO_8859_1))
                                        .isEqualTo("Hello, World!");
                            }
                        }
                    };
            threads.add(thread);
            thread.start();
        }
        for (CheckedThread thread : threads) {
            thread.sync();
        }
    }

    @Test
    void testIndexOf() {
        buffer.clear();
        buffer.writeByte(1);
        buffer.writeByte(2);
        buffer.writeByte(3);
        buffer.writeByte(2);
        buffer.writeByte(1);

        assertThat(buffer.indexOf(1, 4, (byte) 1)).isEqualTo(-1);
        assertThat(buffer.indexOf(4, 1, (byte) 1)).isEqualTo(-1);
        assertThat(buffer.indexOf(1, 4, (byte) 2)).isOne();
        assertThat(buffer.indexOf(4, 1, (byte) 2)).isEqualTo(3);
    }

    @Test
    void testNioBuffer1() {
        assumeThat(buffer.nioBufferCount()).isOne();

        byte[] value = new byte[buffer.capacity()];
        random.nextBytes(value);
        buffer.clear();
        buffer.writeBytes(value);

        assertRemainingEquals(ByteBuffer.wrap(value), buffer.nioBuffer());
    }

    @Test
    void testToByteBuffer2() {
        assumeThat(buffer.nioBufferCount()).isOne();

        byte[] value = new byte[buffer.capacity()];
        random.nextBytes(value);
        buffer.clear();
        buffer.writeBytes(value);

        for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
            assertRemainingEquals(
                    ByteBuffer.wrap(value, i, BLOCK_SIZE), buffer.nioBuffer(i, BLOCK_SIZE));
        }
    }

    private static void assertRemainingEquals(ByteBuffer expected, ByteBuffer actual) {
        int remaining = expected.remaining();
        int remaining2 = actual.remaining();

        assumeThat(remaining2).isEqualTo(remaining);
        byte[] array1 = new byte[remaining];
        byte[] array2 = new byte[remaining2];
        expected.get(array1);
        actual.get(array2);
        assertThat(array2).isEqualTo(array1);
    }

    @Test
    void testToByteBuffer3() {
        assumeThat(buffer.nioBufferCount()).isOne();

        assertThat(buffer.nioBuffer().order()).isEqualTo(buffer.order());
    }

    @Test
    void testSkipBytes1() {
        buffer.setIndex(CAPACITY / 4, CAPACITY / 2);

        buffer.skipBytes(CAPACITY / 4);
        assertThat(buffer.readerIndex()).isEqualTo(CAPACITY / 4 * 2);

        assertThatThrownBy(() -> buffer.skipBytes(CAPACITY / 4 + 1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // Should remain unchanged.
        assertThat(buffer.readerIndex()).isEqualTo(CAPACITY / 4 * 2);
    }

    @Test
    void testHashCode() {
        ByteBuf elemA = buffer(15);
        ByteBuf elemB = directBuffer(15);
        elemA.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5});
        elemB.writeBytes(new byte[] {6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9});

        Set<ByteBuf> set = new HashSet<ByteBuf>();
        set.add(elemA);
        set.add(elemB);

        assertThat(set).hasSize(2);
        ByteBuf elemACopy = elemA.copy();
        assertThat(set).contains(elemACopy);

        ByteBuf elemBCopy = elemB.copy();
        assertThat(set).contains(elemBCopy);

        buffer.clear();
        buffer.writeBytes(elemA.duplicate());

        assertThat(set.remove(buffer)).isTrue();
        assertThat(set).doesNotContainSequence(elemA).hasSize(1);

        buffer.clear();
        buffer.writeBytes(elemB.duplicate());
        assertThat(set.remove(buffer)).isTrue();
        assertThat(set).doesNotContainSequence(elemB).isEmpty();
        elemA.release();
        elemB.release();
        elemACopy.release();
        elemBCopy.release();
    }

    // Test case for https://github.com/netty/netty/issues/325
    @Test
    void testDiscardAllReadBytes() {
        buffer.writerIndex(buffer.capacity());
        buffer.readerIndex(buffer.writerIndex());
        buffer.discardReadBytes();
    }

    @Test
    void testForEachByte() {
        buffer.clear();
        for (int i = 0; i < CAPACITY; i++) {
            buffer.writeByte(i + 1);
        }

        final AtomicInteger lastIndex = new AtomicInteger();
        buffer.setIndex(CAPACITY / 4, CAPACITY * 3 / 4);
        assertThat(
                        buffer.forEachByte(
                                new ByteProcessor() {
                                    int i = CAPACITY / 4;

                                    @Override
                                    public boolean process(byte value) {
                                        assertThat(value).isEqualTo((byte) (i + 1));
                                        lastIndex.set(i);
                                        i++;
                                        return true;
                                    }
                                }))
                .isEqualTo(-1);

        assertThat(lastIndex).hasValue(CAPACITY * 3 / 4 - 1);
    }

    @Test
    void testForEachByteAbort() {
        buffer.clear();
        for (int i = 0; i < CAPACITY; i++) {
            buffer.writeByte(i + 1);
        }

        final int stop = CAPACITY / 2;
        assertThat(
                        buffer.forEachByte(
                                CAPACITY / 3,
                                CAPACITY / 3,
                                new ByteProcessor() {
                                    int i = CAPACITY / 3;

                                    @Override
                                    public boolean process(byte value) {
                                        assertThat(value).isEqualTo((byte) (i + 1));
                                        if (i == stop) {
                                            return false;
                                        }

                                        i++;
                                        return true;
                                    }
                                }))
                .isEqualTo(stop);
    }

    @Test
    void testForEachByteDesc() {
        buffer.clear();
        for (int i = 0; i < CAPACITY; i++) {
            buffer.writeByte(i + 1);
        }

        final AtomicInteger lastIndex = new AtomicInteger();
        assertThat(
                        buffer.forEachByteDesc(
                                CAPACITY / 4,
                                CAPACITY * 2 / 4,
                                new ByteProcessor() {
                                    int i = CAPACITY * 3 / 4 - 1;

                                    @Override
                                    public boolean process(byte value) {
                                        assertThat(value).isEqualTo((byte) (i + 1));
                                        lastIndex.set(i);
                                        i--;
                                        return true;
                                    }
                                }))
                .isEqualTo(-1);

        assertThat(lastIndex).hasValue(CAPACITY / 4);
    }

    @Test
    void testInternalNioBuffer() {
        testInternalNioBuffer(128);
        testInternalNioBuffer(1024);
        testInternalNioBuffer(4 * 1024);
        testInternalNioBuffer(64 * 1024);
        testInternalNioBuffer(32 * 1024 * 1024);
        testInternalNioBuffer(64 * 1024 * 1024);
    }

    private void testInternalNioBuffer(int a) {
        ByteBuf buffer = newBuffer(2);
        ByteBuffer buf = buffer.internalNioBuffer(buffer.readerIndex(), 1);
        assertThat(buf.remaining()).isOne();

        byte[] data = new byte[a];
        PlatformDependent.threadLocalRandom().nextBytes(data);
        buffer.writeBytes(data);

        buf = buffer.internalNioBuffer(buffer.readerIndex(), a);
        assertThat(buf.remaining()).isEqualTo(a);

        for (int i = 0; i < a; i++) {
            assertThat(buf.get()).isEqualTo(data[i]);
        }
        assertThat(buf.hasRemaining()).isFalse();
        buffer.release();
    }

    @Test
    void testDuplicateReadGatheringByteChannelMultipleThreads() throws Exception {
        testReadGatheringByteChannelMultipleThreads(false);
    }

    @Test
    void testSliceReadGatheringByteChannelMultipleThreads() throws Exception {
        testReadGatheringByteChannelMultipleThreads(true);
    }

    private void testReadGatheringByteChannelMultipleThreads(final boolean slice) throws Exception {
        final byte[] bytes = new byte[8];
        random.nextBytes(bytes);

        final ByteBuf buffer = newBuffer(8);
        buffer.writeBytes(bytes);
        final CountDownLatch latch = new CountDownLatch(60000);
        final CyclicBarrier barrier = new CyclicBarrier(11);
        for (int i = 0; i < 10; i++) {
            new Thread(
                            () -> {
                                while (latch.getCount() > 0) {
                                    ByteBuf buf;
                                    if (slice) {
                                        buf = buffer.slice();
                                    } else {
                                        buf = buffer.duplicate();
                                    }
                                    TestGatheringByteChannel channel =
                                            new TestGatheringByteChannel();

                                    while (buf.isReadable()) {
                                        try {
                                            buf.readBytes(channel, buf.readableBytes());
                                        } catch (IOException e) {
                                            // Never happens
                                            return;
                                        }
                                    }
                                    assertThat(channel.writtenBytes()).isEqualTo(bytes);
                                    latch.countDown();
                                }
                                try {
                                    barrier.await();
                                } catch (Exception e) {
                                    // ignore
                                }
                            })
                    .start();
        }
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        barrier.await(5, TimeUnit.SECONDS);
        buffer.release();
    }

    @Test
    void testDuplicateReadOutputStreamMultipleThreads() throws Exception {
        testReadOutputStreamMultipleThreads(false);
    }

    @Test
    void testSliceReadOutputStreamMultipleThreads() throws Exception {
        testReadOutputStreamMultipleThreads(true);
    }

    private void testReadOutputStreamMultipleThreads(final boolean slice) throws Exception {
        final byte[] bytes = new byte[8];
        random.nextBytes(bytes);

        final ByteBuf buffer = newBuffer(8);
        buffer.writeBytes(bytes);
        final CountDownLatch latch = new CountDownLatch(60000);
        final CyclicBarrier barrier = new CyclicBarrier(11);
        for (int i = 0; i < 10; i++) {
            new Thread(
                            () -> {
                                while (latch.getCount() > 0) {
                                    ByteBuf buf;
                                    if (slice) {
                                        buf = buffer.slice();
                                    } else {
                                        buf = buffer.duplicate();
                                    }
                                    ByteArrayOutputStream out = new ByteArrayOutputStream();

                                    while (buf.isReadable()) {
                                        try {
                                            buf.readBytes(out, buf.readableBytes());
                                        } catch (IOException e) {
                                            // Never happens
                                            return;
                                        }
                                    }
                                    assertThat(out.toByteArray()).isEqualTo(bytes);
                                    latch.countDown();
                                }
                                try {
                                    barrier.await();
                                } catch (Exception e) {
                                    // ignore
                                }
                            })
                    .start();
        }
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        barrier.await(5, TimeUnit.SECONDS);
        buffer.release();
    }

    @Test
    void testDuplicateBytesInArrayMultipleThreads() throws Exception {
        testBytesInArrayMultipleThreads(false);
    }

    @Test
    void testSliceBytesInArrayMultipleThreads() throws Exception {
        testBytesInArrayMultipleThreads(true);
    }

    private void testBytesInArrayMultipleThreads(final boolean slice) throws Exception {
        final byte[] bytes = new byte[8];
        random.nextBytes(bytes);

        final ByteBuf buffer = newBuffer(8);
        buffer.writeBytes(bytes);
        final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(60000);
        final CyclicBarrier barrier = new CyclicBarrier(11);
        for (int i = 0; i < 10; i++) {
            new Thread(
                            () -> {
                                while (cause.get() == null && latch.getCount() > 0) {
                                    ByteBuf buf;
                                    if (slice) {
                                        buf = buffer.slice();
                                    } else {
                                        buf = buffer.duplicate();
                                    }

                                    byte[] array = new byte[8];
                                    buf.readBytes(array);

                                    assertThat(array).isEqualTo(bytes);

                                    Arrays.fill(array, (byte) 0);
                                    buf.getBytes(0, array);
                                    assertThat(array).isEqualTo(bytes);

                                    latch.countDown();
                                }
                                try {
                                    barrier.await();
                                } catch (Exception e) {
                                    // ignore
                                }
                            })
                    .start();
        }
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        barrier.await(5, TimeUnit.SECONDS);
        assertThat(cause.get()).isNull();
        buffer.release();
    }

    @Test
    void readByteThrowsIndexOutOfBoundsException() {
        final ByteBuf buffer = newBuffer(8);
        try {
            buffer.writeByte(0);
            assertThat(buffer.readByte()).isZero();
            assertThatThrownBy(buffer::readByte).isInstanceOf(IndexOutOfBoundsException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    @SuppressWarnings("ForLoopThatDoesntUseLoopVariable")
    void testNioBufferExposeOnlyRegion() {
        final ByteBuf buffer = newBuffer(8);
        byte[] data = new byte[8];
        random.nextBytes(data);
        buffer.writeBytes(data);

        ByteBuffer nioBuf = buffer.nioBuffer(1, data.length - 2);
        assertThat(nioBuf.position()).isZero();
        assertThat(nioBuf.remaining()).isEqualTo(6);

        for (int i = 1; nioBuf.hasRemaining(); i++) {
            assertThat(nioBuf.get()).isEqualTo(data[i]);
        }
        buffer.release();
    }

    @Test
    void ensureWritableWithForceDoesNotThrow() {
        ensureWritableDoesNotThrow(true);
    }

    @Test
    void ensureWritableWithOutForceDoesNotThrow() {
        ensureWritableDoesNotThrow(false);
    }

    private void ensureWritableDoesNotThrow(boolean force) {
        final ByteBuf buffer = newBuffer(8);
        buffer.writerIndex(buffer.capacity());
        buffer.ensureWritable(8, force);
        buffer.release();
    }

    // See:
    // - https://github.com/netty/netty/issues/2587
    // - https://github.com/netty/netty/issues/2580
    @Test
    void testLittleEndianWithExpand() {
        ByteBuf buffer = newBuffer(0).order(LITTLE_ENDIAN);
        buffer.writeInt(0x12345678);
        assertThat(ByteBufUtil.hexDump(buffer)).isEqualTo("78563412");
        buffer.release();
    }

    private ByteBuf releasedBuffer() {
        ByteBuf buffer = newBuffer(8);

        // Clear the buffer so we are sure the reader and writer indices are 0.
        // This is important as we may return a slice from newBuffer(...).
        buffer.clear();
        assertThat(buffer.release()).isTrue();
        return buffer;
    }

    @Test
    void testDiscardReadBytesAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().discardReadBytes())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testDiscardSomeReadBytesAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().discardSomeReadBytes())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testEnsureWritableAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().ensureWritable(16))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetBooleanAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getBoolean(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getByte(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetUnsignedByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getUnsignedByte(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetShortAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getShort(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetShortLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getShortLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetUnsignedShortAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getUnsignedShort(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetUnsignedShortLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getUnsignedShortLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetMediumAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getMedium(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetMediumLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getMediumLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetUnsignedMediumAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getUnsignedMedium(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetIntAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getInt(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetIntLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getIntLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetUnsignedIntAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getUnsignedInt(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetUnsignedIntLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getUnsignedIntLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetLongAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getLong(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetLongLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getLongLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetCharAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getChar(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetFloatAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getFloat(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetFloatLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getFloatLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetDoubleAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getDouble(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetDoubleLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().getDoubleLE(0))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetBytesAfterRelease() {
        ByteBuf buffer = buffer(8);
        try {
            assertThatThrownBy(() -> releasedBuffer().getBytes(0, buffer))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testGetBytesAfterRelease2() {
        ByteBuf buffer = buffer();
        try {
            assertThatThrownBy(() -> releasedBuffer().getBytes(0, buffer, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testGetBytesAfterRelease3() {
        ByteBuf buffer = buffer();
        try {
            assertThatThrownBy(() -> releasedBuffer().getBytes(0, buffer, 0, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testGetBytesAfterRelease4() {
        assertThatThrownBy(() -> releasedBuffer().getBytes(0, new byte[8]))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetBytesAfterRelease5() {
        assertThatThrownBy(() -> releasedBuffer().getBytes(0, new byte[8], 0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetBytesAfterRelease6() {
        assertThatThrownBy(() -> releasedBuffer().getBytes(0, ByteBuffer.allocate(8)))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetBytesAfterRelease7() {
        assertThatThrownBy(() -> releasedBuffer().getBytes(0, new ByteArrayOutputStream(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testGetBytesAfterRelease8() {
        assertThatThrownBy(() -> releasedBuffer().getBytes(0, new DevNullGatheringByteChannel(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetBooleanAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setBoolean(0, true))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setByte(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetShortAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setShort(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetShortLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setShortLE(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetMediumAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setMedium(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetMediumLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setMediumLE(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetIntAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setInt(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetIntLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setIntLE(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetLongAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setLong(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetLongLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setLongLE(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetCharAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setChar(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetFloatAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setFloat(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetDoubleAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setDouble(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetBytesAfterRelease() {
        ByteBuf buffer = buffer();
        try {
            assertThatThrownBy(() -> releasedBuffer().setBytes(0, buffer))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testSetBytesAfterRelease2() {
        ByteBuf buffer = buffer();
        try {
            assertThatThrownBy(() -> releasedBuffer().setBytes(0, buffer, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testSetBytesAfterRelease3() {
        ByteBuf buffer = buffer();
        try {
            assertThatThrownBy(() -> releasedBuffer().setBytes(0, buffer, 0, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testSetUsAsciiCharSequenceAfterRelease() {
        assertThatThrownBy(() -> testSetCharSequenceAfterRelease0(CharsetUtil.US_ASCII))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetIso88591CharSequenceAfterRelease() {
        assertThatThrownBy(() -> testSetCharSequenceAfterRelease0(CharsetUtil.ISO_8859_1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetUtf8CharSequenceAfterRelease() {
        assertThatThrownBy(() -> testSetCharSequenceAfterRelease0(CharsetUtil.UTF_8))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetUtf16CharSequenceAfterRelease() {
        assertThatThrownBy(() -> testSetCharSequenceAfterRelease0(CharsetUtil.UTF_16))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    private void testSetCharSequenceAfterRelease0(Charset charset) {
        releasedBuffer().setCharSequence(0, "x", charset);
    }

    @Test
    void testSetBytesAfterRelease4() {
        assertThatThrownBy(() -> releasedBuffer().setBytes(0, new byte[8]))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetBytesAfterRelease5() {
        assertThatThrownBy(() -> releasedBuffer().setBytes(0, new byte[8], 0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetBytesAfterRelease6() {
        assertThatThrownBy(() -> releasedBuffer().setBytes(0, ByteBuffer.allocate(8)))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetBytesAfterRelease7() {
        assertThatThrownBy(
                        () ->
                                releasedBuffer()
                                        .setBytes(0, new ByteArrayInputStream(new byte[8]), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetBytesAfterRelease8() {
        assertThatThrownBy(() -> releasedBuffer().setBytes(0, new TestScatteringByteChannel(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSetZeroAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().setZero(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBooleanAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readBoolean())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readByte())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedByte())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadShortAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readShort())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadShortLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readShortLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedShortAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedShort())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedShortLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedShortLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadMediumAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readMedium())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadMediumLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readMediumLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedMediumAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedMedium())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedMediumLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedMediumLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadIntAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readInt())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadIntLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readIntLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedIntAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedInt())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadUnsignedIntLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readUnsignedIntLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadLongAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readLong())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadLongLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readLongLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadCharAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readChar())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadFloatAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readFloat())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadFloatLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readFloatLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadDoubleAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readDouble())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadDoubleLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readDoubleLE())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease2() {
        ByteBuf buffer = buffer(8);
        try {
            assertThatThrownBy(() -> releasedBuffer().readBytes(buffer))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testReadBytesAfterRelease3() {
        ByteBuf buffer = buffer(8);
        try {
            assertThatThrownBy(() -> releasedBuffer().readBytes(buffer))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testReadBytesAfterRelease4() {
        ByteBuf buffer = buffer(8);
        try {
            assertThatThrownBy(() -> releasedBuffer().readBytes(buffer, 0, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testReadBytesAfterRelease5() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(new byte[8]))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease6() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(new byte[8], 0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease7() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(ByteBuffer.allocate(8)))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease8() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(new ByteArrayOutputStream(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease9() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(new ByteArrayOutputStream(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testReadBytesAfterRelease10() {
        assertThatThrownBy(() -> releasedBuffer().readBytes(new DevNullGatheringByteChannel(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteBooleanAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeBoolean(true))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeByte(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteShortAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeShort(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteShortLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeShortLE(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteMediumAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeMedium(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteMediumLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeMediumLE(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteIntAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeInt(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteIntLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeIntLE(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteLongAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeLong(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteLongLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeLongLE(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteCharAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeChar(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteFloatAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeFloat(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteFloatLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeFloatLE(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteDoubleAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeDouble(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteDoubleLEAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeDoubleLE(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteBytesAfterRelease() {
        ByteBuf buffer = buffer(8);
        try {
            assertThatThrownBy(() -> releasedBuffer().writeBytes(buffer))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testWriteBytesAfterRelease2() {
        ByteBuf buffer = copiedBuffer(new byte[8]);
        try {
            assertThatThrownBy(() -> releasedBuffer().writeBytes(buffer, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testWriteBytesAfterRelease3() {
        ByteBuf buffer = buffer(8);
        try {
            assertThatThrownBy(() -> releasedBuffer().writeBytes(buffer, 0, 1))
                    .isInstanceOf(IllegalReferenceCountException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testWriteBytesAfterRelease4() {
        assertThatThrownBy(() -> releasedBuffer().writeBytes(new byte[8]))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteBytesAfterRelease5() {
        assertThatThrownBy(() -> releasedBuffer().writeBytes(new byte[8], 0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteBytesAfterRelease6() {
        assertThatThrownBy(() -> releasedBuffer().writeBytes(ByteBuffer.allocate(8)))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteBytesAfterRelease7() {
        assertThatThrownBy(
                        () -> releasedBuffer().writeBytes(new ByteArrayInputStream(new byte[8]), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteBytesAfterRelease8() {
        assertThatThrownBy(() -> releasedBuffer().writeBytes(new TestScatteringByteChannel(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteZeroAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().writeZero(1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteUsAsciiCharSequenceAfterRelease() {
        assertThatThrownBy(() -> testWriteCharSequenceAfterRelease0(CharsetUtil.US_ASCII))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteIso88591CharSequenceAfterRelease() {
        assertThatThrownBy(() -> testWriteCharSequenceAfterRelease0(CharsetUtil.ISO_8859_1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteUtf8CharSequenceAfterRelease() {
        assertThatThrownBy(() -> testWriteCharSequenceAfterRelease0(CharsetUtil.UTF_8))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testWriteUtf16CharSequenceAfterRelease() {
        assertThatThrownBy(() -> testWriteCharSequenceAfterRelease0(CharsetUtil.UTF_16))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    private void testWriteCharSequenceAfterRelease0(Charset charset) {
        releasedBuffer().writeCharSequence("x", charset);
    }

    @Test
    void testForEachByteAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().forEachByte(new TestByteProcessor()))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testForEachByteAfterRelease1() {
        assertThatThrownBy(() -> releasedBuffer().forEachByte(0, 1, new TestByteProcessor()))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testForEachByteDescAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().forEachByteDesc(new TestByteProcessor()))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testForEachByteDescAfterRelease1() {
        assertThatThrownBy(() -> releasedBuffer().forEachByteDesc(0, 1, new TestByteProcessor()))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testCopyAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().copy())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testCopyAfterRelease1() {
        assertThatThrownBy(() -> releasedBuffer().copy())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testNioBufferAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().nioBuffer())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testNioBufferAfterRelease1() {
        assertThatThrownBy(() -> releasedBuffer().nioBuffer(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testInternalNioBufferAfterRelease() {
        ByteBuf releasedBuffer = releasedBuffer();
        assertThatThrownBy(() -> releasedBuffer.internalNioBuffer(releasedBuffer.readerIndex(), 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testNioBuffersAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().nioBuffers())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testNioBuffersAfterRelease2() {
        assertThatThrownBy(() -> releasedBuffer().nioBuffers(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testArrayAfterRelease() {
        ByteBuf buf = releasedBuffer();
        if (buf.hasArray()) {
            assertThatThrownBy(buf::array).isInstanceOf(IllegalReferenceCountException.class);
        }
    }

    @Test
    void testMemoryAddressAfterRelease() {
        ByteBuf buf = releasedBuffer();
        if (buf.hasMemoryAddress()) {
            assertThatThrownBy(buf::memoryAddress)
                    .isInstanceOf(IllegalReferenceCountException.class);
        }
    }

    @Test
    void testSliceAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().slice())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testSliceAfterRelease2() {
        assertThatThrownBy(() -> releasedBuffer().slice(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    private static void assertSliceFailAfterRelease(ByteBuf... bufs) {
        for (ByteBuf buf : bufs) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
        for (ByteBuf buf : bufs) {
            assertThat(buf.refCnt()).isZero();
            assertThatThrownBy(buf::slice).isInstanceOf(IllegalReferenceCountException.class);
        }
    }

    @Test
    void testSliceAfterReleaseRetainedSlice() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        assertSliceFailAfterRelease(buf, buf2);
    }

    @Test
    void testSliceAfterReleaseRetainedSliceDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        ByteBuf buf3 = buf2.duplicate();
        assertSliceFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testSliceAfterReleaseRetainedSliceRetainedDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        ByteBuf buf3 = buf2.retainedDuplicate();
        assertSliceFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testSliceAfterReleaseRetainedDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        assertSliceFailAfterRelease(buf, buf2);
    }

    @Test
    void testSliceAfterReleaseRetainedDuplicateSlice() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        ByteBuf buf3 = buf2.slice(0, 1);
        assertSliceFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testRetainedSliceAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().retainedSlice())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testRetainedSliceAfterRelease2() {
        assertThatThrownBy(() -> releasedBuffer().retainedSlice(0, 1))
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    private static void assertRetainedSliceFailAfterRelease(ByteBuf... bufs) {
        for (ByteBuf buf : bufs) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
        for (ByteBuf buf : bufs) {
            assertThat(buf.refCnt()).isZero();
            assertThatThrownBy(buf::retainedSlice)
                    .isInstanceOf(IllegalReferenceCountException.class);
        }
    }

    @Test
    void testRetainedSliceAfterReleaseRetainedSlice() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        assertRetainedSliceFailAfterRelease(buf, buf2);
    }

    @Test
    void testRetainedSliceAfterReleaseRetainedSliceDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        ByteBuf buf3 = buf2.duplicate();
        assertRetainedSliceFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testRetainedSliceAfterReleaseRetainedSliceRetainedDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        ByteBuf buf3 = buf2.retainedDuplicate();
        assertRetainedSliceFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testRetainedSliceAfterReleaseRetainedDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        assertRetainedSliceFailAfterRelease(buf, buf2);
    }

    @Test
    void testRetainedSliceAfterReleaseRetainedDuplicateSlice() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        ByteBuf buf3 = buf2.slice(0, 1);
        assertRetainedSliceFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testDuplicateAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().duplicate())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    @Test
    void testRetainedDuplicateAfterRelease() {
        assertThatThrownBy(() -> releasedBuffer().retainedDuplicate())
                .isInstanceOf(IllegalReferenceCountException.class);
    }

    private static void assertDuplicateFailAfterRelease(ByteBuf... bufs) {
        for (ByteBuf buf : bufs) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
        for (ByteBuf buf : bufs) {
            assertThat(buf.refCnt()).isZero();
            assertThatThrownBy(buf::duplicate).isInstanceOf(IllegalReferenceCountException.class);
        }
    }

    @Test
    void testDuplicateAfterReleaseRetainedSliceDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        ByteBuf buf3 = buf2.duplicate();
        assertDuplicateFailAfterRelease(buf, buf2, buf3);
    }

    @Test
    void testDuplicateAfterReleaseRetainedDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        assertDuplicateFailAfterRelease(buf, buf2);
    }

    @Test
    void testDuplicateAfterReleaseRetainedDuplicateSlice() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        ByteBuf buf3 = buf2.slice(0, 1);
        assertDuplicateFailAfterRelease(buf, buf2, buf3);
    }

    private static void assertRetainedDuplicateFailAfterRelease(ByteBuf... bufs) {
        for (ByteBuf buf : bufs) {
            if (buf.refCnt() > 0) {
                buf.release();
            }
        }
        for (ByteBuf buf : bufs) {
            assertThat(buf.refCnt()).isZero();
            assertThatThrownBy(buf::retainedDuplicate)
                    .isInstanceOf(IllegalReferenceCountException.class);
        }
    }

    @Test
    void testRetainedDuplicateAfterReleaseRetainedDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedDuplicate();
        assertRetainedDuplicateFailAfterRelease(buf, buf2);
    }

    @Test
    void testRetainedDuplicateAfterReleaseDuplicate() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.duplicate();
        assertRetainedDuplicateFailAfterRelease(buf, buf2);
    }

    @Test
    void testRetainedDuplicateAfterReleaseRetainedSlice() {
        ByteBuf buf = newBuffer(1);
        ByteBuf buf2 = buf.retainedSlice(0, 1);
        assertRetainedDuplicateFailAfterRelease(buf, buf2);
    }

    @Test
    void testSliceRelease() {
        ByteBuf buf = newBuffer(8);
        assertThat(buf.refCnt()).isOne();
        assertThat(buf.slice().release()).isTrue();
        assertThat(buf.refCnt()).isZero();
    }

    @Test
    void testReadSliceOutOfBounds() {
        assertThatThrownBy(() -> testReadSliceOutOfBounds(false))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testReadRetainedSliceOutOfBounds() {
        assertThatThrownBy(() -> testReadSliceOutOfBounds(true))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    private void testReadSliceOutOfBounds(boolean retainedSlice) {
        ByteBuf buf = newBuffer(100);
        try {
            buf.writeZero(50);
            if (retainedSlice) {
                buf.readRetainedSlice(51);
            } else {
                buf.readSlice(51);
            }
        } finally {
            buf.release();
        }
    }

    @Test
    void testWriteUsAsciiCharSequenceExpand() {
        testWriteCharSequenceExpand(CharsetUtil.US_ASCII);
    }

    @Test
    void testWriteUtf8CharSequenceExpand() {
        testWriteCharSequenceExpand(CharsetUtil.UTF_8);
    }

    @Test
    void testWriteIso88591CharSequenceExpand() {
        testWriteCharSequenceExpand(CharsetUtil.ISO_8859_1);
    }

    @Test
    void testWriteUtf16CharSequenceExpand() {
        testWriteCharSequenceExpand(CharsetUtil.UTF_16);
    }

    private void testWriteCharSequenceExpand(Charset charset) {
        ByteBuf buf = newBuffer(1);
        try {
            int writerIndex = buf.capacity() - 1;
            buf.writerIndex(writerIndex);
            int written = buf.writeCharSequence("AB", charset);
            assertThat(buf.writerIndex() - written).isEqualTo(writerIndex);
        } finally {
            buf.release();
        }
    }

    @Test
    void testSetUsAsciiCharSequenceNoExpand() {
        assertThatThrownBy(() -> testSetCharSequenceNoExpand(CharsetUtil.US_ASCII))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testSetUtf8CharSequenceNoExpand() {
        assertThatThrownBy(() -> testSetCharSequenceNoExpand(CharsetUtil.UTF_8))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testSetIso88591CharSequenceNoExpand() {
        assertThatThrownBy(() -> testSetCharSequenceNoExpand(CharsetUtil.ISO_8859_1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testSetUtf16CharSequenceNoExpand() {
        assertThatThrownBy(() -> testSetCharSequenceNoExpand(CharsetUtil.UTF_16))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    private void testSetCharSequenceNoExpand(Charset charset) {
        ByteBuf buf = newBuffer(1);
        try {
            buf.setCharSequence(0, "AB", charset);
        } finally {
            buf.release();
        }
    }

    @Test
    void testSetUsAsciiCharSequence() {
        testSetGetCharSequence(CharsetUtil.US_ASCII);
    }

    @Test
    void testSetUtf8CharSequence() {
        testSetGetCharSequence(CharsetUtil.UTF_8);
    }

    @Test
    void testSetIso88591CharSequence() {
        testSetGetCharSequence(CharsetUtil.ISO_8859_1);
    }

    @Test
    void testSetUtf16CharSequence() {
        testSetGetCharSequence(CharsetUtil.UTF_16);
    }

    private static final CharBuffer EXTENDED_ASCII_CHARS, ASCII_CHARS;

    static {
        char[] chars = new char[256];
        for (char c = 0; c < chars.length; c++) {
            chars[c] = c;
        }
        EXTENDED_ASCII_CHARS = CharBuffer.wrap(chars);
        ASCII_CHARS = CharBuffer.wrap(chars, 0, 128);
    }

    private void testSetGetCharSequence(Charset charset) {
        ByteBuf buf = newBuffer(1024);
        CharBuffer sequence =
                CharsetUtil.US_ASCII.equals(charset) ? ASCII_CHARS : EXTENDED_ASCII_CHARS;
        int bytes = buf.setCharSequence(1, sequence, charset);
        CharSequence actual = CharBuffer.wrap(buf.getCharSequence(1, bytes, charset));
        assertThat(actual).isEqualTo(sequence);
        buf.release();
    }

    @Test
    void testWriteReadUsAsciiCharSequence() {
        testWriteReadCharSequence(CharsetUtil.US_ASCII);
    }

    @Test
    void testWriteReadUtf8CharSequence() {
        testWriteReadCharSequence(CharsetUtil.UTF_8);
    }

    @Test
    void testWriteReadIso88591CharSequence() {
        testWriteReadCharSequence(CharsetUtil.ISO_8859_1);
    }

    @Test
    void testWriteReadUtf16CharSequence() {
        testWriteReadCharSequence(CharsetUtil.UTF_16);
    }

    private void testWriteReadCharSequence(Charset charset) {
        ByteBuf buf = newBuffer(1024);
        CharBuffer sequence =
                CharsetUtil.US_ASCII.equals(charset) ? ASCII_CHARS : EXTENDED_ASCII_CHARS;
        buf.writerIndex(1);
        int bytes = buf.writeCharSequence(sequence, charset);
        buf.readerIndex(1);
        CharSequence actual = CharBuffer.wrap(buf.readCharSequence(bytes, charset));
        assertThat(actual).isEqualTo(sequence);
        buf.release();
    }

    @Test
    void testRetainedSliceIndexOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(true, true, true))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testRetainedSliceLengthOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(true, true, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testMixedSliceAIndexOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(true, false, true))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testMixedSliceALengthOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(true, false, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testMixedSliceBIndexOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(false, true, true))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testMixedSliceBLengthOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(false, true, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testSliceIndexOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(false, false, true))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testSliceLengthOutOfBounds() {
        assertThatThrownBy(() -> testSliceOutOfBounds(false, false, false))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testRetainedSliceAndRetainedDuplicateContentIsExpected() {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected1 = newBuffer(6).resetWriterIndex();
        ByteBuf expected2 = newBuffer(5).resetWriterIndex();
        ByteBuf expected3 = newBuffer(4).resetWriterIndex();
        ByteBuf expected4 = newBuffer(3).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected1.writeBytes(new byte[] {2, 3, 4, 5, 6, 7});
        expected2.writeBytes(new byte[] {3, 4, 5, 6, 7});
        expected3.writeBytes(new byte[] {4, 5, 6, 7});
        expected4.writeBytes(new byte[] {5, 6, 7});

        ByteBuf slice1 = buf.retainedSlice(buf.readerIndex() + 1, 6);
        assertThat(slice1).isEqualTo(expected1).isEqualTo(buf.slice(buf.readerIndex() + 1, 6));
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        // Advance the reader index on the slice.
        slice1.readByte();

        ByteBuf dup1 = slice1.retainedDuplicate();
        assertThat(dup1).isEqualTo(expected2).isEqualTo(slice1.duplicate());

        // Advance the reader index on dup1.
        dup1.readByte();

        ByteBuf dup2 = dup1.duplicate();
        assertThat(dup2).isEqualTo(expected3);

        // Advance the reader index on dup2.
        dup2.readByte();

        ByteBuf slice2 = dup2.retainedSlice(dup2.readerIndex(), 3);
        assertThat(slice2).isEqualTo(expected4).isEqualTo(dup2.slice(dup2.readerIndex(), 3));

        // Cleanup the expected buffers used for testing.
        assertThat(expected1.release()).isTrue();
        assertThat(expected2.release()).isTrue();
        assertThat(expected3.release()).isTrue();
        assertThat(expected4.release()).isTrue();

        slice2.release();
        dup2.release();

        assertThat(dup2.refCnt()).isEqualTo(slice2.refCnt());
        assertThat(dup1.refCnt()).isEqualTo(dup2.refCnt());

        // The handler is now done with the original slice
        assertThat(slice1.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(slice1.refCnt()).isZero();
        assertThat(slice2.refCnt()).isZero();
        assertThat(dup1.refCnt()).isZero();
        assertThat(dup2.refCnt()).isZero();
    }

    @Test
    void testRetainedDuplicateAndRetainedSliceContentIsExpected() {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected1 = newBuffer(6).resetWriterIndex();
        ByteBuf expected2 = newBuffer(5).resetWriterIndex();
        ByteBuf expected3 = newBuffer(4).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected1.writeBytes(new byte[] {2, 3, 4, 5, 6, 7});
        expected2.writeBytes(new byte[] {3, 4, 5, 6, 7});
        expected3.writeBytes(new byte[] {5, 6, 7});

        ByteBuf dup1 = buf.retainedDuplicate();
        assertThat(dup1).isEqualTo(buf).isEqualTo(buf.slice());
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        // Advance the reader index on the dup.
        dup1.readByte();

        ByteBuf slice1 = dup1.retainedSlice(dup1.readerIndex(), 6);
        assertThat(slice1).isEqualTo(expected1).isEqualTo(slice1.duplicate());

        // Advance the reader index on slice1.
        slice1.readByte();

        ByteBuf dup2 = slice1.duplicate();
        assertThat(dup2).isEqualTo(slice1);

        // Advance the reader index on dup2.
        dup2.readByte();

        ByteBuf slice2 = dup2.retainedSlice(dup2.readerIndex() + 1, 3);
        assertThat(slice2).isEqualTo(expected3).isEqualTo(dup2.slice(dup2.readerIndex() + 1, 3));

        // Cleanup the expected buffers used for testing.
        assertThat(expected1.release()).isTrue();
        assertThat(expected2.release()).isTrue();
        assertThat(expected3.release()).isTrue();

        slice2.release();
        slice1.release();

        assertThat(dup2.refCnt()).isEqualTo(slice2.refCnt());
        assertThat(slice1.refCnt()).isEqualTo(dup2.refCnt());

        // The handler is now done with the original slice
        assertThat(dup1.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(slice1.refCnt()).isZero();
        assertThat(slice2.refCnt()).isZero();
        assertThat(dup1.refCnt()).isZero();
        assertThat(dup2.refCnt()).isZero();
    }

    @Test
    void testRetainedSliceContents() {
        testSliceContents(true);
    }

    @Test
    void testMultipleLevelRetainedSlice1() {
        testMultipleLevelRetainedSliceWithNonRetained(true, true);
    }

    @Test
    void testMultipleLevelRetainedSlice2() {
        testMultipleLevelRetainedSliceWithNonRetained(true, false);
    }

    @Test
    void testMultipleLevelRetainedSlice3() {
        testMultipleLevelRetainedSliceWithNonRetained(false, true);
    }

    @Test
    void testMultipleLevelRetainedSlice4() {
        testMultipleLevelRetainedSliceWithNonRetained(false, false);
    }

    @Test
    void testRetainedSliceReleaseOriginal1() {
        testSliceReleaseOriginal(true, true);
    }

    @Test
    void testRetainedSliceReleaseOriginal2() {
        testSliceReleaseOriginal(true, false);
    }

    @Test
    void testRetainedSliceReleaseOriginal3() {
        testSliceReleaseOriginal(false, true);
    }

    @Test
    void testRetainedSliceReleaseOriginal4() {
        testSliceReleaseOriginal(false, false);
    }

    @Test
    void testRetainedDuplicateReleaseOriginal1() {
        testDuplicateReleaseOriginal(true, true);
    }

    @Test
    void testRetainedDuplicateReleaseOriginal2() {
        testDuplicateReleaseOriginal(true, false);
    }

    @Test
    void testRetainedDuplicateReleaseOriginal3() {
        testDuplicateReleaseOriginal(false, true);
    }

    @Test
    void testRetainedDuplicateReleaseOriginal4() {
        testDuplicateReleaseOriginal(false, false);
    }

    @Test
    void testMultipleRetainedSliceReleaseOriginal1() {
        testMultipleRetainedSliceReleaseOriginal(true, true);
    }

    @Test
    void testMultipleRetainedSliceReleaseOriginal2() {
        testMultipleRetainedSliceReleaseOriginal(true, false);
    }

    @Test
    void testMultipleRetainedSliceReleaseOriginal3() {
        testMultipleRetainedSliceReleaseOriginal(false, true);
    }

    @Test
    void testMultipleRetainedSliceReleaseOriginal4() {
        testMultipleRetainedSliceReleaseOriginal(false, false);
    }

    @Test
    void testMultipleRetainedDuplicateReleaseOriginal1() {
        testMultipleRetainedDuplicateReleaseOriginal(true, true);
    }

    @Test
    void testMultipleRetainedDuplicateReleaseOriginal2() {
        testMultipleRetainedDuplicateReleaseOriginal(true, false);
    }

    @Test
    void testMultipleRetainedDuplicateReleaseOriginal3() {
        testMultipleRetainedDuplicateReleaseOriginal(false, true);
    }

    @Test
    void testMultipleRetainedDuplicateReleaseOriginal4() {
        testMultipleRetainedDuplicateReleaseOriginal(false, false);
    }

    @Test
    void testSliceContents() {
        testSliceContents(false);
    }

    @Test
    void testRetainedDuplicateContents() {
        testDuplicateContents(true);
    }

    @Test
    void testDuplicateContents() {
        testDuplicateContents(false);
    }

    @Test
    void testDuplicateCapacityChange() {
        testDuplicateCapacityChange(false);
    }

    @Test
    void testRetainedDuplicateCapacityChange() {
        testDuplicateCapacityChange(true);
    }

    @Test
    void testSliceCapacityChange() {
        assertThatThrownBy(() -> testSliceCapacityChange(false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRetainedSliceCapacityChange() {
        assertThatThrownBy(() -> testSliceCapacityChange(true))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRetainedSliceUnreleasable1() {
        testRetainedSliceUnreleasable(true, true);
    }

    @Test
    void testRetainedSliceUnreleasable2() {
        testRetainedSliceUnreleasable(true, false);
    }

    @Test
    void testRetainedSliceUnreleasable3() {
        testRetainedSliceUnreleasable(false, true);
    }

    @Test
    void testRetainedSliceUnreleasable4() {
        testRetainedSliceUnreleasable(false, false);
    }

    @Test
    void testReadRetainedSliceUnreleasable1() {
        testReadRetainedSliceUnreleasable(true, true);
    }

    @Test
    void testReadRetainedSliceUnreleasable2() {
        testReadRetainedSliceUnreleasable(true, false);
    }

    @Test
    void testReadRetainedSliceUnreleasable3() {
        testReadRetainedSliceUnreleasable(false, true);
    }

    @Test
    void testReadRetainedSliceUnreleasable4() {
        testReadRetainedSliceUnreleasable(false, false);
    }

    @Test
    void testRetainedDuplicateUnreleasable1() {
        testRetainedDuplicateUnreleasable(true, true);
    }

    @Test
    void testRetainedDuplicateUnreleasable2() {
        testRetainedDuplicateUnreleasable(true, false);
    }

    @Test
    void testRetainedDuplicateUnreleasable3() {
        testRetainedDuplicateUnreleasable(false, true);
    }

    @Test
    void testRetainedDuplicateUnreleasable4() {
        testRetainedDuplicateUnreleasable(false, false);
    }

    private void testRetainedSliceUnreleasable(
            boolean initRetainedSlice, boolean finalRetainedSlice) {
        ByteBuf buf = newBuffer(8);
        ByteBuf buf1 = initRetainedSlice ? buf.retainedSlice() : buf.slice().retain();
        ByteBuf buf2 = unreleasableBuffer(buf1);
        ByteBuf buf3 = finalRetainedSlice ? buf2.retainedSlice() : buf2.slice().retain();
        assertThat(buf3.release()).isFalse();
        assertThat(buf2.release()).isFalse();
        buf1.release();
        assertThat(buf.release()).isTrue();
        assertThat(buf1.refCnt()).isZero();
        assertThat(buf.refCnt()).isZero();
    }

    private void testReadRetainedSliceUnreleasable(
            boolean initRetainedSlice, boolean finalRetainedSlice) {
        ByteBuf buf = newBuffer(8);
        ByteBuf buf1 = initRetainedSlice ? buf.retainedSlice() : buf.slice().retain();
        ByteBuf buf2 = unreleasableBuffer(buf1);
        ByteBuf buf3 =
                finalRetainedSlice
                        ? buf2.readRetainedSlice(buf2.readableBytes())
                        : buf2.readSlice(buf2.readableBytes()).retain();
        assertThat(buf3.release()).isFalse();
        assertThat(buf2.release()).isFalse();
        buf1.release();
        assertThat(buf.release()).isTrue();
        assertThat(buf1.refCnt()).isZero();
        assertThat(buf.refCnt()).isZero();
    }

    private void testRetainedDuplicateUnreleasable(
            boolean initRetainedDuplicate, boolean finalRetainedDuplicate) {
        ByteBuf buf = newBuffer(8);
        ByteBuf buf1 = initRetainedDuplicate ? buf.retainedDuplicate() : buf.duplicate().retain();
        ByteBuf buf2 = unreleasableBuffer(buf1);
        ByteBuf buf3 =
                finalRetainedDuplicate ? buf2.retainedDuplicate() : buf2.duplicate().retain();
        assertThat(buf3.release()).isFalse();
        assertThat(buf2.release()).isFalse();
        buf1.release();
        assertThat(buf.release()).isTrue();
        assertThat(buf1.refCnt()).isZero();
        assertThat(buf.refCnt()).isZero();
    }

    private void testDuplicateCapacityChange(boolean retainedDuplicate) {
        ByteBuf buf = newBuffer(8);
        ByteBuf dup = retainedDuplicate ? buf.retainedDuplicate() : buf.duplicate();
        try {
            dup.capacity(10);
            assertThat(dup.capacity()).isEqualTo(buf.capacity());
            dup.capacity(5);
            assertThat(dup.capacity()).isEqualTo(buf.capacity());
        } finally {
            if (retainedDuplicate) {
                dup.release();
            }
            buf.release();
        }
    }

    private void testSliceCapacityChange(boolean retainedSlice) {
        ByteBuf buf = newBuffer(8);
        ByteBuf slice =
                retainedSlice
                        ? buf.retainedSlice(buf.readerIndex() + 1, 3)
                        : buf.slice(buf.readerIndex() + 1, 3);
        try {
            slice.capacity(10);
        } finally {
            if (retainedSlice) {
                slice.release();
            }
            buf.release();
        }
    }

    private void testSliceOutOfBounds(
            boolean initRetainedSlice, boolean finalRetainedSlice, boolean indexOutOfBounds) {
        ByteBuf buf = newBuffer(8);
        ByteBuf slice =
                initRetainedSlice
                        ? buf.retainedSlice(buf.readerIndex() + 1, 2)
                        : buf.slice(buf.readerIndex() + 1, 2);
        try {
            assertThat(slice.capacity()).isEqualTo(2);
            assertThat(slice.maxCapacity()).isEqualTo(2);
            final int index = indexOutOfBounds ? 3 : 0;
            final int length = indexOutOfBounds ? 0 : 3;
            if (finalRetainedSlice) {
                // This is expected to fail ... so no need to release.
                slice.retainedSlice(index, length);
            } else {
                slice.slice(index, length);
            }
        } finally {
            if (initRetainedSlice) {
                slice.release();
            }
            buf.release();
        }
    }

    private void testSliceContents(boolean retainedSlice) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected = newBuffer(3).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected.writeBytes(new byte[] {4, 5, 6});
        ByteBuf slice =
                retainedSlice
                        ? buf.retainedSlice(buf.readerIndex() + 3, 3)
                        : buf.slice(buf.readerIndex() + 3, 3);
        try {
            assertThat(slice).isEqualTo(slice.duplicate()).isEqualTo(expected);
            ByteBuf b = slice.retainedDuplicate();
            assertThat(slice).isEqualTo(b);

            b.release();
            assertThat(slice).isEqualTo(slice.slice(0, slice.capacity()));
        } finally {
            if (retainedSlice) {
                slice.release();
            }
            buf.release();
            expected.release();
        }
    }

    private void testSliceReleaseOriginal(boolean retainedSlice1, boolean retainedSlice2) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected1 = newBuffer(3).resetWriterIndex();
        ByteBuf expected2 = newBuffer(2).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected1.writeBytes(new byte[] {6, 7, 8});
        expected2.writeBytes(new byte[] {7, 8});
        ByteBuf slice1 =
                retainedSlice1
                        ? buf.retainedSlice(buf.readerIndex() + 5, 3)
                        : buf.slice(buf.readerIndex() + 5, 3).retain();
        assertThat(slice1).isEqualTo(expected1);
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        ByteBuf slice2 =
                retainedSlice2
                        ? slice1.retainedSlice(slice1.readerIndex() + 1, 2)
                        : slice1.slice(slice1.readerIndex() + 1, 2).retain();
        assertThat(slice2).isEqualTo(expected2);

        // Cleanup the expected buffers used for testing.
        assertThat(expected1.release()).isTrue();
        assertThat(expected2.release()).isTrue();

        // The handler created a slice of the slice and is now done with it.
        slice2.release();

        // The handler is now done with the original slice
        assertThat(slice1.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(slice1.refCnt()).isZero();
        assertThat(slice2.refCnt()).isZero();
    }

    private void testMultipleLevelRetainedSliceWithNonRetained(boolean doSlice1, boolean doSlice2) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected1 = newBuffer(6).resetWriterIndex();
        ByteBuf expected2 = newBuffer(4).resetWriterIndex();
        ByteBuf expected3 = newBuffer(2).resetWriterIndex();
        ByteBuf expected4SliceSlice = newBuffer(1).resetWriterIndex();
        ByteBuf expected4DupSlice = newBuffer(1).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected1.writeBytes(new byte[] {2, 3, 4, 5, 6, 7});
        expected2.writeBytes(new byte[] {3, 4, 5, 6});
        expected3.writeBytes(new byte[] {4, 5});
        expected4SliceSlice.writeBytes(new byte[] {5});
        expected4DupSlice.writeBytes(new byte[] {4});

        ByteBuf slice1 = buf.retainedSlice(buf.readerIndex() + 1, 6);
        assertThat(slice1).isEqualTo(expected1);
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        ByteBuf slice2 = slice1.retainedSlice(slice1.readerIndex() + 1, 4);
        assertThat(slice2).isEqualTo(expected2);
        assertThat(slice2).isEqualTo(slice2.duplicate());
        assertThat(slice2).isEqualTo(slice2.slice());

        ByteBuf tmpBuf = slice2.retainedDuplicate();
        assertThat(slice2).isEqualTo(tmpBuf);
        tmpBuf.release();
        tmpBuf = slice2.retainedSlice();
        assertThat(slice2).isEqualTo(tmpBuf);
        tmpBuf.release();

        ByteBuf slice3 = doSlice1 ? slice2.slice(slice2.readerIndex() + 1, 2) : slice2.duplicate();
        if (doSlice1) {
            assertThat(slice3).isEqualTo(expected3);
        } else {
            assertThat(slice3).isEqualTo(expected2);
        }

        ByteBuf slice4 = doSlice2 ? slice3.slice(slice3.readerIndex() + 1, 1) : slice3.duplicate();
        if (doSlice1 && doSlice2) {
            assertThat(slice4).isEqualTo(expected4SliceSlice);
        } else if (doSlice2) {
            assertThat(slice4).isEqualTo(expected4DupSlice);
        } else {
            assertThat(slice3).isEqualTo(slice4);
        }

        // Cleanup the expected buffers used for testing.
        assertThat(expected1.release()).isTrue();
        assertThat(expected2.release()).isTrue();
        assertThat(expected3.release()).isTrue();
        assertThat(expected4SliceSlice.release()).isTrue();
        assertThat(expected4DupSlice.release()).isTrue();

        // Slice 4, 3, and 2 should effectively "share" a reference count.
        slice4.release();
        assertThat(slice2.refCnt()).isEqualTo(slice3.refCnt());
        assertThat(slice4.refCnt()).isEqualTo(slice3.refCnt());

        // Slice 1 should also release the original underlying buffer without throwing exceptions
        assertThat(slice1.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(slice1.refCnt()).isZero();
        assertThat(slice2.refCnt()).isZero();
        assertThat(slice3.refCnt()).isZero();
    }

    private void testDuplicateReleaseOriginal(
            boolean retainedDuplicate1, boolean retainedDuplicate2) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected = newBuffer(8).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
        ByteBuf dup1 = retainedDuplicate1 ? buf.retainedDuplicate() : buf.duplicate().retain();
        assertThat(dup1).isEqualTo(expected);
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        ByteBuf dup2 = retainedDuplicate2 ? dup1.retainedDuplicate() : dup1.duplicate().retain();
        assertThat(dup2).isEqualTo(expected);

        // Cleanup the expected buffers used for testing.
        assertThat(expected.release()).isTrue();

        // The handler created a slice of the slice and is now done with it.
        dup2.release();

        // The handler is now done with the original slice
        assertThat(dup1.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(dup1.refCnt()).isZero();
        assertThat(dup2.refCnt()).isZero();
    }

    private void testMultipleRetainedSliceReleaseOriginal(
            boolean retainedSlice1, boolean retainedSlice2) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected1 = newBuffer(3).resetWriterIndex();
        ByteBuf expected2 = newBuffer(2).resetWriterIndex();
        ByteBuf expected3 = newBuffer(2).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected1.writeBytes(new byte[] {6, 7, 8});
        expected2.writeBytes(new byte[] {7, 8});
        expected3.writeBytes(new byte[] {6, 7});
        ByteBuf slice1 =
                retainedSlice1
                        ? buf.retainedSlice(buf.readerIndex() + 5, 3)
                        : buf.slice(buf.readerIndex() + 5, 3).retain();
        assertThat(slice1).isEqualTo(expected1);
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        ByteBuf slice2 =
                retainedSlice2
                        ? slice1.retainedSlice(slice1.readerIndex() + 1, 2)
                        : slice1.slice(slice1.readerIndex() + 1, 2).retain();
        assertThat(slice2).isEqualTo(expected2);

        // The handler created a slice of the slice and is now done with it.
        slice2.release();

        ByteBuf slice3 = slice1.retainedSlice(slice1.readerIndex(), 2);
        assertThat(slice3).isEqualTo(expected3);

        // The handler created another slice of the slice and is now done with it.
        slice3.release();

        // The handler is now done with the original slice
        assertThat(slice1.release()).isTrue();

        // Cleanup the expected buffers used for testing.
        assertThat(expected1.release()).isTrue();
        assertThat(expected2.release()).isTrue();
        assertThat(expected3.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(slice1.refCnt()).isZero();
        assertThat(slice2.refCnt()).isZero();
        assertThat(slice3.refCnt()).isZero();
    }

    private void testMultipleRetainedDuplicateReleaseOriginal(
            boolean retainedDuplicate1, boolean retainedDuplicate2) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        ByteBuf expected = newBuffer(8).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        expected.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
        ByteBuf dup1 = retainedDuplicate1 ? buf.retainedDuplicate() : buf.duplicate().retain();
        assertThat(dup1).isEqualTo(expected);
        // Simulate a handler that releases the original buffer, and propagates a slice.
        buf.release();

        ByteBuf dup2 = retainedDuplicate2 ? dup1.retainedDuplicate() : dup1.duplicate().retain();
        assertThat(dup2).isEqualTo(expected).isEqualTo(dup2.duplicate()).isEqualTo(dup2.slice());

        ByteBuf tmpBuf = dup2.retainedDuplicate();
        assertThat(dup2).isEqualTo(tmpBuf);
        tmpBuf.release();
        tmpBuf = dup2.retainedSlice();
        assertThat(dup2).isEqualTo(tmpBuf);
        tmpBuf.release();

        // The handler created a slice of the slice and is now done with it.
        dup2.release();

        ByteBuf dup3 = dup1.retainedDuplicate();
        assertThat(dup3).isEqualTo(expected);

        // The handler created another slice of the slice and is now done with it.
        dup3.release();

        // The handler is now done with the original slice
        assertThat(dup1.release()).isTrue();

        // Cleanup the expected buffers used for testing.
        assertThat(expected.release()).isTrue();

        // Reference counting may be shared, or may be independently tracked, but at this point all
        // buffers should
        // be deallocated and have a reference count of 0.
        assertThat(buf.refCnt()).isZero();
        assertThat(dup1.refCnt()).isZero();
        assertThat(dup2.refCnt()).isZero();
        assertThat(dup3.refCnt()).isZero();
    }

    private void testDuplicateContents(boolean retainedDuplicate) {
        ByteBuf buf = newBuffer(8).resetWriterIndex();
        buf.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        ByteBuf dup = retainedDuplicate ? buf.retainedDuplicate() : buf.duplicate();
        try {
            assertThat(dup).isEqualTo(buf).isEqualTo(dup.duplicate());
            ByteBuf b = dup.retainedDuplicate();
            assertThat(dup).isEqualTo(b);
            b.release();
            assertThat(dup).isEqualTo(dup.slice(dup.readerIndex(), dup.readableBytes()));
        } finally {
            if (retainedDuplicate) {
                dup.release();
            }
            buf.release();
        }
    }

    @Test
    void testDuplicateRelease() {
        ByteBuf buf = newBuffer(8);
        assertThat(buf.refCnt()).isOne();
        assertThat(buf.duplicate().release()).isTrue();
        assertThat(buf.refCnt()).isZero();
    }

    // Test-case trying to reproduce:
    // https://github.com/netty/netty/issues/2843
    @Test
    void testRefCnt() throws Exception {
        testRefCnt0(false);
    }

    // Test-case trying to reproduce:
    // https://github.com/netty/netty/issues/2843
    @Test
    void testRefCnt2() throws Exception {
        testRefCnt0(true);
    }

    @Test
    void testEmptyNioBuffers() {
        ByteBuf buffer = newBuffer(8);
        buffer.clear();
        assertThat(buffer.isReadable()).isFalse();
        ByteBuffer[] nioBuffers = buffer.nioBuffers();
        assertThat(nioBuffers).hasSize(1);
        assertThat(nioBuffers[0].hasRemaining()).isFalse();
        buffer.release();
    }

    @Test
    void testGetReadOnlyDirectDst() {
        testGetReadOnlyDst(true);
    }

    @Test
    void testGetReadOnlyHeapDst() {
        testGetReadOnlyDst(false);
    }

    private void testGetReadOnlyDst(boolean direct) {
        byte[] bytes = {'a', 'b', 'c', 'd'};

        ByteBuf buffer = newBuffer(bytes.length);
        buffer.writeBytes(bytes);

        ByteBuffer dst =
                direct
                        ? ByteBuffer.allocateDirect(bytes.length)
                        : ByteBuffer.allocate(bytes.length);
        ByteBuffer readOnlyDst = dst.asReadOnlyBuffer();
        assertThatThrownBy(() -> buffer.getBytes(0, readOnlyDst))
                .isInstanceOf(ReadOnlyBufferException.class);
        assertThat(readOnlyDst.position()).isZero();
        buffer.release();
    }

    @Test
    void testReadBytesAndWriteBytesWithFileChannel() throws IOException {
        File file = File.createTempFile("file-channel", ".tmp");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileChannel channel = randomAccessFile.getChannel();
            // channelPosition should never be changed
            long channelPosition = channel.position();

            byte[] bytes = {'a', 'b', 'c', 'd'};
            int len = bytes.length;
            ByteBuf buffer = newBuffer(len);
            buffer.resetReaderIndex();
            buffer.resetWriterIndex();
            buffer.writeBytes(bytes);

            int oldReaderIndex = buffer.readerIndex();
            assertThat(buffer.readBytes(channel, 10, len)).isEqualTo(len);
            assertThat(buffer.readerIndex()).isEqualTo(oldReaderIndex + len);
            assertThat(channel.position()).isEqualTo(channelPosition);

            ByteBuf buffer2 = newBuffer(len);
            buffer2.resetReaderIndex();
            buffer2.resetWriterIndex();
            int oldWriterIndex = buffer2.writerIndex();
            assertThat(buffer2.writeBytes(channel, 10, len)).isEqualTo(len);
            assertThat(channel.position()).isEqualTo(channelPosition);
            assertThat(buffer2.writerIndex()).isEqualTo(oldWriterIndex + len);
            assertThat((char) buffer2.getByte(0)).isEqualTo('a');
            assertThat((char) buffer2.getByte(1)).isEqualTo('b');
            assertThat((char) buffer2.getByte(2)).isEqualTo('c');
            assertThat((char) buffer2.getByte(3)).isEqualTo('d');
            buffer.release();
            buffer2.release();
        } finally {
            file.delete();
        }
    }

    @Test
    void testGetBytesAndSetBytesWithFileChannel() throws IOException {
        File file = File.createTempFile("file-channel", ".tmp");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            FileChannel channel = randomAccessFile.getChannel();
            // channelPosition should never be changed
            long channelPosition = channel.position();

            byte[] bytes = {'a', 'b', 'c', 'd'};
            int len = bytes.length;
            ByteBuf buffer = newBuffer(len);
            buffer.resetReaderIndex();
            buffer.resetWriterIndex();
            buffer.writeBytes(bytes);

            int oldReaderIndex = buffer.readerIndex();
            assertThat(buffer.getBytes(oldReaderIndex, channel, 10, len)).isEqualTo(len);
            assertThat(buffer.readerIndex()).isEqualTo(oldReaderIndex);
            assertThat(channel.position()).isEqualTo(channelPosition);

            ByteBuf buffer2 = newBuffer(len);
            buffer2.resetReaderIndex();
            buffer2.resetWriterIndex();
            int oldWriterIndex = buffer2.writerIndex();
            assertThat(buffer2.setBytes(oldWriterIndex, channel, 10, len)).isEqualTo(len);
            assertThat(channel.position()).isEqualTo(channelPosition);

            assertThat(buffer2.writerIndex()).isEqualTo(oldWriterIndex);
            assertThat((char) buffer2.getByte(oldWriterIndex)).isEqualTo('a');
            assertThat((char) buffer2.getByte(oldWriterIndex + 1)).isEqualTo('b');
            assertThat((char) buffer2.getByte(oldWriterIndex + 2)).isEqualTo('c');
            assertThat((char) buffer2.getByte(oldWriterIndex + 3)).isEqualTo('d');

            buffer.release();
            buffer2.release();
        } finally {
            file.delete();
        }
    }

    @Test
    void testReadBytes() {
        ByteBuf buffer = newBuffer(8);
        byte[] bytes = new byte[8];
        buffer.writeBytes(bytes);

        ByteBuf buffer2 = buffer.readBytes(4);
        assertThat(buffer2.alloc()).isSameAs(buffer.alloc());
        assertThat(buffer.readerIndex()).isEqualTo(4);
        assertThat(buffer.release()).isTrue();
        assertThat(buffer.refCnt()).isZero();
        assertThat(buffer2.release()).isTrue();
        assertThat(buffer2.refCnt()).isZero();
    }

    @Test
    void testForEachByteDesc2() {
        byte[] expected = {1, 2, 3, 4};
        ByteBuf buf = newBuffer(expected.length);
        try {
            buf.writeBytes(expected);
            final byte[] bytes = new byte[expected.length];
            int i =
                    buf.forEachByteDesc(
                            new ByteProcessor() {
                                private int index = bytes.length - 1;

                                @Override
                                public boolean process(byte value) {
                                    bytes[index--] = value;
                                    return true;
                                }
                            });
            assertThat(i).isEqualTo(-1);
            assertThat(bytes).isEqualTo(expected);
        } finally {
            buf.release();
        }
    }

    @Test
    void testForEachByte2() {
        byte[] expected = {1, 2, 3, 4};
        ByteBuf buf = newBuffer(expected.length);
        try {
            buf.writeBytes(expected);
            final byte[] bytes = new byte[expected.length];
            int i =
                    buf.forEachByte(
                            new ByteProcessor() {
                                private int index;

                                @Override
                                public boolean process(byte value) {
                                    bytes[index++] = value;
                                    return true;
                                }
                            });
            assertThat(i).isEqualTo(-1);
            assertThat(bytes).isEqualTo(expected);
        } finally {
            buf.release();
        }
    }

    @Test
    void testGetBytesByteBuffer() {
        byte[] bytes = {'a', 'b', 'c', 'd', 'e', 'f', 'g'};
        // Ensure destination buffer is bigger then what is in the ByteBuf.
        ByteBuffer nioBuffer = ByteBuffer.allocate(bytes.length + 1);
        ByteBuf buffer = newBuffer(bytes.length);
        try {
            buffer.writeBytes(bytes);
            assertThatThrownBy(() -> buffer.getBytes(buffer.readerIndex(), nioBuffer))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        } finally {
            buffer.release();
        }
    }

    private void testRefCnt0(final boolean parameter) throws Exception {
        for (int i = 0; i < 10; i++) {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch innerLatch = new CountDownLatch(1);

            final ByteBuf buffer = newBuffer(4);
            assertThat(buffer.refCnt()).isOne();
            final AtomicInteger cnt = new AtomicInteger(Integer.MAX_VALUE);
            Thread t1 =
                    new Thread(
                            new Runnable() {
                                @Override
                                public void run() {
                                    boolean released;
                                    if (parameter) {
                                        released = buffer.release(buffer.refCnt());
                                    } else {
                                        released = buffer.release();
                                    }
                                    assertThat(released).isTrue();
                                    Thread t2 =
                                            new Thread(
                                                    () -> {
                                                        cnt.set(buffer.refCnt());
                                                        latch.countDown();
                                                    });
                                    t2.start();
                                    try {
                                        // Keep Thread alive a bit so the ThreadLocal caches are not
                                        // freed
                                        innerLatch.await();
                                    } catch (InterruptedException ignore) {
                                        // ignore
                                    }
                                }
                            });
            t1.start();

            latch.await();
            assertThat(cnt).hasValue(0);
            innerLatch.countDown();
        }
    }

    static final class TestGatheringByteChannel implements GatheringByteChannel {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final WritableByteChannel channel = Channels.newChannel(out);
        private final int limit;

        TestGatheringByteChannel(int limit) {
            this.limit = limit;
        }

        TestGatheringByteChannel() {
            this(Integer.MAX_VALUE);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            long written = 0;
            for (; offset < length; offset++) {
                written += write(srcs[offset]);
                if (written >= limit) {
                    break;
                }
            }
            return written;
        }

        @Override
        public long write(ByteBuffer[] srcs) throws IOException {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int oldLimit = src.limit();
            if (limit < src.remaining()) {
                src.limit(src.position() + limit);
            }
            int w = channel.write(src);
            src.limit(oldLimit);
            return w;
        }

        @Override
        public boolean isOpen() {
            return channel.isOpen();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        public byte[] writtenBytes() {
            return out.toByteArray();
        }
    }

    private static final class DevNullGatheringByteChannel implements GatheringByteChannel {
        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestScatteringByteChannel implements ScatteringByteChannel {
        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long read(ByteBuffer[] dsts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(ByteBuffer dst) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestByteProcessor implements ByteProcessor {
        @Override
        public boolean process(byte value) {
            return true;
        }
    }

    @Test
    void testCapacityEnforceMaxCapacity() {
        ByteBuf buffer = newBuffer(3, 13);
        assertThat(buffer.maxCapacity()).isEqualTo(13);
        assertThat(buffer.capacity()).isEqualTo(3);
        try {
            assertThatThrownBy(() -> buffer.capacity(14))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testCapacityNegative() {
        ByteBuf buffer = newBuffer(3, 13);
        assertThat(buffer.maxCapacity()).isEqualTo(13);
        assertThat(buffer.capacity()).isEqualTo(3);
        try {
            assertThatThrownBy(() -> buffer.capacity(-1))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testCapacityDecrease() {
        ByteBuf buffer = newBuffer(3, 13);
        assertThat(buffer.maxCapacity()).isEqualTo(13);
        assertThat(buffer.capacity()).isEqualTo(3);
        try {
            buffer.capacity(2);
            assertThat(buffer.capacity()).isEqualTo(2);
            assertThat(buffer.maxCapacity()).isEqualTo(13);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testCapacityIncrease() {
        ByteBuf buffer = newBuffer(3, 13);
        assertThat(buffer.maxCapacity()).isEqualTo(13);
        assertThat(buffer.capacity()).isEqualTo(3);
        try {
            buffer.capacity(4);
            assertThat(buffer.capacity()).isEqualTo(4);
            assertThat(buffer.maxCapacity()).isEqualTo(13);
        } finally {
            buffer.release();
        }
    }

    @Test
    void testReaderIndexLargerThanWriterIndex() {
        String content1 = "hello";
        String content2 = "world";
        int length = content1.length() + content2.length();
        ByteBuf buffer = newBuffer(length);
        buffer.setIndex(0, 0);
        buffer.writeCharSequence(content1, CharsetUtil.US_ASCII);
        buffer.markWriterIndex();
        buffer.skipBytes(content1.length());
        buffer.writeCharSequence(content2, CharsetUtil.US_ASCII);
        buffer.skipBytes(content2.length());
        assertThat(buffer.readerIndex()).isLessThanOrEqualTo(buffer.writerIndex());

        try {
            assertThatThrownBy(buffer::resetWriterIndex)
                    .isInstanceOf(IndexOutOfBoundsException.class);
        } finally {
            buffer.release();
        }
    }
}
