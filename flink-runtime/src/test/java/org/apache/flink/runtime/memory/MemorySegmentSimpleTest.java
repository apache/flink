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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.data.Offset.offset;

/** Test reading and writing primitive types to {@link MemorySegment}. */
class MemorySegmentSimpleTest {

    public static final long RANDOM_SEED = 643196033469871L;

    public static final int MANAGED_MEMORY_SIZE = 1024 * 1024 * 16;

    public static final int PAGE_SIZE = 1024 * 512;

    private MemoryManager manager;

    private MemorySegment segment;

    private Random random;

    @BeforeEach
    void setUp() throws Exception {
        this.manager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(MANAGED_MEMORY_SIZE)
                        .setPageSize(PAGE_SIZE)
                        .build();
        this.segment = manager.allocatePages(new DummyInvokable(), 1).get(0);
        this.random = new Random(RANDOM_SEED);
    }

    @AfterEach
    void tearDown() {
        this.manager.release(this.segment);
        this.random = null;
        this.segment = null;

        assertThat(this.manager.verifyEmpty())
                .as("Not all memory has been properly released.")
                .isTrue();
        this.manager = null;
    }

    @Test
    void bulkByteAccess() {

        // test exceptions
        {
            byte[] bytes = new byte[PAGE_SIZE / 4];

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.put(3 * (PAGE_SIZE / 4) + 1, bytes));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(
                            () -> segment.put(7 * (PAGE_SIZE / 8) + 1, bytes, 0, bytes.length / 2));
        }

        // test expected correct behavior with default offset / length
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            byte[] src = new byte[PAGE_SIZE / 8];
            for (int i = 0; i < 8; i++) {
                random.nextBytes(src);
                segment.put(i * (PAGE_SIZE / 8), src);
            }

            random.setSeed(seed);
            byte[] expected = new byte[PAGE_SIZE / 8];
            byte[] actual = new byte[PAGE_SIZE / 8];
            for (int i = 0; i < 8; i++) {
                random.nextBytes(expected);
                segment.get(i * (PAGE_SIZE / 8), actual);

                assertThat(actual).isEqualTo(expected);
            }
        }

        // test expected correct behavior with specific offset / length
        {
            byte[] expected = new byte[PAGE_SIZE];
            random.nextBytes(expected);

            for (int i = 0; i < 16; i++) {
                segment.put(i * (PAGE_SIZE / 16), expected, i * (PAGE_SIZE / 16), PAGE_SIZE / 16);
            }

            byte[] actual = new byte[PAGE_SIZE];
            for (int i = 0; i < 16; i++) {
                segment.get(i * (PAGE_SIZE / 16), actual, i * (PAGE_SIZE / 16), PAGE_SIZE / 16);
            }

            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void byteAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.put(-1, (byte) 0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.put(PAGE_SIZE, (byte) 0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.get(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.get(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i < PAGE_SIZE; i++) {
                segment.put(i, (byte) random.nextInt());
            }

            random.setSeed(seed);
            for (int i = 0; i < PAGE_SIZE; i++) {
                assertThat(segment.get(i)).isEqualTo((byte) random.nextInt());
            }
        }
    }

    @Test
    void booleanAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putBoolean(-1, false));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putBoolean(PAGE_SIZE, false));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getBoolean(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getBoolean(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i < PAGE_SIZE; i++) {
                segment.putBoolean(i, random.nextBoolean());
            }

            random.setSeed(seed);
            for (int i = 0; i < PAGE_SIZE; i++) {
                assertThat(segment.getBoolean(i)).isEqualTo(random.nextBoolean());
            }
        }
    }

    @Test
    void charAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putChar(-1, 'a'));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putChar(PAGE_SIZE, 'a'));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getChar(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getChar(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
                segment.putChar(i, (char) ('a' + random.nextInt(26)));
            }

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
                assertThat(segment.getChar(i)).isEqualTo((char) ('a' + random.nextInt(26)));
            }
        }
    }

    @Test
    void doubleAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putDouble(-1, 0.0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putDouble(PAGE_SIZE, 0.0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getDouble(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getDouble(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
                segment.putDouble(i, random.nextDouble());
            }

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
                assertThat(segment.getDouble(i)).isCloseTo(random.nextDouble(), offset(0.0));
            }
        }
    }

    // @Test
    void floatAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putFloat(-1, 0.0f));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putFloat(PAGE_SIZE, 0.0f));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getFloat(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getFloat(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
                segment.putFloat(i, random.nextFloat());
            }

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
                assertThat(segment.getFloat(i)).isCloseTo(random.nextFloat(), offset(0.0f));
            }
        }
    }

    @Test
    void longAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putLong(-1, 0L));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putLong(PAGE_SIZE, 0L));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getLong(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getLong(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
                segment.putLong(i, random.nextLong());
            }

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 8; i += 8) {
                assertThat(segment.getLong(i)).isEqualTo(random.nextLong());
            }
        }

        // test unaligned offsets
        {
            final long seed = random.nextLong();

            random.setSeed(seed);
            for (int offset = 0; offset < PAGE_SIZE - 8; offset += random.nextInt(24) + 8) {
                long value = random.nextLong();
                segment.putLong(offset, value);
            }

            random.setSeed(seed);
            for (int offset = 0; offset < PAGE_SIZE - 8; offset += random.nextInt(24) + 8) {
                long shouldValue = random.nextLong();
                long isValue = segment.getLong(offset);
                assertThat(isValue).isEqualTo(shouldValue);
            }
        }
    }

    @Test
    void intAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putInt(-1, 0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putInt(PAGE_SIZE, 0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getInt(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getInt(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
                segment.putInt(i, random.nextInt());
            }

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 4; i += 4) {
                assertThat(segment.getInt(i)).isEqualTo(random.nextInt());
            }
        }
    }

    @Test
    void shortAccess() {
        // test exceptions
        {
            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putShort(-1, (short) 0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.putShort(PAGE_SIZE, (short) 0));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getShort(-1));

            assertThatExceptionOfType(IndexOutOfBoundsException.class)
                    .as("IndexOutOfBoundsException expected")
                    .isThrownBy(() -> segment.getShort(PAGE_SIZE));
        }

        // test expected correct behavior
        {
            long seed = random.nextLong();

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
                segment.putShort(i, (short) random.nextInt());
            }

            random.setSeed(seed);
            for (int i = 0; i <= PAGE_SIZE - 2; i += 2) {
                assertThat(segment.getShort(i)).isEqualTo((short) random.nextInt());
            }
        }
    }

    @Test
    void testByteBufferWrapping() {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(1024);

        ByteBuffer buf1 = seg.wrap(13, 47);
        assertThat(buf1.position()).isEqualTo(13);
        assertThat(buf1.limit()).isEqualTo(60);
        assertThat(buf1.remaining()).isEqualTo(47);

        ByteBuffer buf2 = seg.wrap(500, 267);
        assertThat(buf2.position()).isEqualTo(500);
        assertThat(buf2.limit()).isEqualTo(767);
        assertThat(buf2.remaining()).isEqualTo(267);

        ByteBuffer buf3 = seg.wrap(0, 1024);
        assertThat(buf3.position()).isZero();
        assertThat(buf3.limit()).isEqualTo(1024);
        assertThat(buf3.remaining()).isEqualTo(1024);
    }

    @Test
    void testGetOffHeapBuffer() {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(1024);
        assertThatExceptionOfType(IllegalStateException.class)
                .as("Memory segment does not represent off-heap buffer")
                .isThrownBy(seg::getOffHeapBuffer);
        seg.free();

        seg = MemorySegmentFactory.allocateUnpooledOffHeapMemory(1024);
        assertThat(seg.getOffHeapBuffer()).isNotNull();
        seg.free();

        seg = MemorySegmentFactory.allocateOffHeapUnsafeMemory(1024);
        assertThat(seg.getOffHeapBuffer()).isNotNull();
        seg.free();
    }
}
