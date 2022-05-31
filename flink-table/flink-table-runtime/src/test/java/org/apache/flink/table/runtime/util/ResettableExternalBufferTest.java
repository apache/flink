/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.memory.MemoryManager.DEFAULT_PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ResettableExternalBuffer}. */
public class ResettableExternalBufferTest {

    private static final int MEMORY_SIZE = 1024 * DEFAULT_PAGE_SIZE;

    private MemoryManager memManager;
    private IOManager ioManager;
    private Random random;
    private BinaryRowDataSerializer serializer;
    private BinaryRowDataSerializer multiColumnFixedLengthSerializer;
    private BinaryRowDataSerializer multiColumnVariableLengthSerializer;

    @Before
    public void before() {
        this.memManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
        this.ioManager = new IOManagerAsync();
        this.random = new Random();
        this.serializer = new BinaryRowDataSerializer(1);
        this.multiColumnFixedLengthSerializer = new BinaryRowDataSerializer(3);
        this.multiColumnVariableLengthSerializer = new BinaryRowDataSerializer(5);
    }

    private ResettableExternalBuffer newBuffer(long memorySize) {
        return newBuffer(memorySize, this.serializer, true);
    }

    private ResettableExternalBuffer newBuffer(
            long memorySize, BinaryRowDataSerializer serializer, boolean isRowAllInFixedPart) {
        return new ResettableExternalBuffer(
                ioManager,
                new LazyMemorySegmentPool(
                        this, memManager, (int) (memorySize / memManager.getPageSize())),
                serializer,
                isRowAllInFixedPart);
    }

    @Test
    public void testLess() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 100;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // repeat read
        assertBuffer(expected, buffer);
        buffer.newIterator();
        assertBuffer(expected, buffer);

        buffer.close();
    }

    @Test
    public void testSpill() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 5000; // 16 * 5000
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // repeat read
        assertBuffer(expected, buffer);
        buffer.newIterator();
        assertBuffer(expected, buffer);

        buffer.close();
    }

    @Test
    public void testBufferReset() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        // less
        insertMulti(buffer, 10);
        buffer.reset();
        assertThat(0).isEqualTo(buffer.size());

        // not spill
        List<Long> expected = insertMulti(buffer, 100);
        assertThat(100).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();

        // spill
        expected = insertMulti(buffer, 2500);
        assertThat(2500).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);

        buffer.close();
    }

    @Test
    public void testBufferResetWithSpill() throws Exception {
        int inMemoryThreshold = 20;
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        // spill
        List<Long> expected = insertMulti(buffer, 2500);
        assertThat(2500).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();

        // spill, but not read the values
        insertMulti(buffer, 2500);
        buffer.newIterator();
        assertThat(2500).isEqualTo(buffer.size());
        buffer.reset();

        // not spill
        expected = insertMulti(buffer, inMemoryThreshold / 2);
        assertBuffer(expected, buffer);
        buffer.reset();
        assertThat(0).isEqualTo(buffer.size());

        // less
        expected = insertMulti(buffer, 100);
        assertThat(100).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();

        buffer.close();
    }

    @Test
    public void testHugeRecord() throws Exception {
        try (ResettableExternalBuffer buffer =
                new ResettableExternalBuffer(
                        ioManager,
                        new LazyMemorySegmentPool(
                                this, memManager, 3 * DEFAULT_PAGE_SIZE / memManager.getPageSize()),
                        new BinaryRowDataSerializer(1),
                        false)) {
            assertThatThrownBy(
                            () -> {
                                writeHuge(buffer, 50000);
                                writeHuge(buffer, 10);
                            })
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    public void testRandomAccessLess() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 100;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            assertRandomAccess(expected, buffer, beginPos.get(i));
        }

        buffer.close();
    }

    @Test
    public void testRandomAccessSpill() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 5000;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            assertRandomAccess(expected, buffer, beginPos.get(i));
        }

        buffer.close();
    }

    @Test
    public void testBufferResetWithSpillAndRandomAccess() throws Exception {
        final int tries = 100;
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        // spill, random access and reset twice
        List<Long> expected;
        for (int i = 0; i < 2; i++) {
            expected = insertMulti(buffer, 2500);
            assertThat(2500).isEqualTo(buffer.size());
            for (int j = 0; j < tries; j++) {
                assertRandomAccess(expected, buffer);
            }
            buffer.reset();
        }

        // spill, but not read the values
        insertMulti(buffer, 2500);
        buffer.newIterator();
        assertThat(2500).isEqualTo(buffer.size());
        buffer.reset();

        // not spill
        expected = insertMulti(buffer, 10);
        for (int i = 0; i < tries; i++) {
            assertRandomAccess(expected, buffer);
        }
        buffer.reset();
        assertThat(0).isEqualTo(buffer.size());

        // less
        expected = insertMulti(buffer, 100);
        assertThat(100).isEqualTo(buffer.size());
        for (int i = 0; i < tries; i++) {
            assertRandomAccess(expected, buffer);
        }
        buffer.reset();

        buffer.close();
    }

    @Test
    public void testMultiColumnFixedLengthRandomAccessLess() throws Exception {
        testMultiColumnRandomAccessLess(
                multiColumnFixedLengthSerializer, FixedLengthRowData.class, true);
    }

    @Test
    public void testMultiColumnFixedLengthRandomAccessSpill() throws Exception {
        testMultiColumnRandomAccessSpill(
                multiColumnFixedLengthSerializer, FixedLengthRowData.class, true);
    }

    @Test
    public void testBufferResetWithSpillAndMultiColumnFixedLengthRandomAccess() throws Exception {
        testBufferResetWithSpillAndMultiColumnRandomAccess(
                multiColumnFixedLengthSerializer, FixedLengthRowData.class, true);
    }

    @Test
    public void testMultiColumnVariableLengthRandomAccessLess() throws Exception {
        testMultiColumnRandomAccessLess(
                multiColumnVariableLengthSerializer, VariableLengthRowData.class, false);
    }

    @Test
    public void testMultiColumnVariableLengthRandomAccessSpill() throws Exception {
        testMultiColumnRandomAccessSpill(
                multiColumnVariableLengthSerializer, VariableLengthRowData.class, false);
    }

    @Test
    public void testBufferResetWithSpillAndMultiColumnVariableLengthRandomAccess()
            throws Exception {
        testBufferResetWithSpillAndMultiColumnRandomAccess(
                multiColumnVariableLengthSerializer, VariableLengthRowData.class, false);
    }

    @Test
    public void testIteratorOnFixedLengthEmptyBuffer() throws Exception {
        testIteratorOnMultiColumnEmptyBuffer(multiColumnFixedLengthSerializer, true);
    }

    @Test
    public void testFixedLengthRandomAccessOutOfRange() throws Exception {
        testRandomAccessOutOfRange(
                multiColumnFixedLengthSerializer, FixedLengthRowData.class, true);
    }

    @Test
    public void testIteratorOnVariableLengthEmptyBuffer() throws Exception {
        testIteratorOnMultiColumnEmptyBuffer(multiColumnVariableLengthSerializer, false);
    }

    @Test
    public void testVariableLengthRandomAccessOutOfRange() throws Exception {
        testRandomAccessOutOfRange(
                multiColumnVariableLengthSerializer, VariableLengthRowData.class, false);
    }

    @Test
    public void testIteratorReset() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 100;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // reset and read
        ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator();
        assertBuffer(expected, iterator);
        iterator.reset();
        assertBuffer(expected, iterator);
        iterator.close();

        buffer.close();
    }

    @Test
    public void testIteratorResetWithSpill() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 5000; // 16 * 5000
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // reset and read
        ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator();
        assertBuffer(expected, iterator);
        iterator.reset();
        assertBuffer(expected, iterator);
        iterator.close();

        buffer.close();
    }

    @Test
    public void testIteratorResetWithRandomAccess() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 100;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            int begin = beginPos.get(i);
            ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator(begin);
            assertRandomAccess(expected, iterator, begin);
            iterator.reset();
            assertRandomAccess(expected, iterator, begin);
            iterator.close();
        }

        buffer.close();
    }

    @Test
    public void testIteratorResetWithRandomAccessSpill() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 5000;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            int begin = beginPos.get(i);
            ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator(begin);
            assertRandomAccess(expected, iterator, begin);
            iterator.reset();
            assertRandomAccess(expected, iterator, begin);
            iterator.close();
        }

        buffer.close();
    }

    @Test
    public void testMultipleIteratorsLess() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 100;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            int beginIdx = beginPos.get(i);
            ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator(beginIdx);
            assertRandomAccess(expected, iterator, beginIdx);
            if (i % 3 == 0) {
                iterator.close();
            }
        }

        buffer.close();
    }

    @Test
    public void testMultipleIteratorsSpill() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 5000;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            int beginIdx = beginPos.get(i);
            ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator(beginIdx);
            assertRandomAccess(expected, iterator, beginIdx);
            if (i % 3 == 0) {
                iterator.close();
            }
        }

        buffer.close();
    }

    @Test
    public void testMultipleIteratorsWithIteratorReset() throws Exception {
        ResettableExternalBuffer buffer = newBuffer(DEFAULT_PAGE_SIZE * 2);

        int number = 5000; // 16 * 5000
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // reset and read
        ResettableExternalBuffer.BufferIterator iterator1 = buffer.newIterator();
        assertBuffer(expected, iterator1);
        iterator1.reset();
        assertBuffer(expected, iterator1);

        ResettableExternalBuffer.BufferIterator iterator2 = buffer.newIterator();
        assertBuffer(expected, iterator2);
        iterator2.reset();
        assertBuffer(expected, iterator2);

        iterator1.reset();
        assertBuffer(expected, iterator1);
        iterator2.reset();
        assertBuffer(expected, iterator2);

        iterator1.close();

        iterator2.reset();
        assertBuffer(expected, iterator2);
        iterator2.close();

        buffer.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateIteratorFixedLengthLess() throws Exception {
        testUpdateIteratorLess(multiColumnFixedLengthSerializer, FixedLengthRowData.class, true);
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateIteratorFixedLengthSpill() throws Exception {
        testUpdateIteratorSpill(multiColumnFixedLengthSerializer, FixedLengthRowData.class, true);
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateIteratorVariableLengthLess() throws Exception {
        testUpdateIteratorLess(
                multiColumnVariableLengthSerializer, VariableLengthRowData.class, false);
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateIteratorVariableLengthSpill() throws Exception {
        testUpdateIteratorSpill(
                multiColumnVariableLengthSerializer, VariableLengthRowData.class, false);
    }

    private <T extends RowData> void testMultiColumnRandomAccessLess(
            BinaryRowDataSerializer serializer, Class<T> clazz, boolean isRowAllInFixedPart)
            throws Exception {
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        int number = 30;
        List<RowData> expected = insertMultiColumn(buffer, number, clazz);
        assertThat(number).isEqualTo(buffer.size());
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            assertMultiColumnRandomAccess(expected, buffer, beginPos.get(i));
        }

        buffer.close();
    }

    private <T extends RowData> void testMultiColumnRandomAccessSpill(
            BinaryRowDataSerializer serializer, Class<T> clazz, boolean isRowAllInFixedPart)
            throws Exception {
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        int number = 4000;
        List<RowData> expected = insertMultiColumn(buffer, number, clazz);
        assertThat(number).isEqualTo(buffer.size());
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // repeat random access
        List<Integer> beginPos = new ArrayList<>();
        for (int i = 0; i < buffer.size(); i++) {
            beginPos.add(i);
        }
        Collections.shuffle(beginPos);
        for (int i = 0; i < buffer.size(); i++) {
            assertMultiColumnRandomAccess(expected, buffer, beginPos.get(i));
        }

        buffer.close();
    }

    private <T extends RowData> void testBufferResetWithSpillAndMultiColumnRandomAccess(
            BinaryRowDataSerializer serializer, Class<T> clazz, boolean isRowAllInFixedPart)
            throws Exception {
        final int tries = 100;
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        // spill, random access and reset twice
        List<RowData> expected;
        for (int i = 0; i < 2; i++) {
            expected = insertMultiColumn(buffer, 1500, clazz);
            assertThat(1500).isEqualTo(buffer.size());
            for (int j = 0; j < tries; j++) {
                assertMultiColumnRandomAccess(expected, buffer);
            }
            buffer.reset();
        }

        // spill, but not read the values
        insertMultiColumn(buffer, 1500, clazz);
        buffer.newIterator();
        assertThat(1500).isEqualTo(buffer.size());
        buffer.reset();

        // not spill
        expected = insertMultiColumn(buffer, 10, clazz);
        for (int i = 0; i < tries; i++) {
            assertMultiColumnRandomAccess(expected, buffer);
        }
        buffer.reset();
        assertThat(0).isEqualTo(buffer.size());

        // less
        expected = insertMultiColumn(buffer, 30, clazz);
        assertThat(30).isEqualTo(buffer.size());
        for (int i = 0; i < tries; i++) {
            assertMultiColumnRandomAccess(expected, buffer);
        }
        buffer.reset();

        buffer.close();
    }

    private void testIteratorOnMultiColumnEmptyBuffer(
            BinaryRowDataSerializer serializer, boolean isRowAllInFixedPart) {
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        ResettableExternalBuffer.BufferIterator iterator;
        buffer.complete();
        iterator = buffer.newIterator(0);
        assertThat(iterator.advanceNext()).isFalse();
        iterator = buffer.newIterator(random.nextInt(Integer.MAX_VALUE));
        assertThat(iterator.advanceNext()).isFalse();

        buffer.close();
    }

    private <T extends RowData> void testRandomAccessOutOfRange(
            BinaryRowDataSerializer serializer, Class<T> clazz, boolean isRowAllInFixedPart)
            throws Exception {
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        int number = 100;
        List<RowData> expected = insertMultiColumn(buffer, number, clazz);
        assertThat(number).isEqualTo(buffer.size());
        assertMultiColumnRandomAccess(expected, buffer, 0);

        ResettableExternalBuffer.BufferIterator iterator;
        iterator = buffer.newIterator(number);
        assertThat(iterator.advanceNext()).isFalse();
        iterator = buffer.newIterator(number + random.nextInt(Integer.MAX_VALUE));
        assertThat(iterator.advanceNext()).isFalse();
        iterator = buffer.newIterator(random.nextInt(number));
        assertThat(iterator.advanceNext()).isTrue();

        buffer.close();
    }

    private <T extends RowData> void testUpdateIteratorLess(
            BinaryRowDataSerializer serializer, Class<T> clazz, boolean isRowAllInFixedPart)
            throws Exception {
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        int number = 20;
        int iters = 3;

        List<RowData> expected = new ArrayList<>();
        List<ResettableExternalBuffer.BufferIterator> iterators = new ArrayList<>();

        for (int i = 0; i < iters; i++) {
            iterators.add(buffer.newIterator());
        }

        for (int i = 0; i < number; i++) {
            RowData data = clazz.newInstance();
            data.insertIntoBuffer(buffer);
            expected.add(data);

            for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
                assertThat(iterator.advanceNext()).isTrue();
                BinaryRowData row = iterator.getRow();
                data.checkSame(row);

                assertThat(iterator.advanceNext()).isFalse();
            }
        }

        for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
            iterator.reset();
        }

        for (int i = 0; i < number; i++) {
            for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
                assertThat(iterator.advanceNext()).isTrue();
                BinaryRowData row = iterator.getRow();
                expected.get(i).checkSame(row);
            }
        }

        for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
            iterator.close();
        }

        assertMultiColumnRandomAccess(expected, buffer);

        buffer.close();
    }

    private <T extends RowData> void testUpdateIteratorSpill(
            BinaryRowDataSerializer serializer, Class<T> clazz, boolean isRowAllInFixedPart)
            throws Exception {
        ResettableExternalBuffer buffer =
                newBuffer(DEFAULT_PAGE_SIZE * 2, serializer, isRowAllInFixedPart);

        int number = 100;
        int step = 20;
        int iters = 3;

        List<RowData> expected = new ArrayList<>();
        List<RowData> smallExpected = new ArrayList<>();
        List<ResettableExternalBuffer.BufferIterator> iterators = new ArrayList<>();

        for (int i = 0; i < iters; i++) {
            iterators.add(buffer.newIterator());
        }

        for (int i = 0; i < number; i++) {
            smallExpected.clear();
            for (int j = 0; j < step; j++) {
                RowData data = clazz.newInstance();
                data.insertIntoBuffer(buffer);
                expected.add(data);
                smallExpected.add(data);
            }

            for (int j = 0; j < step; j++) {
                for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
                    assertThat(iterator.advanceNext()).isTrue();
                    BinaryRowData row = iterator.getRow();
                    smallExpected.get(j).checkSame(row);
                }
            }
            for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
                assertThat(iterator.advanceNext()).isFalse();
            }
        }

        for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
            iterator.reset();
        }

        for (int i = 0; i < number * step; i++) {
            for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
                assertThat(iterator.advanceNext()).isTrue();
                BinaryRowData row = iterator.getRow();
                expected.get(i).checkSame(row);
            }
        }

        for (ResettableExternalBuffer.BufferIterator iterator : iterators) {
            iterator.close();
        }

        assertMultiColumnRandomAccess(expected, buffer);

        buffer.close();
    }

    private void writeHuge(ResettableExternalBuffer buffer, int size) throws IOException {
        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeString(0, StringData.fromString(RandomStringUtils.random(size)));
        writer.complete();
        buffer.add(row);
    }

    private void assertBuffer(List<Long> expected, ResettableExternalBuffer buffer) {
        ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator();
        assertBuffer(expected, iterator);
        iterator.close();
    }

    private void assertBuffer(
            List<Long> expected, ResettableExternalBuffer.BufferIterator iterator) {
        List<Long> values = new ArrayList<>();
        while (iterator.advanceNext()) {
            values.add(iterator.getRow().getLong(0));
        }
        assertThat(values).isEqualTo(expected);
    }

    private List<Long> insertMulti(ResettableExternalBuffer buffer, int cnt) throws IOException {
        ArrayList<Long> expected = new ArrayList<>(cnt);
        insertMulti(buffer, cnt, expected);
        buffer.complete();
        return expected;
    }

    private void insertMulti(ResettableExternalBuffer buffer, int cnt, List<Long> expected)
            throws IOException {
        for (int i = 0; i < cnt; i++) {
            expected.add(randomInsert(buffer));
        }
    }

    private long randomInsert(ResettableExternalBuffer buffer) throws IOException {
        long l = random.nextLong();
        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeLong(0, l);
        writer.complete();
        buffer.add(row);
        return l;
    }

    private void assertRandomAccess(List<Long> expected, ResettableExternalBuffer buffer) {
        int begin = random.nextInt(buffer.size());
        assertRandomAccess(expected, buffer, begin);
    }

    private void assertRandomAccess(
            List<Long> expected, ResettableExternalBuffer buffer, int begin) {
        ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator(begin);
        assertRandomAccess(expected, iterator, begin);
        iterator.close();
    }

    private void assertRandomAccess(
            List<Long> expected, ResettableExternalBuffer.BufferIterator iterator, int begin) {
        List<Long> values = new ArrayList<>();
        while (iterator.advanceNext()) {
            values.add(iterator.getRow().getLong(0));
        }
        assertThat(values).isEqualTo(expected.subList(begin, expected.size()));
    }

    private <T extends RowData> List<RowData> insertMultiColumn(
            ResettableExternalBuffer buffer, int cnt, Class<T> clazz)
            throws IOException, IllegalAccessException, InstantiationException {
        ArrayList<RowData> expected = new ArrayList<>(cnt);
        insertMultiColumn(buffer, cnt, expected, clazz);
        buffer.complete();
        return expected;
    }

    private <T extends RowData> void insertMultiColumn(
            ResettableExternalBuffer buffer, int cnt, List<RowData> expected, Class<T> clazz)
            throws IOException, IllegalAccessException, InstantiationException {
        for (int i = 0; i < cnt; i++) {
            RowData data = clazz.newInstance();
            data.insertIntoBuffer(buffer);
            expected.add(data);
        }
        buffer.complete();
    }

    private void assertMultiColumnRandomAccess(
            List<RowData> expected, ResettableExternalBuffer buffer) {
        int begin = random.nextInt(buffer.size());
        assertMultiColumnRandomAccess(expected, buffer, begin);
    }

    private void assertMultiColumnRandomAccess(
            List<RowData> expected, ResettableExternalBuffer buffer, int begin) {
        ResettableExternalBuffer.BufferIterator iterator = buffer.newIterator(begin);
        for (int i = begin; i < buffer.size(); i++) {
            assertThat(iterator.advanceNext()).isTrue();
            expected.get(i).checkSame(iterator.getRow());
        }
    }

    private interface RowData {
        void insertIntoBuffer(ResettableExternalBuffer buffer) throws IOException;

        void checkSame(BinaryRowData row);
    }

    private static class FixedLengthRowData implements RowData {
        private final boolean col0;
        private final long col1;
        private final int col2;

        FixedLengthRowData() {
            Random random = new Random();
            col0 = random.nextBoolean();
            col1 = random.nextLong();
            col2 = random.nextInt();
        }

        @Override
        public void insertIntoBuffer(ResettableExternalBuffer buffer) throws IOException {
            BinaryRowData row = new BinaryRowData(3);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.reset();
            writer.writeBoolean(0, col0);
            writer.writeLong(1, col1);
            writer.writeInt(2, col2);
            writer.complete();
            buffer.add(row);
        }

        @Override
        public void checkSame(BinaryRowData row) {
            assertThat(row.getBoolean(0)).isEqualTo(col0);
            assertThat(row.getLong(1)).isEqualTo(col1);
            assertThat(row.getInt(2)).isEqualTo(col2);
        }
    }

    private static class VariableLengthRowData implements RowData {
        private final boolean col0;
        private final long col1;
        private final StringData col2;
        private final int col3;
        private final StringData col4;

        public VariableLengthRowData() {
            Random random = new Random();
            col0 = random.nextBoolean();
            col1 = random.nextLong();
            col2 = StringData.fromString(RandomStringUtils.random(random.nextInt(50) + 1));
            col3 = random.nextInt();
            col4 = StringData.fromString(RandomStringUtils.random(random.nextInt(50) + 1));
        }

        @Override
        public void insertIntoBuffer(ResettableExternalBuffer buffer) throws IOException {
            BinaryRowData row = new BinaryRowData(5);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.reset();
            writer.writeBoolean(0, col0);
            writer.writeLong(1, col1);
            writer.writeString(2, col2);
            writer.writeInt(3, col3);
            writer.writeString(4, col4);
            writer.complete();
            buffer.add(row);
        }

        @Override
        public void checkSame(BinaryRowData row) {
            assertThat(row.getBoolean(0)).isEqualTo(col0);
            assertThat(row.getLong(1)).isEqualTo(col1);
            assertThat(row.getString(2)).isEqualTo(col2);
            assertThat(row.getInt(3)).isEqualTo(col3);
            assertThat(row.getString(4)).isEqualTo(col4);
        }
    }
}
