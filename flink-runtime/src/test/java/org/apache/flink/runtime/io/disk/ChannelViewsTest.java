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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** */
class ChannelViewsTest {
    private static final long SEED = 649180756312423613L;

    private static final int KEY_MAX = Integer.MAX_VALUE;

    private static final int VALUE_SHORT_LENGTH = 114;

    private static final int VALUE_LONG_LENGTH = 112 * 1024;

    private static final int NUM_PAIRS_SHORT = 1000000;

    private static final int NUM_PAIRS_LONG = 3000;

    private static final int MEMORY_SIZE = 1024 * 1024;

    private static final int MEMORY_PAGE_SIZE = 64 * 1024;

    private static final int NUM_MEMORY_SEGMENTS = 3;

    private final AbstractInvokable parentTask = new DummyInvokable();

    private IOManager ioManager;

    private MemoryManager memoryManager;

    // --------------------------------------------------------------------------------------------

    @BeforeEach
    void beforeTest() {
        this.memoryManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(MEMORY_SIZE)
                        .setPageSize(MEMORY_PAGE_SIZE)
                        .build();
        this.ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        this.ioManager.close();

        if (memoryManager != null) {
            assertThat(this.memoryManager.verifyEmpty())
                    .withFailMessage(
                            "Memory leak: not all segments have been returned to the memory manager.")
                    .isTrue();
            this.memoryManager.shutdown();
            this.memoryManager = null;
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testWriteReadSmallRecords() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final FileIOChannel.ID channel = this.ioManager.createChannel();
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        List<MemorySegment> memory =
                this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channel);
        final ChannelWriterOutputView outView =
                new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);
        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }
        this.memoryManager.release(outView.close());

        // create the reader input view
        memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channel);
        final ChannelReaderInputView inView =
                new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
        generator.reset();

        // read and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);
            assertReadRecordMatchRegenerated(readRec, rec);
        }

        this.memoryManager.release(inView.close());
        reader.deleteChannel();
    }

    @Test
    void testWriteAndReadLongRecords() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_LONG_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final FileIOChannel.ID channel = this.ioManager.createChannel();
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        List<MemorySegment> memory =
                this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channel);
        final ChannelWriterOutputView outView =
                new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_LONG; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }
        this.memoryManager.release(outView.close());

        // create the reader input view
        memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channel);
        final ChannelReaderInputView inView =
                new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
        generator.reset();

        // read and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_LONG; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);
            assertReadRecordMatchRegenerated(readRec, rec);
        }

        this.memoryManager.release(inView.close());
        reader.deleteChannel();
    }

    @Test
    void testReadTooMany() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final FileIOChannel.ID channel = this.ioManager.createChannel();
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        List<MemorySegment> memory =
                this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channel);
        final ChannelWriterOutputView outView =
                new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }
        this.memoryManager.release(outView.close());

        // create the reader input view
        memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channel);
        final ChannelReaderInputView inView =
                new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
        generator.reset();

        // read and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);
            assertReadRecordMatchRegenerated(readRec, rec);
        }

        generator.next(rec);
        assertThatThrownBy(() -> serializer.deserialize(readRec, inView))
                .withFailMessage("Expected an EOFException which did not occur.")
                .isInstanceOf(EOFException.class);

        this.memoryManager.release(inView.close());
        reader.deleteChannel();
    }

    @Test
    void testReadWithoutKnownBlockCount() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final FileIOChannel.ID channel = this.ioManager.createChannel();
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        List<MemorySegment> memory =
                this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channel);
        final ChannelWriterOutputView outView =
                new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }
        this.memoryManager.release(outView.close());

        // create the reader input view
        memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channel);
        final ChannelReaderInputView inView = new ChannelReaderInputView(reader, memory, true);
        generator.reset();

        // read and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);
            assertReadRecordMatchRegenerated(readRec, rec);
        }

        this.memoryManager.release(inView.close());
        reader.deleteChannel();
    }

    @Test
    void testWriteReadOneBufferOnly() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final FileIOChannel.ID channel = this.ioManager.createChannel();
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        List<MemorySegment> memory = this.memoryManager.allocatePages(this.parentTask, 1);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channel);
        final ChannelWriterOutputView outView =
                new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }
        this.memoryManager.release(outView.close());

        // create the reader input view
        memory = this.memoryManager.allocatePages(this.parentTask, 1);
        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channel);
        final ChannelReaderInputView inView =
                new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
        generator.reset();

        // read and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);
            assertReadRecordMatchRegenerated(readRec, rec);
        }

        this.memoryManager.release(inView.close());
        reader.deleteChannel();
    }

    @Test
    void testWriteReadNotAll() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_SHORT_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final FileIOChannel.ID channel = this.ioManager.createChannel();
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        List<MemorySegment> memory =
                this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channel);
        final ChannelWriterOutputView outView =
                new ChannelWriterOutputView(writer, memory, MEMORY_PAGE_SIZE);

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }
        this.memoryManager.release(outView.close());

        // create the reader input view
        memory = this.memoryManager.allocatePages(this.parentTask, NUM_MEMORY_SEGMENTS);
        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channel);
        final ChannelReaderInputView inView =
                new ChannelReaderInputView(reader, memory, outView.getBlockCount(), true);
        generator.reset();

        // read and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_SHORT / 2; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);
            assertReadRecordMatchRegenerated(readRec, rec);
        }

        this.memoryManager.release(inView.close());
        reader.deleteChannel();
    }

    private static void assertReadRecordMatchRegenerated(
            Tuple2<Integer, String> readRec, Tuple2<Integer, String> rec) {
        int k1 = rec.f0;
        String v1 = rec.f1;

        int k2 = readRec.f0;
        String v2 = readRec.f1;

        assertThat(k2)
                .withFailMessage("The re-generated and the read record do not match.")
                .isEqualTo(k1);
        assertThat(v2)
                .withFailMessage("The re-generated and the read record do not match.")
                .isEqualTo(v1);
    }
}
