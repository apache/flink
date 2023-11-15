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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
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
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpillingBufferTest {

    private static final long SEED = 649180756312423613L;

    private static final int KEY_MAX = Integer.MAX_VALUE;

    private static final int VALUE_LENGTH = 114;

    private static final int NUM_PAIRS_INMEM = 6000;

    private static final int NUM_PAIRS_EXTERNAL = 30000;

    private static final int MEMORY_SIZE = 1024 * 1024;

    private static final int NUM_MEMORY_SEGMENTS = 23;

    private final AbstractInvokable parentTask = new DummyInvokable();

    private IOManager ioManager;

    private MemoryManager memoryManager;

    // --------------------------------------------------------------------------------------------

    @BeforeEach
    void beforeTest() {
        memoryManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
        ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        ioManager.close();

        if (memoryManager != null) {
            assertThat(memoryManager.verifyEmpty())
                    .withFailMessage(
                            "Memory leak: not all segments have been returned to the memory manager.")
                    .isTrue();
            memoryManager.shutdown();
            memoryManager = null;
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testWriteReadInMemory() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
        this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
        final SpillingBuffer outView =
                new SpillingBuffer(
                        this.ioManager,
                        new ListMemorySegmentSource(memory),
                        this.memoryManager.getPageSize());

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }

        // create the reader input view
        DataInputView inView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        // re-notifyNonEmpty the data
        inView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        this.memoryManager.release(outView.close());
        this.memoryManager.release(memory);
    }

    @Test
    void testWriteReadTooMuchInMemory() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
        this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
        final SpillingBuffer outView =
                new SpillingBuffer(
                        this.ioManager,
                        new ListMemorySegmentSource(memory),
                        this.memoryManager.getPageSize());

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }

        // create the reader input view
        DataInputView inView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        generator.next(rec);
        assertThatThrownBy(() -> serializer.deserialize(readRec, inView))
                .withFailMessage("Read too much, expected EOFException.")
                .isInstanceOf(EOFException.class);

        // re-notifyNonEmpty the data
        DataInputView nextInView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        for (int i = 0; i < NUM_PAIRS_INMEM; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, nextInView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        this.memoryManager.release(outView.close());
        this.memoryManager.release(memory);
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testWriteReadExternal() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
        this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
        final SpillingBuffer outView =
                new SpillingBuffer(
                        this.ioManager,
                        new ListMemorySegmentSource(memory),
                        this.memoryManager.getPageSize());

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }

        // create the reader input view
        DataInputView inView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        // re-notifyNonEmpty the data
        inView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        this.memoryManager.release(outView.close());
        this.memoryManager.release(memory);
    }

    @Test
    void testWriteReadTooMuchExternal() throws Exception {
        final TestData.TupleGenerator generator =
                new TestData.TupleGenerator(
                        SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.RANDOM_LENGTH);
        final TypeSerializer<Tuple2<Integer, String>> serializer =
                TestData.getIntStringTupleSerializer();

        // create the writer output view
        final ArrayList<MemorySegment> memory = new ArrayList<MemorySegment>(NUM_MEMORY_SEGMENTS);
        this.memoryManager.allocatePages(this.parentTask, memory, NUM_MEMORY_SEGMENTS);
        final SpillingBuffer outView =
                new SpillingBuffer(
                        this.ioManager,
                        new ListMemorySegmentSource(memory),
                        this.memoryManager.getPageSize());

        // write a number of pairs
        final Tuple2<Integer, String> rec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
            generator.next(rec);
            serializer.serialize(rec, outView);
        }

        // create the reader input view
        DataInputView inView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        final Tuple2<Integer, String> readRec = new Tuple2<>();
        for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, inView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        generator.next(rec);
        assertThatThrownBy(() -> serializer.deserialize(readRec, inView))
                .withFailMessage("Read too much, expected EOFException.")
                .isInstanceOf(EOFException.class);

        // re-notifyNonEmpty the data
        DataInputView nextInView = outView.flip();
        generator.reset();

        // notifyNonEmpty and re-generate all records and compare them
        for (int i = 0; i < NUM_PAIRS_EXTERNAL; i++) {
            generator.next(rec);
            serializer.deserialize(readRec, nextInView);

            int k1 = rec.f0;
            String v1 = rec.f1;

            int k2 = readRec.f0;
            String v2 = readRec.f1;

            assertThat(k1 == k2 && v1.equals(v2))
                    .withFailMessage("The re-generated and the notifyNonEmpty record do not match.")
                    .isTrue();
        }

        this.memoryManager.release(outView.close());
        this.memoryManager.release(memory);
    }
}
