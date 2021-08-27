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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LargeRecordHandlerTest {

    @Test
    public void testEmptyRecordHandler() {
        final int PAGE_SIZE = 4 * 1024;
        final int NUM_PAGES = 50;

        try (final IOManager ioMan = new IOManagerAsync()) {
            final MemoryManager memMan =
                    MemoryManagerBuilder.newBuilder()
                            .setMemorySize(NUM_PAGES * PAGE_SIZE)
                            .setPageSize(PAGE_SIZE)
                            .build();
            final AbstractInvokable owner = new DummyInvokable();
            final List<MemorySegment> memory = memMan.allocatePages(owner, NUM_PAGES);

            final TupleTypeInfo<Tuple2<Long, String>> typeInfo =
                    (TupleTypeInfo<Tuple2<Long, String>>)
                            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {});

            final TypeSerializer<Tuple2<Long, String>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple2<Long, String>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            LargeRecordHandler<Tuple2<Long, String>> handler =
                    new LargeRecordHandler<Tuple2<Long, String>>(
                            serializer,
                            comparator,
                            ioMan,
                            memMan,
                            memory,
                            owner,
                            128,
                            owner.getExecutionConfig());

            assertFalse(handler.hasData());

            handler.close();

            assertFalse(handler.hasData());

            handler.close();

            try {
                handler.addRecord(new Tuple2<Long, String>(92L, "peter pepper"));
                fail("should throw an exception");
            } catch (IllegalStateException e) {
                // expected
            }

            assertTrue(memMan.verifyEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRecordHandlerSingleKey() {
        final int PAGE_SIZE = 4 * 1024;
        final int NUM_PAGES = 24;
        final int NUM_RECORDS = 25000;

        try (final IOManager ioMan = new IOManagerAsync()) {
            final MemoryManager memMan =
                    MemoryManagerBuilder.newBuilder()
                            .setMemorySize(NUM_PAGES * PAGE_SIZE)
                            .setPageSize(PAGE_SIZE)
                            .build();
            final AbstractInvokable owner = new DummyInvokable();

            final List<MemorySegment> initialMemory = memMan.allocatePages(owner, 6);
            final List<MemorySegment> sortMemory = memMan.allocatePages(owner, NUM_PAGES - 6);

            final TupleTypeInfo<Tuple2<Long, String>> typeInfo =
                    (TupleTypeInfo<Tuple2<Long, String>>)
                            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {});

            final TypeSerializer<Tuple2<Long, String>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple2<Long, String>> comparator =
                    typeInfo.createComparator(
                            new int[] {0}, new boolean[] {true}, 0, new ExecutionConfig());

            LargeRecordHandler<Tuple2<Long, String>> handler =
                    new LargeRecordHandler<Tuple2<Long, String>>(
                            serializer,
                            comparator,
                            ioMan,
                            memMan,
                            initialMemory,
                            owner,
                            128,
                            owner.getExecutionConfig());

            assertFalse(handler.hasData());

            // add the test data
            Random rnd = new Random();

            for (int i = 0; i < NUM_RECORDS; i++) {
                long val = rnd.nextLong();
                handler.addRecord(new Tuple2<Long, String>(val, String.valueOf(val)));
                assertTrue(handler.hasData());
            }

            MutableObjectIterator<Tuple2<Long, String>> sorted =
                    handler.finishWriteAndSortKeys(sortMemory);

            try {
                handler.addRecord(new Tuple2<Long, String>(92L, "peter pepper"));
                fail("should throw an exception");
            } catch (IllegalStateException e) {
                // expected
            }

            Tuple2<Long, String> previous = null;
            Tuple2<Long, String> next;

            while ((next = sorted.next(null)) != null) {
                // key and value must be equal
                assertTrue(next.f0.equals(Long.parseLong(next.f1)));

                // order must be correct
                if (previous != null) {
                    assertTrue(previous.f0 <= next.f0);
                }
                previous = next;
            }

            handler.close();

            assertFalse(handler.hasData());

            handler.close();

            try {
                handler.addRecord(new Tuple2<Long, String>(92L, "peter pepper"));
                fail("should throw an exception");
            } catch (IllegalStateException e) {
                // expected
            }

            assertTrue(memMan.verifyEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRecordHandlerCompositeKey() {
        final int PAGE_SIZE = 4 * 1024;
        final int NUM_PAGES = 24;
        final int NUM_RECORDS = 25000;

        try (final IOManager ioMan = new IOManagerAsync()) {
            final MemoryManager memMan =
                    MemoryManagerBuilder.newBuilder()
                            .setMemorySize(NUM_PAGES * PAGE_SIZE)
                            .setPageSize(PAGE_SIZE)
                            .build();
            final AbstractInvokable owner = new DummyInvokable();

            final List<MemorySegment> initialMemory = memMan.allocatePages(owner, 6);
            final List<MemorySegment> sortMemory = memMan.allocatePages(owner, NUM_PAGES - 6);

            final TupleTypeInfo<Tuple3<Long, String, Byte>> typeInfo =
                    (TupleTypeInfo<Tuple3<Long, String, Byte>>)
                            TypeInformation.of(new TypeHint<Tuple3<Long, String, Byte>>() {});

            final TypeSerializer<Tuple3<Long, String, Byte>> serializer =
                    typeInfo.createSerializer(new ExecutionConfig());
            final TypeComparator<Tuple3<Long, String, Byte>> comparator =
                    typeInfo.createComparator(
                            new int[] {2, 0}, new boolean[] {true, true}, 0, new ExecutionConfig());

            LargeRecordHandler<Tuple3<Long, String, Byte>> handler =
                    new LargeRecordHandler<Tuple3<Long, String, Byte>>(
                            serializer,
                            comparator,
                            ioMan,
                            memMan,
                            initialMemory,
                            owner,
                            128,
                            owner.getExecutionConfig());

            assertFalse(handler.hasData());

            // add the test data
            Random rnd = new Random();

            for (int i = 0; i < NUM_RECORDS; i++) {
                long val = rnd.nextLong();
                handler.addRecord(
                        new Tuple3<Long, String, Byte>(val, String.valueOf(val), (byte) val));
                assertTrue(handler.hasData());
            }

            MutableObjectIterator<Tuple3<Long, String, Byte>> sorted =
                    handler.finishWriteAndSortKeys(sortMemory);

            try {
                handler.addRecord(new Tuple3<Long, String, Byte>(92L, "peter pepper", (byte) 1));
                fail("should throw an exception");
            } catch (IllegalStateException e) {
                // expected
            }

            Tuple3<Long, String, Byte> previous = null;
            Tuple3<Long, String, Byte> next;

            while ((next = sorted.next(null)) != null) {
                // key and value must be equal
                assertTrue(next.f0.equals(Long.parseLong(next.f1)));
                assertTrue(next.f0.byteValue() == next.f2);

                // order must be correct
                if (previous != null) {
                    assertTrue(previous.f2 <= next.f2);
                    assertTrue(
                            previous.f2.byteValue() != next.f2.byteValue()
                                    || previous.f0 <= next.f0);
                }
                previous = next;
            }

            handler.close();

            assertFalse(handler.hasData());

            handler.close();

            try {
                handler.addRecord(new Tuple3<Long, String, Byte>(92L, "peter pepper", (byte) 1));
                fail("should throw an exception");
            } catch (IllegalStateException e) {
                // expected
            }

            assertTrue(memMan.verifyEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
