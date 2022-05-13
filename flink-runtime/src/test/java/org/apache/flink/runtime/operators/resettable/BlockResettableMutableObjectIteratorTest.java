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

package org.apache.flink.runtime.operators.resettable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.MutableObjectIteratorWrapper;
import org.apache.flink.runtime.testutils.recordutils.RecordSerializer;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BlockResettableMutableObjectIteratorTest {

    private static final int MEMORY_CAPACITY = 3 * 128 * 1024;

    private static final int NUM_VALUES = 20000;

    private final TypeSerializer<Record> serializer = RecordSerializer.get();

    private final AbstractInvokable memOwner = new DummyInvokable();

    private MemoryManager memman;

    private MutableObjectIterator<Record> reader;

    private List<Record> objects;

    @Before
    public void startup() {
        // set up IO and memory manager
        this.memman = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_CAPACITY).build();

        // create test objects
        this.objects = new ArrayList<Record>(20000);
        for (int i = 0; i < NUM_VALUES; ++i) {
            this.objects.add(new Record(new IntValue(i)));
        }

        // create the reader
        this.reader = new MutableObjectIteratorWrapper(this.objects.iterator());
    }

    @After
    public void shutdown() {
        this.objects = null;

        // check that the memory manager got all segments back
        if (!this.memman.verifyEmpty()) {
            Assert.fail(
                    "A memory leak has occurred: Not all memory was properly returned to the memory manager.");
        }

        this.memman.shutdown();
        this.memman = null;
    }

    @Test
    public void testSerialBlockResettableIterator() throws Exception {
        try {
            // create the resettable Iterator
            final BlockResettableMutableObjectIterator<Record> iterator =
                    new BlockResettableMutableObjectIterator<Record>(
                            this.memman, this.reader, this.serializer, 1, memOwner);
            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int lower = 0;
            int upper = 0;
            do {
                lower = upper;
                upper = lower;
                // find the upper bound
                Record target = new Record();
                while ((target = iterator.next(target)) != null) {
                    int val = target.getField(0, IntValue.class).getValue();
                    Assert.assertEquals(upper++, val);
                }
                // now reset the buffer a few times
                for (int i = 0; i < 5; ++i) {
                    iterator.reset();
                    target = new Record();
                    int count = 0;
                    while ((target = iterator.next(target)) != null) {
                        int val = target.getField(0, IntValue.class).getValue();
                        Assert.assertEquals(lower + (count++), val);
                    }
                    Assert.assertEquals(upper - lower, count);
                }
            } while (iterator.nextBlock());
            Assert.assertEquals(NUM_VALUES, upper);
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            Assert.fail("Test encountered an exception: " + ex.getMessage());
        }
    }

    @Test
    public void testDoubleBufferedBlockResettableIterator() throws Exception {
        try {
            // create the resettable Iterator
            final BlockResettableMutableObjectIterator<Record> iterator =
                    new BlockResettableMutableObjectIterator<Record>(
                            this.memman, this.reader, this.serializer, 2, memOwner);
            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int lower = 0;
            int upper = 0;
            do {
                lower = upper;
                upper = lower;

                Record target = new Record();
                // find the upper bound
                while ((target = iterator.next(target)) != null) {
                    int val = target.getField(0, IntValue.class).getValue();
                    Assert.assertEquals(upper++, val);
                }
                // now reset the buffer a few times
                for (int i = 0; i < 5; ++i) {
                    iterator.reset();
                    target = new Record();
                    int count = 0;
                    while ((target = iterator.next(target)) != null) {
                        int val = target.getField(0, IntValue.class).getValue();
                        Assert.assertEquals(lower + (count++), val);
                    }
                    Assert.assertEquals(upper - lower, count);
                }
            } while (iterator.nextBlock());
            Assert.assertEquals(NUM_VALUES, upper);
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            Assert.fail("Test encountered an exception: " + ex.getMessage());
        }
    }

    @Test
    public void testTwelveFoldBufferedBlockResettableIterator() throws Exception {
        try {
            // create the resettable Iterator
            final BlockResettableMutableObjectIterator<Record> iterator =
                    new BlockResettableMutableObjectIterator<Record>(
                            this.memman, this.reader, this.serializer, 12, memOwner);
            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int lower = 0;
            int upper = 0;
            do {
                lower = upper;
                upper = lower;

                Record target = new Record();
                // find the upper bound
                while ((target = iterator.next(target)) != null) {
                    int val = target.getField(0, IntValue.class).getValue();
                    Assert.assertEquals(upper++, val);
                }
                // now reset the buffer a few times
                for (int i = 0; i < 5; ++i) {
                    iterator.reset();
                    target = new Record();
                    int count = 0;
                    while ((target = iterator.next(target)) != null) {
                        int val = target.getField(0, IntValue.class).getValue();
                        Assert.assertEquals(lower + (count++), val);
                    }
                    Assert.assertEquals(upper - lower, count);
                }
            } while (iterator.nextBlock());
            Assert.assertEquals(NUM_VALUES, upper);
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            Assert.fail("Test encountered an exception: " + ex.getMessage());
        }
    }
}
