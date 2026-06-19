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
import org.apache.flink.api.common.typeutils.base.IntValueSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.IntValue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class SpillingResettableIteratorTest {

    private static final int NUM_TESTRECORDS = 50000;

    private static final int MEMORY_CAPACITY = 10 * 1024 * 1024;

    private final AbstractInvokable memOwner = new DummyInvokable();

    private IOManager ioman;

    private MemoryManager memman;

    private Iterator<IntValue> reader;

    private final TypeSerializer<IntValue> serializer = new IntValueSerializer();

    @BeforeEach
    void startup() {
        // set up IO and memory manager
        this.memman = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_CAPACITY).build();
        this.ioman = new IOManagerAsync();

        // create test objects
        ArrayList<IntValue> objects = new ArrayList<IntValue>(NUM_TESTRECORDS);

        for (int i = 0; i < NUM_TESTRECORDS; ++i) {
            IntValue tmp = new IntValue(i);
            objects.add(tmp);
        }
        this.reader = objects.iterator();
    }

    @AfterEach
    void shutdown() throws Exception {
        this.ioman.close();
        this.ioman = null;

        assertThat(this.memman.verifyEmpty())
                .withFailMessage(
                        "A memory leak has occurred: Not all memory was properly returned to the memory manager.")
                .isTrue();
        this.memman.shutdown();
        this.memman = null;
    }

    /**
     * Tests the resettable iterator with too few memory, so that the data has to be written to
     * disk.
     */
    @Test
    void testResettableIterator() {
        try {
            // create the resettable Iterator
            SpillingResettableIterator<IntValue> iterator =
                    new SpillingResettableIterator<IntValue>(
                            this.reader,
                            this.serializer,
                            this.memman,
                            this.ioman,
                            2,
                            this.memOwner);
            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int count = 0;
            while (iterator.hasNext()) {
                assertThat(iterator.next().getValue())
                        .withFailMessage(
                                "In initial run, element %d does not match expected value!", count)
                        .isEqualTo(count++);
            }
            assertThat(count)
                    .withFailMessage("Too few elements were deserialized in initial run!")
                    .isEqualTo(NUM_TESTRECORDS);
            // test resetting the iterator a few times
            for (int j = 0; j < 10; ++j) {
                count = 0;
                iterator.reset();
                // now we should get the same results
                while (iterator.hasNext()) {
                    assertThat(iterator.next().getValue())
                            .withFailMessage(
                                    "After reset nr. %d element %d does not match expected value!",
                                    j + 1, count)
                            .isEqualTo(count++);
                }
                assertThat(count)
                        .withFailMessage(
                                "Too few elements were deserialized after reset nr. %d!", j + 1)
                        .isEqualTo(NUM_TESTRECORDS);
            }
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test encountered an exception.");
        }
    }

    /**
     * Tests the resettable iterator with enough memory so that all data is kept locally in a
     * membuffer.
     */
    @Test
    void testResettableIteratorInMemory() {
        try {
            // create the resettable Iterator
            SpillingResettableIterator<IntValue> iterator =
                    new SpillingResettableIterator<IntValue>(
                            this.reader,
                            this.serializer,
                            this.memman,
                            this.ioman,
                            20,
                            this.memOwner);
            // open the iterator
            iterator.open();

            // now test walking through the iterator
            int count = 0;
            while (iterator.hasNext()) {
                assertThat(iterator.next().getValue())
                        .withFailMessage(
                                "In initial run, element %d does not match expected value!", count)
                        .isEqualTo(count++);
            }
            assertThat(count)
                    .withFailMessage("Too few elements were deserialized in initial run!")
                    .isEqualTo(NUM_TESTRECORDS);
            // test resetting the iterator a few times
            for (int j = 0; j < 10; ++j) {
                count = 0;
                iterator.reset();
                // now we should get the same results
                while (iterator.hasNext()) {
                    assertThat(iterator.next().getValue())
                            .withFailMessage(
                                    "After reset nr. %d element %d does not match expected value!",
                                    j + 1, count)
                            .isEqualTo(count++);
                }
                assertThat(count)
                        .withFailMessage(
                                "Too few elements were deserialized after reset nr. %d!", j + 1)
                        .isEqualTo(NUM_TESTRECORDS);
            }
            // close the iterator
            iterator.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test encountered an exception.");
        }
    }

    /** Tests whether multiple call of hasNext() changes the state of the iterator */
    @Test
    void testHasNext() {
        try {
            // create the resettable Iterator
            SpillingResettableIterator<IntValue> iterator =
                    new SpillingResettableIterator<IntValue>(
                            this.reader,
                            this.serializer,
                            this.memman,
                            this.ioman,
                            2,
                            this.memOwner);
            // open the iterator
            iterator.open();

            int cnt = 0;
            while (iterator.hasNext()) {
                iterator.hasNext();
                iterator.next();
                cnt++;
            }

            assertThat(cnt)
                    .withFailMessage(
                            "%d elements read from iterator, but %d expected", cnt, NUM_TESTRECORDS)
                    .isEqualTo(NUM_TESTRECORDS);

            iterator.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test encountered an exception.");
        }
    }

    /** Test whether next() depends on previous call of hasNext() */
    @Test
    void testNext() {
        try {
            // create the resettable Iterator
            SpillingResettableIterator<IntValue> iterator =
                    new SpillingResettableIterator<IntValue>(
                            this.reader,
                            this.serializer,
                            this.memman,
                            this.ioman,
                            2,
                            this.memOwner);
            // open the iterator
            iterator.open();

            IntValue record;
            int cnt = 0;
            while (cnt < NUM_TESTRECORDS) {
                record = iterator.next();
                assertThat(record).withFailMessage("Record was not read from iterator").isNotNull();
                cnt++;
            }

            assertThatThrownBy(iterator::next)
                    .withFailMessage("Too many records were read from iterator.")
                    .isInstanceOf(NoSuchElementException.class);

            iterator.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test encountered an exception.");
        }
    }
}
