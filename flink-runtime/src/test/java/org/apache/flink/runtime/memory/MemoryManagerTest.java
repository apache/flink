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
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the memory manager. */
class MemoryManagerTest {

    private static final long RANDOM_SEED = 643196033469871L;

    private static final int MEMORY_SIZE = 1024 * 1024 * 72; // 72 MiBytes

    private static final int PAGE_SIZE = 1024 * 32; // 32 KiBytes

    private static final int NUM_PAGES = MEMORY_SIZE / PAGE_SIZE;

    private MemoryManager memoryManager;

    private Random random;

    @BeforeEach
    void setUp() {
        this.memoryManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(MEMORY_SIZE)
                        .setPageSize(PAGE_SIZE)
                        .build();
        this.random = new Random(RANDOM_SEED);
    }

    @AfterEach
    void tearDown() {
        assertThat(this.memoryManager.verifyEmpty())
                .as("Memory manager is not complete empty and valid at the end of the test.")
                .isTrue();
        this.memoryManager = null;
        this.random = null;
    }

    @Test
    void allocateAllSingle() {
        final AbstractInvokable mockInvoke = new DummyInvokable();
        List<MemorySegment> segments = new ArrayList<>();

        try {
            for (int i = 0; i < NUM_PAGES; i++) {
                segments.add(this.memoryManager.allocatePages(mockInvoke, 1).get(0));
            }
        } catch (MemoryAllocationException e) {
            fail("Unable to allocate memory");
        }

        this.memoryManager.release(segments);
    }

    @Test
    void allocateAllMulti() {
        final AbstractInvokable mockInvoke = new DummyInvokable();
        final List<MemorySegment> segments = new ArrayList<>();

        try {
            for (int i = 0; i < NUM_PAGES / 2; i++) {
                segments.addAll(this.memoryManager.allocatePages(mockInvoke, 2));
            }
        } catch (MemoryAllocationException e) {
            fail("Unable to allocate memory");
        }

        this.memoryManager.release(segments);
    }

    @Test
    void allocateMultipleOwners() throws Exception {
        final int numOwners = 17;

        AbstractInvokable[] owners = new AbstractInvokable[numOwners];

        @SuppressWarnings("unchecked")
        List<MemorySegment>[] mems = (List<MemorySegment>[]) new List<?>[numOwners];

        for (int i = 0; i < numOwners; i++) {
            owners[i] = new DummyInvokable();
            mems[i] = new ArrayList<>(64);
        }

        // allocate all memory to the different owners
        for (int i = 0; i < NUM_PAGES; i++) {
            final int owner = this.random.nextInt(numOwners);
            mems[owner].addAll(this.memoryManager.allocatePages(owners[owner], 1));
        }

        // free one owner at a time
        for (int i = 0; i < numOwners; i++) {
            this.memoryManager.releaseAll(owners[i]);
            owners[i] = null;
            assertThat(allMemorySegmentsFreed(mems[i]))
                    .as("Released memory segments have not been destroyed.")
                    .isTrue();
            mems[i] = null;

            // check that the owner owners were not affected
            for (int k = i + 1; k < numOwners; k++) {
                assertThat(allMemorySegmentsValid(mems[k]))
                        .as("Non-released memory segments are accidentaly destroyed.")
                        .isTrue();
            }
        }
    }

    @Test
    void allocateTooMuch() throws Exception {
        final AbstractInvokable mockInvoke = new DummyInvokable();

        List<MemorySegment> segs = this.memoryManager.allocatePages(mockInvoke, NUM_PAGES);

        testCannotAllocateAnymore(mockInvoke, 1);

        assertThat(allMemorySegmentsValid(segs))
                .as("The previously allocated segments were not valid any more.")
                .isTrue();

        this.memoryManager.releaseAll(mockInvoke);
    }

    @Test
    void doubleReleaseReturnsMemoryOnlyOnce() throws MemoryAllocationException {
        final AbstractInvokable mockInvoke = new DummyInvokable();

        Collection<MemorySegment> segs = this.memoryManager.allocatePages(mockInvoke, NUM_PAGES);
        MemorySegment segment = segs.iterator().next();

        this.memoryManager.release(segment);
        this.memoryManager.release(segment);

        testCannotAllocateAnymore(mockInvoke, 2);

        this.memoryManager.releaseAll(mockInvoke);
    }

    @Test
    void releaseCollectionAfterReleaseAll() throws MemoryAllocationException {
        final AbstractInvokable mockInvoke = new DummyInvokable();

        Collection<MemorySegment> segs = this.memoryManager.allocatePages(mockInvoke, 1);
        MemorySegment segment = segs.iterator().next();

        this.memoryManager.releaseAll(mockInvoke);
        // the collection must be modifiable
        this.memoryManager.release(new ArrayList<>(Collections.singletonList(segment)));
        // there is no exception. see FLINK-21728
    }

    @Test
    void releaseAfterReleaseAll() throws MemoryAllocationException {
        final AbstractInvokable mockInvoke = new DummyInvokable();

        Collection<MemorySegment> segs = this.memoryManager.allocatePages(mockInvoke, 1);
        MemorySegment segment = segs.iterator().next();

        this.memoryManager.releaseAll(mockInvoke);
        this.memoryManager.release(segment);
        // there is no exception. see FLINK-21728
    }

    @Test
    void releaseSameSegmentFromTwoCollections() throws MemoryAllocationException {
        final AbstractInvokable mockInvoke = new DummyInvokable();

        MemorySegment seg1 = this.memoryManager.allocatePages(mockInvoke, 1).get(0);
        MemorySegment seg2 = this.memoryManager.allocatePages(mockInvoke, 1).get(0);
        MemorySegment seg3 = this.memoryManager.allocatePages(mockInvoke, 1).get(0);

        // the collection must be modifiable
        this.memoryManager.release(new ArrayList<>(Arrays.asList(seg1, seg2)));
        this.memoryManager.release(new ArrayList<>(Arrays.asList(seg2, seg3)));
        // there is no exception. see FLINK-21728
    }

    private boolean allMemorySegmentsValid(List<MemorySegment> memSegs) {
        for (MemorySegment seg : memSegs) {
            if (seg.isFreed()) {
                return false;
            }
        }
        return true;
    }

    private boolean allMemorySegmentsFreed(List<MemorySegment> memSegs) {
        for (MemorySegment seg : memSegs) {
            if (!seg.isFreed()) {
                return false;
            }
        }
        return true;
    }

    @Test
    void testMemoryReservation() throws MemoryReservationException {
        Object owner = new Object();

        memoryManager.reserveMemory(owner, PAGE_SIZE);
        memoryManager.releaseMemory(owner, PAGE_SIZE);
    }

    @Test
    void testAllMemoryReservation() throws MemoryReservationException {
        Object owner = new Object();

        memoryManager.reserveMemory(owner, memoryManager.getMemorySize());
        memoryManager.releaseAllMemory(owner);
    }

    @Test
    void testCannotReserveBeyondTheLimit() throws MemoryReservationException {
        Object owner = new Object();
        memoryManager.reserveMemory(owner, memoryManager.getMemorySize());
        testCannotReserveAnymore(1L);
        memoryManager.releaseAllMemory(owner);
    }

    @Test
    void testMemoryTooBigReservation() {
        long size = memoryManager.getMemorySize() + PAGE_SIZE;
        testCannotReserveAnymore(size);
    }

    @Test
    void testMemoryReleaseMultipleTimes() throws MemoryReservationException {
        Object owner = new Object();
        Object owner2 = new Object();
        long totalHeapMemorySize = memoryManager.availableMemory();
        // to prevent memory size exceeding the limit, reserve some memory from another owner.
        memoryManager.reserveMemory(owner2, PAGE_SIZE);

        // reserve once but release twice
        memoryManager.reserveMemory(owner, PAGE_SIZE);
        memoryManager.releaseMemory(owner, PAGE_SIZE);
        memoryManager.releaseMemory(owner, PAGE_SIZE);
        long heapMemoryLeft = memoryManager.availableMemory();
        assertThat(heapMemoryLeft)
                .as("Memory leak happens")
                .isEqualTo(totalHeapMemorySize - PAGE_SIZE);
        memoryManager.releaseAllMemory(owner2);
    }

    @Test
    void testMemoryReleaseMoreThanReserved() throws MemoryReservationException {
        Object owner = new Object();
        Object owner2 = new Object();
        long totalHeapMemorySize = memoryManager.availableMemory();
        // to prevent memory size exceeding the limit, reserve some memory from another owner.
        memoryManager.reserveMemory(owner2, PAGE_SIZE);

        // release more than reserved size
        memoryManager.reserveMemory(owner, PAGE_SIZE);
        memoryManager.releaseMemory(owner, PAGE_SIZE * 2);
        long heapMemoryLeft = memoryManager.availableMemory();
        assertThat(heapMemoryLeft)
                .as("Memory leak happens")
                .isEqualTo(totalHeapMemorySize - PAGE_SIZE);
        memoryManager.releaseAllMemory(owner2);
    }

    @Test
    void testMemoryAllocationAndReservation()
            throws MemoryAllocationException, MemoryReservationException {
        @SuppressWarnings("NumericCastThatLosesPrecision")
        int totalPagesForType = (int) memoryManager.getMemorySize() / PAGE_SIZE;

        // allocate half memory for segments
        Object owner1 = new Object();
        memoryManager.allocatePages(owner1, totalPagesForType / 2);

        // reserve the other half of memory
        Object owner2 = new Object();
        memoryManager.reserveMemory(owner2, (long) PAGE_SIZE * totalPagesForType / 2);

        testCannotAllocateAnymore(new Object(), 1);
        testCannotReserveAnymore(1L);

        memoryManager.releaseAll(owner1);
        memoryManager.releaseAllMemory(owner2);
    }

    @Test
    void testComputeMemorySize() {
        double fraction = 0.6;
        assertThat(memoryManager.computeMemorySize(fraction))
                .isEqualTo((long) (memoryManager.getMemorySize() * fraction));

        fraction = 1.0;
        assertThat(memoryManager.computeMemorySize(fraction))
                .isEqualTo((long) (memoryManager.getMemorySize() * fraction));
    }

    @Test
    void testComputeMemorySizeFailForZeroFraction() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> memoryManager.computeMemorySize(0.0));
    }

    @Test
    void testComputeMemorySizeFailForTooLargeFraction() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> memoryManager.computeMemorySize(1.1));
    }

    @Test
    void testComputeMemorySizeFailForNegativeFraction() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> memoryManager.computeMemorySize(-0.1));
    }

    @Test
    void testVerifyEmptyCanBeDoneAfterShutdown()
            throws MemoryAllocationException, MemoryReservationException {
        memoryManager.release(memoryManager.allocatePages(new Object(), 1));
        Object owner = new Object();
        memoryManager.reserveMemory(owner, MemoryManager.DEFAULT_PAGE_SIZE);
        memoryManager.releaseMemory(owner, MemoryManager.DEFAULT_PAGE_SIZE);
        memoryManager.shutdown();
        memoryManager.verifyEmpty();
    }

    private void testCannotAllocateAnymore(Object owner, int numPages) {
        assertThatExceptionOfType(MemoryAllocationException.class)
                .as(
                        "Expected MemoryAllocationException. "
                                + "We should not be able to allocate after allocating or(and) reserving all memory of a certain type.")
                .isThrownBy(() -> memoryManager.allocatePages(owner, numPages));
    }

    private void testCannotReserveAnymore(long size) {
        assertThatExceptionOfType(MemoryReservationException.class)
                .as(
                        "Expected MemoryAllocationException. "
                                + "We should not be able to any more memory after allocating or(and) reserving all memory of a certain type.")
                .isThrownBy(() -> memoryManager.reserveMemory(new Object(), size));
    }
}
