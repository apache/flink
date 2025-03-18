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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

/** Validate memory release under concurrent modification exceptions. */
class MemoryManagerConcurrentModReleaseTest {
    private static long testTimeoutMs = 5000;

    @Test
    void testConcurrentModificationOnce() throws MemoryAllocationException {
        final int numSegments = 10000;
        final int segmentSize = 4096;

        MemoryManager memMan =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(numSegments * segmentSize)
                        .setPageSize(segmentSize)
                        .build();

        ArrayList<MemorySegment> segs = new ListWithConcModExceptionOnFirstAccess<>();
        memMan.allocatePages(this, segs, numSegments);

        memMan.release(segs);
    }

    @Test
    void testConcurrentModificationWhileReleasing() throws Exception {
        final int numSegments = 10000;
        final int segmentSize = 4096;

        MemoryManager memMan =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(numSegments * segmentSize)
                        .setPageSize(segmentSize)
                        .build();

        ArrayList<MemorySegment> segs = new ArrayList<>(numSegments);
        memMan.allocatePages(this, segs, numSegments);

        // start a thread that performs concurrent modifications
        Modifier mod = new Modifier(segs, System.currentTimeMillis());
        Thread modRunner = new Thread(mod);
        modRunner.start();

        // give the thread some time to start working
        Thread.sleep(500);

        try {
            memMan.release(segs);
        } finally {
            mod.cancel();
        }

        modRunner.join();
    }

    private static class Modifier implements Runnable {

        private final ArrayList<MemorySegment> toModify;
        private final long startTimeMs;
        private final int multiplier = 2;
        private int exp = 0;

        private volatile boolean running = true;
        private volatile boolean timeout = false;

        private Modifier(ArrayList<MemorySegment> toModify, long startTimeMs) {
            this.toModify = toModify;
            this.startTimeMs = startTimeMs;
        }

        public void cancel() {
            running = false;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    MemorySegment seg = toModify.remove(0);
                    toModify.add(seg);
                    // if the test running time reaches TEST_TIMEOUT_MS, we can have exponential
                    // sleep to let it complete segment release sooner.
                    if (timeout || System.currentTimeMillis() - startTimeMs > testTimeoutMs) {
                        timeout = true;
                        // exponential sleep
                        Thread.sleep((long) Math.pow(multiplier, exp));
                        exp++;
                    }
                } catch (IndexOutOfBoundsException | InterruptedException e) {
                    // may happen, just retry
                }
            }
        }
    }

    private class ListWithConcModExceptionOnFirstAccess<E> extends ArrayList<E> {

        private static final long serialVersionUID = -1623249699823349781L;

        private boolean returnedIterator;

        @Override
        public Iterator<E> iterator() {
            if (returnedIterator) {
                return super.iterator();
            } else {
                returnedIterator = true;
                return new ConcFailingIterator<>();
            }
        }
    }

    private static class ConcFailingIterator<E> implements Iterator<E> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public E next() {
            throw new ConcurrentModificationException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
