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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Validate memory release under concurrent modification exceptions. */
class MemoryManagerConcurrentModReleaseTest {

    /**
     * Number of {@code release()} passes over the list that the modifier thread changes. {@code
     * release()} retries until one pass sees no concurrent modification, so the modifier has to
     * stop at some point: a modifier that never stops can keep {@code release()} retrying forever.
     */
    private static final int PASSES_WITH_MODIFICATION = 10;

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

    /**
     * Checks {@link MemoryManager#release(java.util.Collection)} when another thread changes the
     * list that is being released. The list holds the memory segments that were allocated from the
     * memory manager, and {@code release()} passes over it to give every segment back. A pass that
     * sees a change by the other thread is started again from the first segment. Once the other
     * thread has stopped, one complete pass gives back the remaining segments and empties the list.
     */
    @Test
    void testConcurrentModificationWhileReleasing() throws Exception {
        final int numSegments = 10000;
        final int segmentSize = 4096;

        MemoryManager memMan =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(numSegments * segmentSize)
                        .setPageSize(segmentSize)
                        .build();

        ModifiedWhileReleasingList segs =
                new ModifiedWhileReleasingList(numSegments, PASSES_WITH_MODIFICATION);
        memMan.allocatePages(this, segs, numSegments);

        // start a thread that performs concurrent modifications
        Modifier mod = new Modifier(segs);
        Thread modRunner = new Thread(mod);
        modRunner.start();

        try {
            memMan.release(segs);
        } finally {
            mod.cancel();
            modRunner.join();
        }

        // every pass that saw a modification was retried, then one pass succeeded
        assertThat(segs.getPasses()).isEqualTo(PASSES_WITH_MODIFICATION + 1);
        assertThat(segs).isEmpty();
        assertThat(memMan.verifyEmpty()).isTrue();
    }

    private static class Modifier implements Runnable {

        private final ModifiedWhileReleasingList toModify;

        private volatile boolean running = true;

        private Modifier(ModifiedWhileReleasingList toModify) {
            this.toModify = toModify;
        }

        public void cancel() {
            running = false;
        }

        @Override
        public void run() {
            try {
                while (running && toModify.isModificationAllowed()) {
                    MemorySegment seg = toModify.remove(0);
                    toModify.add(seg);
                    toModify.modified();
                }
            } finally {
                toModify.modifierStopped();
            }
        }
    }

    /**
     * The list of allocated memory segments that is given to {@code release()}. {@code release()}
     * passes over it while the {@link Modifier} changes it from another thread.
     *
     * <p>The modifier moves segments without pausing. To make the test deterministic without
     * sleeping, every one of the first {@code passesWithModification} passes waits halfway until
     * the modifier has moved a segment since the pass started, so the pass is guaranteed to see a
     * concurrent modification. Any later pass first waits until the modifier has stopped, so that
     * this pass and the {@code clear()} that {@code release()} calls on the list after its last
     * pass never overlap with a move.
     */
    private static class ModifiedWhileReleasingList extends ArrayList<MemorySegment> {

        private static final long serialVersionUID = 1L;

        private final int passesWithModification;
        private final int waitAtSegment;
        private final AtomicInteger passes = new AtomicInteger();
        private final AtomicLong modifications = new AtomicLong();
        private final AtomicBoolean modifierStopped = new AtomicBoolean();

        private ModifiedWhileReleasingList(int numSegments, int passesWithModification) {
            super(numSegments);
            this.passesWithModification = passesWithModification;
            this.waitAtSegment = numSegments / 2;
        }

        int getPasses() {
            return passes.get();
        }

        boolean isModificationAllowed() {
            return passes.get() <= passesWithModification;
        }

        void modified() {
            modifications.incrementAndGet();
        }

        void modifierStopped() {
            modifierStopped.set(true);
        }

        @Override
        public Iterator<MemorySegment> iterator() {
            if (passes.incrementAndGet() > passesWithModification) {
                while (!modifierStopped.get()) {
                    Thread.onSpinWait();
                }
                return super.iterator();
            }

            final Iterator<MemorySegment> pass = super.iterator();
            // The next counted move may have completed before the iterator was created; the one
            // after it started afterwards, so this pass sees it.
            final long seenModifications = modifications.get() + 2;
            return new Iterator<MemorySegment>() {
                private int returned;

                @Override
                public boolean hasNext() {
                    return pass.hasNext();
                }

                @Override
                public MemorySegment next() {
                    if (++returned == waitAtSegment) {
                        while (modifications.get() < seenModifications) {
                            Thread.onSpinWait();
                        }
                    }
                    return pass.next();
                }
            };
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
