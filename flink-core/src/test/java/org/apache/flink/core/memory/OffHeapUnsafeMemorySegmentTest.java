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

package org.apache.flink.core.memory;

import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link MemorySegment} in off-heap mode using unsafe memory. */
@ExtendWith(ParameterizedTestExtension.class)
class OffHeapUnsafeMemorySegmentTest extends MemorySegmentTestBase {

    OffHeapUnsafeMemorySegmentTest(int pageSize) {
        super(pageSize);
    }

    @Override
    MemorySegment createSegment(int size) {
        return MemorySegmentFactory.allocateOffHeapUnsafeMemory(size);
    }

    @Override
    MemorySegment createSegment(int size, Object owner) {
        return MemorySegmentFactory.allocateOffHeapUnsafeMemory(size, owner, () -> {});
    }

    @TestTemplate
    @Override
    void testByteBufferWrapping(int pageSize) {
        assertThatThrownBy(() -> createSegment(10).wrap(1, 2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @TestTemplate
    void testCallCleanerOnFree() {
        final CompletableFuture<Void> cleanerFuture = new CompletableFuture<>();
        MemorySegmentFactory.allocateOffHeapUnsafeMemory(
                        10, null, () -> cleanerFuture.complete(null))
                .free();
        assertThat(cleanerFuture).isDone();
    }

    @TestTemplate
    void testCallCleanerOnceOnConcurrentFree() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        final Runnable cleaner =
                () -> {
                    try {
                        counter.incrementAndGet();
                        // make the cleaner unlikely to finish before another invocation (if any)
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                };

        final MemorySegment segment =
                MemorySegmentFactory.allocateOffHeapUnsafeMemory(10, null, cleaner);

        final Thread t1 = new Thread(segment::free);
        final Thread t2 = new Thread(segment::free);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertThat(counter).hasValue(1);
    }
}
