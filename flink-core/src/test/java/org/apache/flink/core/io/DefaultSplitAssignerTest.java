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

package org.apache.flink.core.io;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSplitAssignerTest {

    @Test
    void testSerialSplitAssignment() {
        final int NUM_SPLITS = 50;

        Set<InputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new GenericInputSplit(i, NUM_SPLITS));
        }

        DefaultInputSplitAssigner ia = new DefaultInputSplitAssigner(splits);
        InputSplit is = null;
        while ((is = ia.getNextInputSplit("", 0)) != null) {
            assertThat(splits.remove(is)).isTrue();
        }

        assertThat(splits).isEmpty();
        assertThat(ia.getNextInputSplit("", 0)).isNull();
    }

    @Test
    void testConcurrentSplitAssignment() throws InterruptedException {
        final int NUM_THREADS = 10;
        final int NUM_SPLITS = 500;
        final int SUM_OF_IDS = (NUM_SPLITS - 1) * (NUM_SPLITS) / 2;

        Set<InputSplit> splits = new HashSet<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            splits.add(new GenericInputSplit(i, NUM_SPLITS));
        }

        final DefaultInputSplitAssigner ia = new DefaultInputSplitAssigner(splits);

        final AtomicInteger splitsRetrieved = new AtomicInteger(0);
        final AtomicInteger sumOfIds = new AtomicInteger(0);

        Runnable retriever =
                () -> {
                    String host = "";
                    GenericInputSplit split;
                    while ((split = (GenericInputSplit) ia.getNextInputSplit(host, 0)) != null) {
                        splitsRetrieved.incrementAndGet();
                        sumOfIds.addAndGet(split.getSplitNumber());
                    }
                };

        // create the threads
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(retriever);
            threads[i].setDaemon(true);
        }

        // launch concurrently
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].start();
        }

        // sync
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join(5000);
        }

        // verify
        for (int i = 0; i < NUM_THREADS; i++) {
            assertThat(threads[i].isAlive()).isFalse();
        }

        assertThat(splitsRetrieved).hasValue(NUM_SPLITS);
        assertThat(sumOfIds).hasValue(SUM_OF_IDS);

        // nothing left
        assertThat(ia.getNextInputSplit("", 0)).isNull();
    }
}
