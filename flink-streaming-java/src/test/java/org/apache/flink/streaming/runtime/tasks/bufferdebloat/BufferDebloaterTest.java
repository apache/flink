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

package org.apache.flink.streaming.runtime.tasks.bufferdebloat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.runtime.io.MockInputGate;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.BYTES;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_ENABLED;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_TARGET;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link BufferDebloater}. */
public class BufferDebloaterTest extends TestLogger {

    @Test
    public void testZeroBuffersInUse() {
        // if the gate returns the zero buffers in use it should be transformed to 1.
        testBufferSizeCalculation(3, asList(0, 1, 0), 3333, 50, 2400, 1000, 1111);
    }

    @Test
    public void testCorrectBufferSizeCalculation() {
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, 1100, 1200, 249);
    }

    @Test
    public void testCalculatedBufferSizeLessThanMin() {
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 250, 1100, 1200, 250);
    }

    @Test
    public void testCalculatedBufferSizeForThroughputZero() {
        // When the throughput is zero then min buffer size will be taken.
        testBufferSizeCalculation(3, asList(3, 5, 8), 0, 50, 1100, 1200, 50);
    }

    @Test
    public void testConfiguredConsumptionTimeIsTooLow() {
        // When the consumption time is low then min buffer size will be taken.
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, 1100, 7, 50);
    }

    @Test
    public void testCalculatedBufferSizeGreaterThanMax() {
        // New calculated buffer size should be more than max value it means that we should take max
        // value which means that no updates should happen(-1 means that we take the initial value)
        // because the old value equal to new value.
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, 248, 1200, -1);
    }

    @Test
    public void testCalculatedBufferSlightlyDifferentFromCurrentOne() {
        // New calculated buffer size should be a little less than current value(or max value which
        // is the same) it means that no updates should happen(-1 means that we take the initial
        // value) because the new value is not so different from the old one.
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, 250, 1200, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeMinBufferSize() {
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, -1, 248, 1200, 248);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeMaxBufferSize() {
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, -1, 1200, 248);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMinGreaterThanMaxBufferSize() {
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, 49, 1200, 248);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeConsumptionTime() {
        testBufferSizeCalculation(3, asList(3, 5, 8), 3333, 50, 1100, -1, 248);
    }

    private void testBufferSizeCalculation(
            int numberOfGates,
            List<Integer> numberOfBuffersInUse,
            long throughput,
            long minBufferSize,
            long maxBufferSize,
            int consumptionTime,
            long expectedBufferSize) {
        TestBufferSizeInputGate[] inputGates = new TestBufferSizeInputGate[numberOfGates];
        for (int i = 0; i < numberOfGates; i++) {
            inputGates[i] = new TestBufferSizeInputGate(numberOfBuffersInUse.get(i));
        }

        BufferDebloater bufferDebloater =
                new BufferDebloater(
                        new Configuration()
                                .set(BUFFER_DEBLOAT_ENABLED, true)
                                .set(BUFFER_DEBLOAT_TARGET, Duration.ofMillis(consumptionTime))
                                .set(
                                        MEMORY_SEGMENT_SIZE,
                                        MemorySize.parse("" + maxBufferSize, BYTES))
                                .set(
                                        MIN_MEMORY_SEGMENT_SIZE,
                                        MemorySize.parse("" + minBufferSize, BYTES)),
                        inputGates);

        // when: Buffer size is calculated.
        bufferDebloater.recalculateBufferSize(throughput);

        // then: Buffer size is in all gates should be as expected.
        for (int i = 0; i < numberOfGates; i++) {
            assertThat(inputGates[i].lastBufferSize, is(expectedBufferSize));
        }
    }

    private static class TestBufferSizeInputGate extends MockInputGate {
        private long lastBufferSize = -1;
        private final int bufferInUseCount;

        public TestBufferSizeInputGate(int bufferInUseCount) {
            // Number of channels don't make sense here because
            super(1, Collections.emptyList());
            this.bufferInUseCount = bufferInUseCount;
        }

        @Override
        public int getBuffersInUseCount() {
            return bufferInUseCount;
        }

        @Override
        public void announceBufferSize(int bufferSize) {
            lastBufferSize = bufferSize;
        }
    }
}
