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
import static java.util.Collections.singletonList;
import static org.apache.flink.configuration.MemorySize.MemoryUnit.BYTES;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_ENABLED;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_TARGET;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test for {@link BufferDebloater}. */
public class BufferDebloaterTest extends TestLogger {

    @Test
    public void testZeroBuffersInUse() {
        // if the gate returns the zero buffers in use it should be transformed to 1.
        testBufferDebloater()
                .withDebloatTarget(1000)
                .withBufferSize(50, 2400)
                .withNumberOfBuffersInUse(asList(0, 1, 0))
                .withThroughput(3333)
                .expectBufferSize(1111);
    }

    @Test
    public void testCorrectBufferSizeCalculation() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(249);
    }

    @Test
    public void testCalculatedBufferSizeLessThanMin() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(250, 1100)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(250);
    }

    @Test
    public void testCalculatedBufferSizeForThroughputZero() {
        // When the throughput is zero then min buffer size will be taken.
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(0)
                .expectBufferSize(50);
    }

    @Test
    public void testConfiguredConsumptionTimeIsTooLow() {
        // When the consumption time is low then min buffer size will be taken.
        testBufferDebloater()
                .withDebloatTarget(7)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(50);
    }

    @Test
    public void testCalculatedBufferSizeGreaterThanMax() {
        // New calculated buffer size should be more than max value it means that we should take max
        // value which means that no updates should happen(-1 means that we take the initial value)
        // because the old value equal to new value.
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 248)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(-1);
    }

    @Test
    public void testCalculatedBufferSlightlyDifferentFromCurrentOne() {
        // New calculated buffer size should be a little less than current value(or max value which
        // is the same) it means that no updates should happen(-1 means that we take the initial
        // value) because the new value is not so different from the old one.
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 250)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeMinBufferSize() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(-1, 248)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(248);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeMaxBufferSize() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, -1)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(248);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMinGreaterThanMaxBufferSize() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 49)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(248);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeConsumptionTime() {
        testBufferDebloater()
                .withDebloatTarget(-1)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(asList(3, 5, 8))
                .withThroughput(3333)
                .expectBufferSize(248);
    }

    @Test
    public void testAnnouncedMaxBufferSizeDespiteLastDiffLessThanThreshold() {
        BufferDebloater bufferDebloater =
                testBufferDebloater()
                        .withDebloatTarget(1000)
                        .withBufferSize(50, 1100)
                        .withNumberOfBuffersInUse(singletonList(1))
                        .withThroughput(500)
                        .expectBufferSize(500);

        // Calculate the buffer size a little lower than the max buffer size.
        bufferDebloater.recalculateBufferSize(1000);
        assertThat(bufferDebloater.getLastBufferSize(), is(1000));

        // Recalculate the buffer size to max value.
        bufferDebloater.recalculateBufferSize(2000);

        // The max value should be announced despite it differ from the previous one by less than
        // threshold value.
        assertThat(bufferDebloater.getLastBufferSize(), is(1100));

        // Make sure that there is no repeated announcement of max buffer size.
        bufferDebloater.recalculateBufferSize(2000);
    }

    @Test
    public void testAnnouncedMinBufferSizeEvenDespiteLastDiffLessThanThreshold() {
        BufferDebloater bufferDebloater =
                testBufferDebloater()
                        .withDebloatTarget(1000)
                        .withBufferSize(50, 1100)
                        .withNumberOfBuffersInUse(singletonList(1))
                        .withThroughput(60)
                        .expectBufferSize(60);

        // Calculate the buffer size a little greater than the min buffer size.
        bufferDebloater.recalculateBufferSize(60);
        assertThat(bufferDebloater.getLastBufferSize(), is(60));

        // Recalculate the buffer size to min value.
        bufferDebloater.recalculateBufferSize(40);

        // The min value should be announced despite it differ from the previous one by less than
        // threshold value.
        assertThat(bufferDebloater.getLastBufferSize(), is(50));

        // Make sure that there is no repeated announcement of min buffer size.
        bufferDebloater.recalculateBufferSize(40);
    }

    @Test
    public void testSkipUpdate() {
        int maxBufferSize = 32768;
        int minBufferSize = 256;
        double threshold = 0.3;
        int currentBufferSize = maxBufferSize / 2;
        BufferDebloater bufferDebloater =
                testBufferDebloater()
                        .withDebloatTarget(1000)
                        .withBufferSize(minBufferSize, maxBufferSize)
                        // 30 % Threshold.
                        .withThresholdPercentages((int) (threshold * 100))
                        .withThroughput(currentBufferSize)
                        .withNumberOfBuffersInUse(singletonList(1))
                        .expectBufferSize(currentBufferSize);

        // It is true because less than threshold.
        assertTrue(bufferDebloater.skipUpdate(currentBufferSize));
        assertTrue(bufferDebloater.skipUpdate(currentBufferSize - 1));
        assertTrue(bufferDebloater.skipUpdate(currentBufferSize + 1));

        assertTrue(
                bufferDebloater.skipUpdate(
                        currentBufferSize - (int) (currentBufferSize * threshold) + 1));
        assertTrue(
                bufferDebloater.skipUpdate(
                        currentBufferSize + (int) (currentBufferSize * threshold) - 1));

        // It is false because it reaches threshold.
        assertFalse(
                bufferDebloater.skipUpdate(
                        currentBufferSize - (int) (currentBufferSize * threshold)));
        assertFalse(
                bufferDebloater.skipUpdate(
                        currentBufferSize + (int) (currentBufferSize * threshold)));
        assertFalse(bufferDebloater.skipUpdate(minBufferSize + 1));
        assertFalse(bufferDebloater.skipUpdate(minBufferSize));
        assertFalse(bufferDebloater.skipUpdate(maxBufferSize - 1));
        assertFalse(bufferDebloater.skipUpdate(maxBufferSize));

        // Beyond the min and max size is always false.
        assertFalse(bufferDebloater.skipUpdate(maxBufferSize + 1));
        assertFalse(bufferDebloater.skipUpdate(minBufferSize - 1));
    }

    private static class TestBufferSizeInputGate extends MockInputGate {
        private int lastBufferSize = -1;
        private final int bufferInUseCount;

        public TestBufferSizeInputGate(int bufferInUseCount) {
            // Number of channels don't make sense here because
            super(1, Collections.emptyList(), false);
            this.bufferInUseCount = bufferInUseCount;
        }

        @Override
        public int getBuffersInUseCount() {
            return bufferInUseCount;
        }

        @Override
        public void announceBufferSize(int bufferSize) {
            // Announce the same value doesn't make sense.
            assertThat(bufferSize, is(not(lastBufferSize)));
            lastBufferSize = bufferSize;
        }
    }

    public static BufferDebloaterTestBuilder testBufferDebloater() {
        return new BufferDebloaterTestBuilder();
    }

    private static class BufferDebloaterTestBuilder {
        private List<Integer> numberOfBuffersInUse;
        private long throughput;
        private long minBufferSize;
        private long maxBufferSize;
        private int debloatTarget;
        private int thresholdPercentages = BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES.defaultValue();

        public BufferDebloaterTestBuilder withNumberOfBuffersInUse(
                List<Integer> numberOfBuffersInUse) {
            this.numberOfBuffersInUse = numberOfBuffersInUse;
            return this;
        }

        public BufferDebloaterTestBuilder withThroughput(long throughput) {
            this.throughput = throughput;
            return this;
        }

        public BufferDebloaterTestBuilder withBufferSize(long minBufferSize, long maxBufferSize) {
            this.minBufferSize = minBufferSize;
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public BufferDebloaterTestBuilder withDebloatTarget(int debloatTarget) {
            this.debloatTarget = debloatTarget;
            return this;
        }

        public BufferDebloaterTestBuilder withThresholdPercentages(int thresholdPercentages) {
            this.thresholdPercentages = thresholdPercentages;
            return this;
        }

        public BufferDebloater expectBufferSize(int expectedBufferSize) {
            int numberOfGates = numberOfBuffersInUse.size();
            TestBufferSizeInputGate[] inputGates = new TestBufferSizeInputGate[numberOfGates];
            for (int i = 0; i < numberOfGates; i++) {
                inputGates[i] = new TestBufferSizeInputGate(numberOfBuffersInUse.get(i));
            }

            BufferDebloater bufferDebloater =
                    new BufferDebloater(
                            new Configuration()
                                    .set(BUFFER_DEBLOAT_ENABLED, true)
                                    .set(BUFFER_DEBLOAT_TARGET, Duration.ofMillis(debloatTarget))
                                    .set(BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES, thresholdPercentages)
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

            return bufferDebloater;
        }
    }
}
