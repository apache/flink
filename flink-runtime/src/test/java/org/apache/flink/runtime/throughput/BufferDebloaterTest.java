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

package org.apache.flink.runtime.throughput;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.OptionalInt;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
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
                .withNumberOfBuffersInUse(0)
                .withThroughput(1200)
                .expectBufferSize(1200);
    }

    @Test
    public void testCorrectBufferSizeCalculation() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(16)
                .withThroughput(3333)
                .expectBufferSize(249);
    }

    @Test
    public void testCalculatedBufferSizeLessThanMin() {
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(250, 1100)
                .withNumberOfBuffersInUse(16)
                .withThroughput(3333)
                .expectBufferSize(250);
    }

    @Test
    public void testCalculatedBufferSizeForThroughputZero() {
        // When the throughput is zero then min buffer size will be taken.
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(16)
                .withThroughput(0)
                .expectBufferSize(50);
    }

    @Test
    public void testConfiguredConsumptionTimeIsTooLow() {
        // When the consumption time is low then min buffer size will be taken.
        testBufferDebloater()
                .withDebloatTarget(7)
                .withBufferSize(50, 1100)
                .withNumberOfBuffersInUse(16)
                .withThroughput(3333)
                .expectBufferSize(50);
    }

    @Test
    public void testCalculatedBufferSizeGreaterThanMax() {
        // New calculated buffer size should be more than max value it means that we should take max
        // value which means that no updates should happen because the old value equal to new value.
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 248)
                .withNumberOfBuffersInUse(16)
                .withThroughput(3333)
                .expectNoChangeInBufferSize();
    }

    @Test
    public void testCalculatedBufferSlightlyDifferentFromCurrentOne() {
        // New calculated buffer size should be a little less than current value(or max value which
        // is the same) it means that no updates should happen because the new value is not so
        // different from the old one.
        testBufferDebloater()
                .withDebloatTarget(1200)
                .withBufferSize(50, 250)
                .withNumberOfBuffersInUse(16)
                .withThroughput(3333)
                .expectNoChangeInBufferSize();
    }

    @Test
    public void testAnnouncedMaxBufferSizeDespiteLastDiffLessThanThreshold() {
        final int numberOfBuffersInUse = 1;
        BufferDebloater bufferDebloater =
                testBufferDebloater()
                        .withDebloatTarget(1000)
                        .withBufferSize(50, 1100)
                        .withNumberOfBuffersInUse(numberOfBuffersInUse)
                        .withThroughput(500)
                        .expectBufferSize(500);

        // Calculate the buffer size a little lower than the max buffer size.
        bufferDebloater.recalculateBufferSize(1000, numberOfBuffersInUse);
        assertThat(bufferDebloater.getLastBufferSize(), is(1000));

        // Recalculate the buffer size to max value.
        bufferDebloater.recalculateBufferSize(2000, numberOfBuffersInUse);

        // The max value should be announced despite it differ from the previous one by less than
        // threshold value.
        assertThat(bufferDebloater.getLastBufferSize(), is(1100));

        // Make sure that there is no repeated announcement of max buffer size.
        bufferDebloater.recalculateBufferSize(2000, numberOfBuffersInUse);
    }

    @Test
    public void testAnnouncedMinBufferSizeEvenDespiteLastDiffLessThanThreshold() {
        final int numberOfBuffersInUse = 1;
        BufferDebloater bufferDebloater =
                testBufferDebloater()
                        .withDebloatTarget(1000)
                        .withBufferSize(50, 1100)
                        .withNumberOfBuffersInUse(numberOfBuffersInUse)
                        .withThroughput(60)
                        .expectBufferSize(60);

        // Calculate the buffer size a little greater than the min buffer size.
        bufferDebloater.recalculateBufferSize(60, numberOfBuffersInUse);
        assertThat(bufferDebloater.getLastBufferSize(), is(60));

        // Recalculate the buffer size to min value.
        bufferDebloater.recalculateBufferSize(40, numberOfBuffersInUse);

        // The min value should be announced despite it differ from the previous one by less than
        // threshold value.
        assertThat(bufferDebloater.getLastBufferSize(), is(50));

        // Make sure that there is no repeated announcement of min buffer size.
        bufferDebloater.recalculateBufferSize(40, numberOfBuffersInUse);
    }

    @Test
    public void testSkipUpdate() {
        int maxBufferSize = 32768;
        int minBufferSize = 256;
        double threshold = 0.3;
        BufferDebloater bufferDebloater =
                testBufferDebloater()
                        .withDebloatTarget(1000)
                        .withBufferSize(minBufferSize, maxBufferSize)
                        // 30 % Threshold.
                        .withThresholdPercentages((int) (threshold * 100))
                        .getBufferDebloater();

        int currentBufferSize = maxBufferSize / 2;

        OptionalInt optionalInt = bufferDebloater.recalculateBufferSize(currentBufferSize, 1);
        assertTrue(optionalInt.isPresent());
        assertEquals(currentBufferSize, optionalInt.getAsInt());

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

    public static BufferDebloaterTestBuilder testBufferDebloater() {
        return new BufferDebloaterTestBuilder();
    }

    private static class BufferDebloaterTestBuilder {
        private int numberOfBuffersInUse;
        private long throughput;
        private int minBufferSize;
        private int maxBufferSize;
        private int debloatTarget;
        private int thresholdPercentages = BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES.defaultValue();

        public BufferDebloaterTestBuilder withNumberOfBuffersInUse(Integer numberOfBuffersInUse) {
            this.numberOfBuffersInUse = numberOfBuffersInUse;
            return this;
        }

        public BufferDebloaterTestBuilder withThroughput(long throughput) {
            this.throughput = throughput;
            return this;
        }

        public BufferDebloaterTestBuilder withBufferSize(int minBufferSize, int maxBufferSize) {
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

        public void expectNoChangeInBufferSize() {
            BufferDebloater bufferDebloater = getBufferDebloater();

            // when: Buffer size is calculated.
            final OptionalInt newBufferSize =
                    bufferDebloater.recalculateBufferSize(throughput, numberOfBuffersInUse);

            assertFalse(newBufferSize.isPresent());
        }

        public BufferDebloater expectBufferSize(int expectedBufferSize) {
            BufferDebloater bufferDebloater = getBufferDebloater();

            // when: Buffer size is calculated.
            final OptionalInt newBufferSize =
                    bufferDebloater.recalculateBufferSize(throughput, numberOfBuffersInUse);

            assertTrue(newBufferSize.isPresent());
            assertThat(newBufferSize.getAsInt(), is(expectedBufferSize));
            return bufferDebloater;
        }

        private BufferDebloater getBufferDebloater() {
            return new BufferDebloater(
                    0, debloatTarget, maxBufferSize, minBufferSize, thresholdPercentages, 1);
        }
    }
}
