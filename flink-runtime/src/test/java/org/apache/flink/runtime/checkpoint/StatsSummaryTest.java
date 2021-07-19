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

package org.apache.flink.runtime.checkpoint;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

public class StatsSummaryTest {

    /** Test the initial/empty state. */
    @Test
    public void testInitialState() throws Exception {
        StatsSummary mma = new StatsSummary();

        assertEquals(0, mma.getMinimum());
        assertEquals(0, mma.getMaximum());
        assertEquals(0, mma.getSum());
        assertEquals(0, mma.getCount());
        assertEquals(0, mma.getAverage());
    }

    /** Test that non-positive numbers are not counted. */
    @Test
    public void testAddNonPositiveStats() throws Exception {
        StatsSummary mma = new StatsSummary();
        mma.add(-1);

        assertEquals(0, mma.getMinimum());
        assertEquals(0, mma.getMaximum());
        assertEquals(0, mma.getSum());
        assertEquals(0, mma.getCount());
        assertEquals(0, mma.getAverage());

        mma.add(0);

        assertEquals(0, mma.getMinimum());
        assertEquals(0, mma.getMaximum());
        assertEquals(0, mma.getSum());
        assertEquals(1, mma.getCount());
        assertEquals(0, mma.getAverage());
    }

    /** Test sequence of random numbers. */
    @Test
    public void testAddRandomNumbers() throws Exception {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        StatsSummary mma = new StatsSummary();

        long count = 13;
        long sum = 0;
        long min = Integer.MAX_VALUE;
        long max = Integer.MIN_VALUE;

        for (int i = 0; i < count; i++) {
            int number = rand.nextInt(124) + 1;
            sum += number;
            min = Math.min(min, number);
            max = Math.max(max, number);

            mma.add(number);
        }

        assertEquals(min, mma.getMinimum());
        assertEquals(max, mma.getMaximum());
        assertEquals(sum, mma.getSum());
        assertEquals(count, mma.getCount());
        assertEquals(sum / count, mma.getAverage());
    }

    @Test
    public void testQuantile() {
        StatsSummary summary = new StatsSummary(100);
        for (int i = 0; i < 123; i++) {
            summary.add(100000); // should be forced out by the later values
        }
        for (int i = 1; i <= 100; i++) {
            summary.add(i);
        }
        StatsSummarySnapshot snapshot = summary.createSnapshot();
        for (double q = 0.01; q <= 1; q++) {
            assertEquals(q, snapshot.getQuantile(q), 1);
        }
    }
}
