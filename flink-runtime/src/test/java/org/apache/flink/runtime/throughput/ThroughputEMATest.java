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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Test for {@link ThroughputEMA}. */
public class ThroughputEMATest extends TestLogger {
    @Test
    public void testWarmUpThroughputCalculation() {
        ThroughputEMA calculator = new ThroughputEMA(5);

        // Calculation throughput bytes/second.
        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4040L));
        assertThat(calculator.calculateThroughput(100, 9), is(6397L));
    }

    @Test
    public void testWarmUpIsFinished() {
        ThroughputEMA calculator = new ThroughputEMA(2);

        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(5050L));
    }

    @Test
    public void testCalculationThroughput() {
        ThroughputEMA calculator = new ThroughputEMA(3);

        // warm-up
        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));
        assertThat(calculator.calculateThroughput(100, 10), is(7272L));

        // Calculation of the average
        assertThat(calculator.calculateThroughput(100, 10), is(8636L));

        // The result value seeks to the upper limit but it will take a while until it reaches it.
        assertThat(calculator.calculateThroughput(100, 10), is(9318L));
        assertThat(calculator.calculateThroughput(100, 10), is(9659L));
        assertThat(calculator.calculateThroughput(100, 10), is(9829L));
        assertThat(calculator.calculateThroughput(100, 10), is(9914L));
        assertThat(calculator.calculateThroughput(100, 10), is(9957L));

        // Downgrade
        // The fast drop for the first value because the last value has the highest weight.
        assertThat(calculator.calculateThroughput(10, 10), is(5478L));
        assertThat(calculator.calculateThroughput(10, 10), is(3239L));
        assertThat(calculator.calculateThroughput(10, 10), is(2119L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeDataSize() {
        ThroughputEMA calculator = new ThroughputEMA(2);
        calculator.calculateThroughput(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTime() {
        ThroughputEMA calculator = new ThroughputEMA(2);
        calculator.calculateThroughput(1, -1);
    }

    @Test
    public void testPreviousValueWhenTimeIsZero() {
        ThroughputEMA calculator = new ThroughputEMA(2);

        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 0), is(3030L));
    }
}
