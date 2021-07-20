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

/** Test for {@link SMAThroughputCalculator}. */
public class SMAThroughputCalculatorTest extends TestLogger {

    @Test
    public void testWarmUpThroughputCalculation() {
        SMAThroughputCalculator calculator = new SMAThroughputCalculator(5);

        // Calculation throughput bytes/second.
        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));
        assertThat(calculator.calculateThroughput(100, 9), is(6733L));

        assertThat(calculator.isWarmedUp(), is(false));
    }

    @Test
    public void testWarmUpIsFinished() {
        SMAThroughputCalculator calculator = new SMAThroughputCalculator(2);

        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));

        assertThat(calculator.isWarmedUp(), is(true));
    }

    @Test
    public void testCalculationThroughput() {
        SMAThroughputCalculator calculator = new SMAThroughputCalculator(3);

        // warm-up
        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));
        assertThat(calculator.calculateThroughput(100, 10), is(6363L));
        assertThat(calculator.isWarmedUp(), is(true));

        // Calculation of the average
        assertThat(calculator.calculateThroughput(100, 10), is(8686L));
        // After the numberOfPreviousValues result is stabilized.
        assertThat(calculator.calculateThroughput(100, 10), is(10000L));
        assertThat(calculator.calculateThroughput(100, 10), is(10000L));

        // Downgrade
        assertThat(calculator.calculateThroughput(10, 10), is(7000L));
        assertThat(calculator.calculateThroughput(10, 10), is(4000L));
        assertThat(calculator.calculateThroughput(10, 10), is(1000L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeDataSize() {
        SMAThroughputCalculator calculator = new SMAThroughputCalculator(2);
        calculator.calculateThroughput(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTime() {
        SMAThroughputCalculator calculator = new SMAThroughputCalculator(2);
        calculator.calculateThroughput(1, -1);
    }

    @Test
    public void testPreviousValueWhenTimeIsZero() {
        SMAThroughputCalculator calculator = new SMAThroughputCalculator(2);

        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 0), is(3030L));
    }
}
