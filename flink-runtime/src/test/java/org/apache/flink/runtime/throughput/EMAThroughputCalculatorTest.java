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

/** Test for {@link EMAThroughputCalculator}. */
public class EMAThroughputCalculatorTest extends TestLogger {
    @Test
    public void testWarmUpThroughputCalculation() {
        EMAThroughputCalculator calculator = new EMAThroughputCalculator(5);

        // Calculation throughput bytes/second.
        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));
        assertThat(calculator.calculateThroughput(100, 9), is(6733L));
    }

    @Test
    public void testWarmUpIsFinished() {
        EMAThroughputCalculator calculator = new EMAThroughputCalculator(2);

        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));
    }

    @Test
    public void testCalculationThroughput() {
        EMAThroughputCalculator calculator = new EMAThroughputCalculator(3);

        // warm-up
        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 33), is(4545L));
        assertThat(calculator.calculateThroughput(100, 10), is(6363L));

        // Calculation of the average
        assertThat(calculator.calculateThroughput(100, 10), is(8181L));

        // The result value seeks to the upper limit but it will take a while until it reaches it.
        assertThat(calculator.calculateThroughput(100, 10), is(9090L));
        assertThat(calculator.calculateThroughput(100, 10), is(9545L));
        assertThat(calculator.calculateThroughput(100, 10), is(9772L));
        assertThat(calculator.calculateThroughput(100, 10), is(9886L));
        assertThat(calculator.calculateThroughput(100, 10), is(9943L));

        // Downgrade
        // The fast drop for the first value because the last value has the highest weight.
        assertThat(calculator.calculateThroughput(10, 10), is(5471L));
        assertThat(calculator.calculateThroughput(10, 10), is(3235L));
        assertThat(calculator.calculateThroughput(10, 10), is(2117L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeDataSize() {
        EMAThroughputCalculator calculator = new EMAThroughputCalculator(2);
        calculator.calculateThroughput(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTime() {
        EMAThroughputCalculator calculator = new EMAThroughputCalculator(2);
        calculator.calculateThroughput(1, -1);
    }

    @Test
    public void testPreviousValueWhenTimeIsZero() {
        EMAThroughputCalculator calculator = new EMAThroughputCalculator(2);

        assertThat(calculator.calculateThroughput(100, 33), is(3030L));
        assertThat(calculator.calculateThroughput(200, 0), is(3030L));
    }
}
