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

package org.apache.flink.api.java.summarize.aggregation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Tests for {@link CompensatedSum}. */
class CompensatedSumTest {

    /**
     * When adding a series of numbers the order of the numbers should not impact the results.
     *
     * <p>This test shows that a naive summation comes up with a different result than Kahan
     * Summation when you start with either a smaller or larger number in some cases and helps prove
     * our Kahan Summation is working.
     */
    @Test
    void testAdd1() {
        final CompensatedSum smallSum = new CompensatedSum(0.001, 0.0);
        final CompensatedSum largeSum = new CompensatedSum(1000, 0.0);

        CompensatedSum compensatedResult1 = smallSum;
        CompensatedSum compensatedResult2 = largeSum;
        double naiveResult1 = smallSum.value();
        double naiveResult2 = largeSum.value();

        for (int i = 0; i < 10; i++) {
            compensatedResult1 = compensatedResult1.add(smallSum);
            compensatedResult2 = compensatedResult2.add(smallSum);
            naiveResult1 += smallSum.value();
            naiveResult2 += smallSum.value();
        }

        compensatedResult1 = compensatedResult1.add(largeSum);
        compensatedResult2 = compensatedResult2.add(smallSum);
        naiveResult1 += largeSum.value();
        naiveResult2 += smallSum.value();

        // Kahan summation gave the same result no matter what order we added
        assertThat(compensatedResult1.value()).isCloseTo(1000.011, offset(0.0));
        assertThat(compensatedResult2.value()).isCloseTo(1000.011, offset(0.0));

        // naive addition gave a small floating point error
        assertThat(naiveResult1).isCloseTo(1000.011, offset(0.0));
        assertThat(naiveResult2).isCloseTo(1000.0109999999997, offset(0.0));

        assertThat(compensatedResult2.value()).isCloseTo(compensatedResult1.value(), offset(0.0));
        assertThat(naiveResult2)
                .isCloseTo(naiveResult1, offset(0.0001))
                .isNotCloseTo(naiveResult1, offset(0.0));
    }

    @Test
    void testDelta() {
        CompensatedSum compensatedResult1 = new CompensatedSum(0.001, 0.0);
        for (int i = 0; i < 10; i++) {
            compensatedResult1 = compensatedResult1.add(0.001);
        }
        assertThat(compensatedResult1.value()).isCloseTo(0.011, offset(0.0));
        assertThat(compensatedResult1.delta())
                .isCloseTo(new Double("8.673617379884035E-19"), offset(0.0));
    }
}
