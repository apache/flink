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

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link CompensatedSum}. */
public class CompensatedSumTest {

    /**
     * When adding a series of numbers the order of the numbers should not impact the results.
     *
     * <p>This test shows that a naive summation comes up with a different result than Kahan
     * Summation when you start with either a smaller or larger number in some cases and helps prove
     * our Kahan Summation is working.
     */
    @Test
    public void testAdd1() throws Exception {
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
        Assert.assertEquals(1000.011, compensatedResult1.value(), 0.0);
        Assert.assertEquals(1000.011, compensatedResult2.value(), 0.0);

        // naive addition gave a small floating point error
        Assert.assertEquals(1000.011, naiveResult1, 0.0);
        Assert.assertEquals(1000.0109999999997, naiveResult2, 0.0);

        Assert.assertEquals(compensatedResult1.value(), compensatedResult2.value(), 0.0);
        Assert.assertEquals(naiveResult1, naiveResult2, 0.0001);
        Assert.assertNotEquals(naiveResult1, naiveResult2, 0.0);
    }

    @Test
    public void testDelta() throws Exception {
        CompensatedSum compensatedResult1 = new CompensatedSum(0.001, 0.0);
        for (int i = 0; i < 10; i++) {
            compensatedResult1 = compensatedResult1.add(0.001);
        }
        Assert.assertEquals(0.011, compensatedResult1.value(), 0.0);
        Assert.assertEquals(new Double("8.673617379884035E-19"), compensatedResult1.delta(), 0.0);
    }
}
