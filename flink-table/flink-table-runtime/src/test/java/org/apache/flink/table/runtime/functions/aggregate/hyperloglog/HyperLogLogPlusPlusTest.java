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

package org.apache.flink.table.runtime.functions.aggregate.hyperloglog;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.DEFAULT_SEED;
import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.hashInt;
import static org.apache.flink.table.runtime.functions.aggregate.hyperloglog.XXH64.hashLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The test of HyperLogLogPlusPlus is inspired from Apache Spark. */
public class HyperLogLogPlusPlusTest {

    @Test
    public void testInvalidRelativeSD() {
        assertThatThrownBy(() -> new HyperLogLogPlusPlus(0.4))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "HLL++ requires at least 4 bits for addressing. Use a lower error, at most 39%."));
    }

    @Test
    public void testInputAllNulls() {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(0.01);
        HllBuffer buffer = createHllBuffer(hll);
        long estimate = hll.query(buffer);
        assertThat(estimate).isEqualTo(0);
    }

    @Test
    public void testDeterministicCardinalityEstimation() {
        int repeats = 10;
        testCardinalityEstimates(
                new double[] {0.1, 0.05, 0.025, 0.01, 0.001},
                new int[] {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000},
                i -> i / repeats,
                i -> i / repeats);
    }

    @Test
    public void testMerge() {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(0.05);
        HllBuffer buffer1a = createHllBuffer(hll);
        HllBuffer buffer1b = createHllBuffer(hll);
        HllBuffer buffer2 = createHllBuffer(hll);

        // Create the
        // Add the lower half
        int i = 0;
        while (i < 500000) {
            hll.updateByHashcode(buffer1a, hashInt(i, DEFAULT_SEED));
            i += 1;
        }

        // Add the upper half
        i = 500000;
        while (i < 1000000) {
            hll.updateByHashcode(buffer1b, hashInt(i, DEFAULT_SEED));
            i += 1;
        }

        // Merge the lower and upper halves.
        hll.merge(buffer1a, buffer1b);

        // Create the other buffer in reverse
        i = 999999;
        while (i >= 0) {
            hll.updateByHashcode(buffer2, hashInt(i, DEFAULT_SEED));
            i -= 1;
        }

        assertThat(buffer2.array).isEqualTo(buffer1a.array);
    }

    @Test
    public void testRandomCardinalityEstimation() {
        Random srng = new Random(323981238L);
        Set<Integer> seen = new HashSet<>();
        Function<Integer, Integer> update =
                i -> {
                    int value = srng.nextInt();
                    seen.add(value);
                    return value;
                };
        Function<Integer, Integer> eval =
                n -> {
                    int cardinality = seen.size();
                    seen.clear();
                    return cardinality;
                };
        testCardinalityEstimates(
                new double[] {0.05, 0.01}, new int[] {100, 10000, 500000}, update, eval);
    }

    @Test
    public void testPositiveAndNegativeZero() {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(0.05);
        HllBuffer buffer = createHllBuffer(hll);
        hll.updateByHashcode(buffer, hashLong(Double.doubleToLongBits(0.0d), DEFAULT_SEED));
        hll.updateByHashcode(buffer, hashLong(Double.doubleToLongBits(-0.0d), DEFAULT_SEED));
        long estimate = hll.query(buffer);
        double error = Math.abs(estimate - 1.0d);
        // not handle in HyperLogLogPlusPlus but in ApproximateCountDistinct
        assertThat(error < hll.trueRsd() * 3.0d).isFalse();
    }

    @Test
    public void testNaN() {
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(0.05);
        HllBuffer buffer = createHllBuffer(hll);
        hll.updateByHashcode(buffer, hashLong(Double.doubleToLongBits(Double.NaN), DEFAULT_SEED));
        long estimate = hll.query(buffer);
        double error = Math.abs(estimate - 1.0d);
        assertThat(error < hll.trueRsd() * 3.0d).isTrue();
    }

    private void testCardinalityEstimates(
            double[] rsds,
            int[] ns,
            Function<Integer, Integer> updateFun,
            Function<Integer, Integer> evalFun) {
        for (double rsd : rsds) {
            for (int n : ns) {
                HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(rsd);
                HllBuffer buffer = createHllBuffer(hll);
                for (int i = 0; i < n; ++i) {
                    hll.updateByHashcode(buffer, hashInt(updateFun.apply(i), DEFAULT_SEED));
                }
                long estimate = hll.query(buffer);
                int cardinality = evalFun.apply(n);
                double error = Math.abs((estimate * 1.0 / cardinality) - 1.0d);
                assertThat(error < hll.trueRsd() * 3.0d).isTrue();
            }
        }
    }

    public HllBuffer createHllBuffer(HyperLogLogPlusPlus hll) {
        HllBuffer buffer = new HllBuffer();
        buffer.array = new long[hll.getNumWords()];
        int word = 0;
        while (word < hll.getNumWords()) {
            buffer.array[word] = 0;
            word++;
        }
        return buffer;
    }
}
