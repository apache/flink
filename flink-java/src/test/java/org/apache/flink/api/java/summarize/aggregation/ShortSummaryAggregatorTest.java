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

import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link ShortSummaryAggregator}. */
public class ShortSummaryAggregatorTest {

    @Test
    public void testIsNan() throws Exception {
        ShortSummaryAggregator ag = new ShortSummaryAggregator();
        // always false for Short
        Assertions.assertFalse(ag.isNan((short) -1));
        Assertions.assertFalse(ag.isNan((short) 0));
        Assertions.assertFalse(ag.isNan((short) 23));
        Assertions.assertFalse(ag.isNan(Short.MAX_VALUE));
        Assertions.assertFalse(ag.isNan(Short.MIN_VALUE));
        Assertions.assertFalse(ag.isNan(null));
    }

    @Test
    public void testIsInfinite() throws Exception {
        ShortSummaryAggregator ag = new ShortSummaryAggregator();
        // always false for Short
        Assertions.assertFalse(ag.isInfinite((short) -1));
        Assertions.assertFalse(ag.isInfinite((short) 0));
        Assertions.assertFalse(ag.isInfinite((short) 23));
        Assertions.assertFalse(ag.isInfinite(Short.MAX_VALUE));
        Assertions.assertFalse(ag.isInfinite(Short.MIN_VALUE));
        Assertions.assertFalse(ag.isInfinite(null));
    }

    @Test
    public void testMean() throws Exception {
        Assertions.assertEquals(50.0, summarize(0, 100).getMean(), 0.0);
        Assertions.assertEquals(33.333333, summarize(0, 0, 100).getMean(), 0.00001);
        Assertions.assertEquals(50.0, summarize(0, 0, 100, 100).getMean(), 0.0);
        Assertions.assertEquals(50.0, summarize(0, 100, null).getMean(), 0.0);
        Assertions.assertNull(summarize().getMean());
    }

    @Test
    public void testSum() throws Exception {
        Assertions.assertEquals(100, summarize(0, 100).getSum().shortValue());
        Assertions.assertEquals(15, summarize(1, 2, 3, 4, 5).getSum().shortValue());
        Assertions.assertEquals(0, summarize(-100, 0, 100, null).getSum().shortValue());
        Assertions.assertEquals(90, summarize(-10, 100, null).getSum().shortValue());
        Assertions.assertNull(summarize().getSum());
    }

    @Test
    public void testMax() throws Exception {
        Assertions.assertEquals(1001, summarize(-1000, 0, 1, 50, 999, 1001).getMax().shortValue());
        Assertions.assertEquals(
                0, summarize((int) Short.MIN_VALUE, -1000, 0).getMax().shortValue());
        Assertions.assertEquals(
                11, summarize(1, 8, 7, 6, 9, 10, 2, 3, 5, 0, 11, -2, 3).getMax().shortValue());
        Assertions.assertEquals(
                11,
                summarize(1, 8, 7, 6, 9, null, 10, 2, 3, 5, null, 0, 11, -2, 3)
                        .getMax()
                        .shortValue());
        Assertions.assertNull(summarize().getMax());
    }

    @Test
    public void testMin() throws Exception {
        Assertions.assertEquals(-1000, summarize(-1000, 0, 1, 50, 999, 1001).getMin().shortValue());
        Assertions.assertEquals(
                Short.MIN_VALUE, summarize((int) Short.MIN_VALUE, -1000, 0).getMin().shortValue());
        Assertions.assertEquals(
                -2, summarize(1, 8, 7, 6, 9, 10, 2, 3, 5, 0, 11, -2, 3).getMin().shortValue());
        Assertions.assertEquals(
                -2,
                summarize(1, 8, 7, 6, 9, null, 10, 2, 3, 5, null, 0, 11, -2, 3)
                        .getMin()
                        .shortValue());
        Assertions.assertNull(summarize().getMin());
    }

    /** Helper method for summarizing a list of values. */
    protected NumericColumnSummary<Short> summarize(Integer... values) {

        // cast everything to short here
        Short[] shortValues = new Short[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                shortValues[i] = values[i].shortValue();
            }
        }

        return new AggregateCombineHarness<
                Short, NumericColumnSummary<Short>, ShortSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Short> result1, NumericColumnSummary<Short> result2) {

                Assertions.assertEquals(result1.getTotalCount(), result2.getTotalCount());
                Assertions.assertEquals(result1.getNullCount(), result2.getNullCount());
                Assertions.assertEquals(result1.getMissingCount(), result2.getMissingCount());
                Assertions.assertEquals(result1.getNonMissingCount(), result2.getNonMissingCount());
                Assertions.assertEquals(result1.getInfinityCount(), result2.getInfinityCount());
                Assertions.assertEquals(result1.getNanCount(), result2.getNanCount());

                Assertions.assertEquals(result1.containsNull(), result2.containsNull());
                Assertions.assertEquals(result1.containsNonNull(), result2.containsNonNull());

                Assertions.assertEquals(
                        result1.getMin().shortValue(), result2.getMin().shortValue());
                Assertions.assertEquals(
                        result1.getMax().shortValue(), result2.getMax().shortValue());
                Assertions.assertEquals(
                        result1.getSum().shortValue(), result2.getSum().shortValue());
                Assertions.assertEquals(
                        result1.getMean().doubleValue(), result2.getMean().doubleValue(), 1e-12d);
                Assertions.assertEquals(
                        result1.getVariance().doubleValue(),
                        result2.getVariance().doubleValue(),
                        1e-9d);
                Assertions.assertEquals(
                        result1.getStandardDeviation().doubleValue(),
                        result2.getStandardDeviation().doubleValue(),
                        1e-12d);
            }
        }.summarize(shortValues);
    }
}
