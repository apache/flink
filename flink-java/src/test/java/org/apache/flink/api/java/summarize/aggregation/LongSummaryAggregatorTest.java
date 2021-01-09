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
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;

/** Tests for {@link LongSummaryAggregator}. */
public class LongSummaryAggregatorTest {

    @Test
    public void testIsNan() throws Exception {
        LongSummaryAggregator ag = new LongSummaryAggregator();
        // always false for Long
        Assertions.assertFalse(ag.isNan(-1L));
        Assertions.assertFalse(ag.isNan(0L));
        Assertions.assertFalse(ag.isNan(23L));
        Assertions.assertFalse(ag.isNan(Long.MAX_VALUE));
        Assertions.assertFalse(ag.isNan(Long.MIN_VALUE));
        Assertions.assertFalse(ag.isNan(null));
    }

    @Test
    public void testIsInfinite() throws Exception {
        LongSummaryAggregator ag = new LongSummaryAggregator();
        // always false for Long
        Assertions.assertFalse(ag.isInfinite(-1L));
        Assertions.assertFalse(ag.isInfinite(0L));
        Assertions.assertFalse(ag.isInfinite(23L));
        Assertions.assertFalse(ag.isInfinite(Long.MAX_VALUE));
        Assertions.assertFalse(ag.isInfinite(Long.MIN_VALUE));
        Assertions.assertFalse(ag.isInfinite(null));
    }

    @Test
    public void testMean() throws Exception {
        Assertions.assertEquals(50.0, summarize(0L, 100L).getMean(), 0.0);
        Assertions.assertEquals(33.333333, summarize(0L, 0L, 100L).getMean(), 0.00001);
        Assertions.assertEquals(50.0, summarize(0L, 0L, 100L, 100L).getMean(), 0.0);
        Assertions.assertEquals(50.0, summarize(0L, 100L, null).getMean(), 0.0);
        Assertions.assertNull(summarize().getMean());
    }

    @Test
    public void testSum() throws Exception {
        Assertions.assertEquals(100L, summarize(0L, 100L).getSum().longValue());
        Assertions.assertEquals(15L, summarize(1L, 2L, 3L, 4L, 5L).getSum().longValue());
        Assertions.assertEquals(0L, summarize(-100L, 0L, 100L, null).getSum().longValue());
        Assertions.assertEquals(90L, summarize(-10L, 100L, null).getSum().longValue());
        Assertions.assertNull(summarize().getSum());
    }

    @Test
    public void testMax() throws Exception {
        Assertions.assertEquals(
                1001L, summarize(-1000L, 0L, 1L, 50L, 999L, 1001L).getMax().longValue());
        Assertions.assertEquals(
                11L,
                summarize(1L, 8L, 7L, 6L, 9L, 10L, 2L, 3L, 5L, 0L, 11L, -2L, 3L)
                        .getMax()
                        .longValue());
        Assertions.assertEquals(
                11L,
                summarize(1L, 8L, 7L, 6L, 9L, null, 10L, 2L, 3L, 5L, null, 0L, 11L, -2L, 3L)
                        .getMax()
                        .longValue());
        Assertions.assertNull(summarize().getMax());
    }

    @Test
    public void testMin() throws Exception {
        Assertions.assertEquals(
                -1000L, summarize(-1000L, 0L, 1L, 50L, 999L, 1001L).getMin().longValue());
        Assertions.assertEquals(
                -2L,
                summarize(1L, 8L, 7L, 6L, 9L, 10L, 2L, 3L, 5L, 0L, 11L, -2L, 3L)
                        .getMin()
                        .longValue());
        Assertions.assertEquals(
                -2L,
                summarize(1L, 8L, 7L, 6L, 9L, null, 10L, 2L, 3L, 5L, null, 0L, 11L, -2L, 3L)
                        .getMin()
                        .longValue());
        Assertions.assertNull(summarize().getMin());
    }

    /** Helper method for summarizing a list of values. */
    protected NumericColumnSummary<Long> summarize(Long... values) {
        return new AggregateCombineHarness<
                Long, NumericColumnSummary<Long>, LongSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Long> result1, NumericColumnSummary<Long> result2) {

                Assertions.assertEquals(result1.getTotalCount(), result2.getTotalCount());
                Assertions.assertEquals(result1.getNullCount(), result2.getNullCount());
                Assertions.assertEquals(result1.getMissingCount(), result2.getMissingCount());
                Assertions.assertEquals(result1.getNonMissingCount(), result2.getNonMissingCount());
                Assertions.assertEquals(result1.getInfinityCount(), result2.getInfinityCount());
                Assertions.assertEquals(result1.getNanCount(), result2.getNanCount());

                Assertions.assertEquals(result1.containsNull(), result2.containsNull());
                Assertions.assertEquals(result1.containsNonNull(), result2.containsNonNull());

                Assertions.assertEquals(result1.getMin().longValue(), result2.getMin().longValue());
                Assertions.assertEquals(result1.getMax().longValue(), result2.getMax().longValue());
                Assertions.assertEquals(result1.getSum().longValue(), result2.getSum().longValue());
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
        }.summarize(values);
    }
}
