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

import org.apache.flink.api.java.summarize.BooleanColumnSummary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link BooleanSummaryAggregator}. */
public class BooleanSummaryAggregatorTest {

    @Test
    public void testMixedGroup() {
        BooleanColumnSummary summary =
                summarize(true, false, null, true, true, true, false, null, true, false, true);
        Assertions.assertEquals(11, summary.getTotalCount());
        Assertions.assertEquals(2, summary.getNullCount());
        Assertions.assertEquals(9, summary.getNonNullCount());
        Assertions.assertEquals(6, summary.getTrueCount());
        Assertions.assertEquals(3, summary.getFalseCount());
    }

    @Test
    public void testAllNullBooleans() {
        BooleanColumnSummary summary = summarize(null, null, null, null);
        Assertions.assertEquals(4, summary.getTotalCount());
        Assertions.assertEquals(4, summary.getNullCount());
        Assertions.assertEquals(0, summary.getNonNullCount());
        Assertions.assertEquals(0, summary.getTrueCount());
        Assertions.assertEquals(0, summary.getFalseCount());
    }

    @Test
    public void testAllTrue() {
        BooleanColumnSummary summary = summarize(true, true, true, true, true, true);
        Assertions.assertEquals(6, summary.getTotalCount());
        Assertions.assertEquals(0, summary.getNullCount());
        Assertions.assertEquals(6, summary.getNonNullCount());
        Assertions.assertEquals(6, summary.getTrueCount());
        Assertions.assertEquals(0, summary.getFalseCount());
    }

    @Test
    public void testAllFalse() {
        BooleanColumnSummary summary = summarize(false, false, false);
        Assertions.assertEquals(3, summary.getTotalCount());
        Assertions.assertEquals(0, summary.getNullCount());
        Assertions.assertEquals(3, summary.getNonNullCount());
        Assertions.assertEquals(0, summary.getTrueCount());
        Assertions.assertEquals(3, summary.getFalseCount());
    }

    /**
     * Helper method for summarizing a list of values.
     *
     * <p>This method breaks the rule of "testing only one thing" by aggregating and combining a
     * bunch of different ways.
     */
    protected BooleanColumnSummary summarize(Boolean... values) {
        return new AggregateCombineHarness<
                Boolean, BooleanColumnSummary, BooleanSummaryAggregator>() {
            @Override
            protected void compareResults(
                    BooleanColumnSummary result1, BooleanColumnSummary result2) {
                Assertions.assertEquals(result1.getNullCount(), result2.getNullCount());
                Assertions.assertEquals(result1.getNonNullCount(), result2.getNonNullCount());
                Assertions.assertEquals(result1.getTrueCount(), result2.getTrueCount());
                Assertions.assertEquals(result1.getFalseCount(), result2.getFalseCount());
            }
        }.summarize(values);
    }
}
