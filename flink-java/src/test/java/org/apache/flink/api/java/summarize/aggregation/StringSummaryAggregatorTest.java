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

import org.apache.flink.api.java.summarize.StringColumnSummary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;

/** Tests for {@link StringSummaryAggregator}. */
public class StringSummaryAggregatorTest {

    @Test
    public void testMixedGroup() {
        StringColumnSummary summary =
                summarize("abc", "", null, "  ", "defghi", "foo", null, null, "", " ");
        Assertions.assertEquals(10, summary.getTotalCount());
        Assertions.assertEquals(3, summary.getNullCount());
        Assertions.assertEquals(7, summary.getNonNullCount());
        Assertions.assertEquals(2, summary.getEmptyCount());
        Assertions.assertEquals(0, summary.getMinLength().intValue());
        Assertions.assertEquals(6, summary.getMaxLength().intValue());
        Assertions.assertEquals(2.142857, summary.getMeanLength().doubleValue(), 0.001);
    }

    @Test
    public void testAllNullStrings() {
        StringColumnSummary summary = summarize(null, null, null, null);
        Assertions.assertEquals(4, summary.getTotalCount());
        Assertions.assertEquals(4, summary.getNullCount());
        Assertions.assertEquals(0, summary.getNonNullCount());
        Assertions.assertEquals(0, summary.getEmptyCount());
        Assertions.assertNull(summary.getMinLength());
        Assertions.assertNull(summary.getMaxLength());
        Assertions.assertNull(summary.getMeanLength());
    }

    @Test
    public void testAllWithValues() {
        StringColumnSummary summary = summarize("cat", "hat", "dog", "frog");
        Assertions.assertEquals(4, summary.getTotalCount());
        Assertions.assertEquals(0, summary.getNullCount());
        Assertions.assertEquals(4, summary.getNonNullCount());
        Assertions.assertEquals(0, summary.getEmptyCount());
        Assertions.assertEquals(3, summary.getMinLength().intValue());
        Assertions.assertEquals(4, summary.getMaxLength().intValue());
        Assertions.assertEquals(3.25, summary.getMeanLength().doubleValue(), 0.0);
    }

    /**
     * Helper method for summarizing a list of values.
     *
     * <p>This method breaks the rule of "testing only one thing" by aggregating and combining a
     * bunch of different ways.
     */
    protected StringColumnSummary summarize(String... values) {

        return new AggregateCombineHarness<String, StringColumnSummary, StringSummaryAggregator>() {

            @Override
            protected void compareResults(
                    StringColumnSummary result1, StringColumnSummary result2) {
                Assertions.assertEquals(result1.getEmptyCount(), result2.getEmptyCount());
                Assertions.assertEquals(result1.getMaxLength(), result2.getMaxLength());
                Assertions.assertEquals(result1.getMinLength(), result2.getMinLength());
                if (result1.getMeanLength() == null) {
                    Assertions.assertEquals(result1.getMeanLength(), result2.getMeanLength());
                } else {
                    Assertions.assertEquals(
                            result1.getMeanLength().doubleValue(),
                            result2.getMeanLength().doubleValue(),
                            1e-5d);
                }
                Assertions.assertEquals(result1.getNullCount(), result2.getNullCount());
                Assertions.assertEquals(result1.getNonNullCount(), result2.getNonNullCount());
            }
        }.summarize(values);
    }
}
