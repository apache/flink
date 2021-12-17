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
import org.apache.flink.types.BooleanValue;

import org.junit.Assert;

/** Tests for {@link ValueSummaryAggregator.BooleanValueSummaryAggregator}. */
public class BooleanValueSummaryAggregatorTest extends BooleanSummaryAggregatorTest {

    /**
     * Helper method for summarizing a list of values.
     *
     * <p>This method breaks the rule of "testing only one thing" by aggregating and combining a
     * bunch of different ways.
     */
    protected BooleanColumnSummary summarize(Boolean... values) {

        BooleanValue[] booleanValues = new BooleanValue[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                booleanValues[i] = new BooleanValue(values[i]);
            }
        }

        return new AggregateCombineHarness<
                BooleanValue,
                BooleanColumnSummary,
                ValueSummaryAggregator.BooleanValueSummaryAggregator>() {
            @Override
            protected void compareResults(
                    BooleanColumnSummary result1, BooleanColumnSummary result2) {
                Assert.assertEquals(result1.getNullCount(), result2.getNullCount());
                Assert.assertEquals(result1.getNonNullCount(), result2.getNonNullCount());
                Assert.assertEquals(result1.getTrueCount(), result2.getTrueCount());
                Assert.assertEquals(result1.getFalseCount(), result2.getFalseCount());
            }
        }.summarize(booleanValues);
    }
}
