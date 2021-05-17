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
import org.apache.flink.types.IntValue;

import org.junit.Assert;

/** Tests for {@link ValueSummaryAggregator.IntegerValueSummaryAggregator}. */
public class IntegerValueSummaryAggregatorTest extends IntegerSummaryAggregatorTest {

    @Override
    protected NumericColumnSummary<Integer> summarize(Integer... values) {

        IntValue[] intValues = new IntValue[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                intValues[i] = new IntValue(values[i]);
            }
        }

        return new AggregateCombineHarness<
                IntValue,
                NumericColumnSummary<Integer>,
                ValueSummaryAggregator.IntegerValueSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Integer> result1, NumericColumnSummary<Integer> result2) {

                Assert.assertEquals(result1.getTotalCount(), result2.getTotalCount());
                Assert.assertEquals(result1.getNullCount(), result2.getNullCount());
                Assert.assertEquals(result1.getMissingCount(), result2.getMissingCount());
                Assert.assertEquals(result1.getNonMissingCount(), result2.getNonMissingCount());
                Assert.assertEquals(result1.getInfinityCount(), result2.getInfinityCount());
                Assert.assertEquals(result1.getNanCount(), result2.getNanCount());

                Assert.assertEquals(result1.containsNull(), result2.containsNull());
                Assert.assertEquals(result1.containsNonNull(), result2.containsNonNull());

                Assert.assertEquals(result1.getMin().intValue(), result2.getMin().intValue());
                Assert.assertEquals(result1.getMax().intValue(), result2.getMax().intValue());
                Assert.assertEquals(result1.getSum().intValue(), result2.getSum().intValue());
                Assert.assertEquals(
                        result1.getMean().doubleValue(), result2.getMean().doubleValue(), 1e-12d);
                Assert.assertEquals(
                        result1.getVariance().doubleValue(),
                        result2.getVariance().doubleValue(),
                        1e-9d);
                Assert.assertEquals(
                        result1.getStandardDeviation().doubleValue(),
                        result2.getStandardDeviation().doubleValue(),
                        1e-12d);
            }
        }.summarize(intValues);
    }
}
