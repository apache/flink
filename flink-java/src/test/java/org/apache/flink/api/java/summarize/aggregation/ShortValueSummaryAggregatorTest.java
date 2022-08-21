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
import org.apache.flink.types.ShortValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Tests for {@link ValueSummaryAggregator.ShortValueSummaryAggregator}. */
class ShortValueSummaryAggregatorTest extends ShortSummaryAggregatorTest {

    /** Helper method for summarizing a list of values. */
    protected NumericColumnSummary<Short> summarize(Integer... values) {

        ShortValue[] shortValues = new ShortValue[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                shortValues[i] = new ShortValue(values[i].shortValue());
            }
        }

        return new AggregateCombineHarness<
                ShortValue,
                NumericColumnSummary<Short>,
                ValueSummaryAggregator.ShortValueSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Short> result1, NumericColumnSummary<Short> result2) {

                assertThat(result2.getTotalCount()).isEqualTo(result1.getTotalCount());
                assertThat(result2.getNullCount()).isEqualTo(result1.getNullCount());
                assertThat(result2.getMissingCount()).isEqualTo(result1.getMissingCount());
                assertThat(result2.getNonMissingCount()).isEqualTo(result1.getNonMissingCount());
                assertThat(result2.getInfinityCount()).isEqualTo(result1.getInfinityCount());
                assertThat(result2.getNanCount()).isEqualTo(result1.getNanCount());

                assertThat(result2.containsNull()).isEqualTo(result1.containsNull());
                assertThat(result2.containsNonNull()).isEqualTo(result1.containsNonNull());

                assertThat(result2.getMin().shortValue()).isEqualTo(result1.getMin().shortValue());
                assertThat(result2.getMax().shortValue()).isEqualTo(result1.getMax().shortValue());
                assertThat(result2.getSum().shortValue()).isEqualTo(result1.getSum().shortValue());
                assertThat(result2.getMean().doubleValue())
                        .isCloseTo(result1.getMean().doubleValue(), offset(1e-12d));
                assertThat(result2.getVariance().doubleValue())
                        .isCloseTo(result1.getVariance().doubleValue(), offset(1e-9d));
                assertThat(result2.getStandardDeviation().doubleValue())
                        .isCloseTo(result1.getStandardDeviation().doubleValue(), offset(1e-12d));
            }
        }.summarize(shortValues);
    }
}
