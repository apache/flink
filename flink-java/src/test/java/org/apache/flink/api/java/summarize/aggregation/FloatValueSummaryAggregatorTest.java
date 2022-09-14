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
import org.apache.flink.types.FloatValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Tests for {@link ValueSummaryAggregator.FloatValueSummaryAggregator}. */
class FloatValueSummaryAggregatorTest extends FloatSummaryAggregatorTest {

    /**
     * Helper method for summarizing a list of values.
     *
     * <p>This method breaks the rule of "testing only one thing" by aggregating and combining a
     * bunch of different ways.
     */
    @Override
    protected NumericColumnSummary<Float> summarize(Float... values) {

        FloatValue[] floatValues = new FloatValue[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                floatValues[i] = new FloatValue(values[i]);
            }
        }

        return new AggregateCombineHarness<
                FloatValue,
                NumericColumnSummary<Float>,
                ValueSummaryAggregator.FloatValueSummaryAggregator>() {

            @Override
            protected void compareResults(
                    NumericColumnSummary<Float> result1, NumericColumnSummary<Float> result2) {
                assertThat(result2.getMin()).isCloseTo(result1.getMin(), offset(0.0f));
                assertThat(result2.getMax()).isCloseTo(result1.getMax(), offset(0.0f));
                assertThat(result2.getMean()).isCloseTo(result1.getMean(), offset(1e-10d));
                assertThat(result2.getVariance()).isCloseTo(result1.getVariance(), offset(1e-9d));
                assertThat(result2.getStandardDeviation())
                        .isCloseTo(result1.getStandardDeviation(), offset(1e-10d));
            }
        }.summarize(floatValues);
    }
}
