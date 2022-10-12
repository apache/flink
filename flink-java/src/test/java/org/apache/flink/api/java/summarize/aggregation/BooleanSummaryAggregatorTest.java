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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BooleanSummaryAggregator}. */
class BooleanSummaryAggregatorTest {

    @Test
    void testMixedGroup() {
        BooleanColumnSummary summary =
                summarize(true, false, null, true, true, true, false, null, true, false, true);
        assertThat(summary.getTotalCount()).isEqualTo(11);
        assertThat(summary.getNullCount()).isEqualTo(2);
        assertThat(summary.getNonNullCount()).isEqualTo(9);
        assertThat(summary.getTrueCount()).isEqualTo(6);
        assertThat(summary.getFalseCount()).isEqualTo(3);
    }

    @Test
    void testAllNullBooleans() {
        BooleanColumnSummary summary = summarize(null, null, null, null);
        assertThat(summary.getTotalCount()).isEqualTo(4);
        assertThat(summary.getNullCount()).isEqualTo(4);
        assertThat(summary.getNonNullCount()).isZero();
        assertThat(summary.getTrueCount()).isZero();
        assertThat(summary.getFalseCount()).isZero();
    }

    @Test
    void testAllTrue() {
        BooleanColumnSummary summary = summarize(true, true, true, true, true, true);
        assertThat(summary.getTotalCount()).isEqualTo(6);
        assertThat(summary.getNullCount()).isZero();
        assertThat(summary.getNonNullCount()).isEqualTo(6);
        assertThat(summary.getTrueCount()).isEqualTo(6);
        assertThat(summary.getFalseCount()).isZero();
    }

    @Test
    void testAllFalse() {
        BooleanColumnSummary summary = summarize(false, false, false);
        assertThat(summary.getTotalCount()).isEqualTo(3);
        assertThat(summary.getNullCount()).isZero();
        assertThat(summary.getNonNullCount()).isEqualTo(3);
        assertThat(summary.getTrueCount()).isZero();
        assertThat(summary.getFalseCount()).isEqualTo(3);
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
                assertThat(result2.getNullCount()).isEqualTo(result1.getNullCount());
                assertThat(result2.getNonNullCount()).isEqualTo(result1.getNonNullCount());
                assertThat(result2.getTrueCount()).isEqualTo(result1.getTrueCount());
                assertThat(result2.getFalseCount()).isEqualTo(result1.getFalseCount());
            }
        }.summarize(values);
    }
}
