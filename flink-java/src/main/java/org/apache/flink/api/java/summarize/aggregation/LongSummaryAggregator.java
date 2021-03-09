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

import org.apache.flink.annotation.Internal;

/** Aggregator that can handle Long types. */
@Internal
public class LongSummaryAggregator extends NumericSummaryAggregator<Long> {

    private static final long serialVersionUID = 1L;

    // Nested classes are only "public static" for Kryo serialization, otherwise they'd be private

    /** Aggregator for min operation. */
    public static class MinLongAggregator implements Aggregator<Long, Long> {

        private long min = Long.MAX_VALUE;

        @Override
        public void aggregate(Long value) {
            min = Math.min(min, value);
        }

        @Override
        public void combine(Aggregator<Long, Long> other) {
            min = Math.min(min, ((MinLongAggregator) other).min);
        }

        @Override
        public Long result() {
            return min;
        }
    }

    /** Aggregator for max operation. */
    public static class MaxLongAggregator implements Aggregator<Long, Long> {

        private long max = Long.MIN_VALUE;

        @Override
        public void aggregate(Long value) {
            max = Math.max(max, value);
        }

        @Override
        public void combine(Aggregator<Long, Long> other) {
            max = Math.max(max, ((MaxLongAggregator) other).max);
        }

        @Override
        public Long result() {
            return max;
        }
    }

    /** Aggregator for sum operation. */
    private static class SumLongAggregator implements Aggregator<Long, Long> {

        private long sum = 0;

        @Override
        public void aggregate(Long value) {
            sum += value;
        }

        @Override
        public void combine(Aggregator<Long, Long> other) {
            sum += ((SumLongAggregator) other).sum;
        }

        @Override
        public Long result() {
            return sum;
        }
    }

    @Override
    protected Aggregator<Long, Long> initMin() {
        return new MinLongAggregator();
    }

    @Override
    protected Aggregator<Long, Long> initMax() {
        return new MaxLongAggregator();
    }

    @Override
    protected Aggregator<Long, Long> initSum() {
        return new SumLongAggregator();
    }

    @Override
    protected boolean isNan(Long number) {
        // NaN never applies here because only types like Float and Double have NaN
        return false;
    }

    @Override
    protected boolean isInfinite(Long number) {
        // Infinity never applies here because only types like Float and Double have Infinity
        return false;
    }
}
