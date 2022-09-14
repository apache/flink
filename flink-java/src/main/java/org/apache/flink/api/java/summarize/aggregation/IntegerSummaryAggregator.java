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

/** Aggregator that can handle Integer types. */
@Internal
public class IntegerSummaryAggregator extends NumericSummaryAggregator<Integer> {

    private static final long serialVersionUID = 1L;

    // Nested classes are only "public static" for Kryo serialization, otherwise they'd be private

    /** Aggregator for min operation. */
    public static class MinIntegerAggregator implements Aggregator<Integer, Integer> {

        private int min = Integer.MAX_VALUE;

        @Override
        public void aggregate(Integer value) {
            min = Math.min(min, value);
        }

        @Override
        public void combine(Aggregator<Integer, Integer> other) {
            min = Math.min(min, ((MinIntegerAggregator) other).min);
        }

        @Override
        public Integer result() {
            return min;
        }
    }

    /** Aggregator for max operation. */
    public static class MaxIntegerAggregator implements Aggregator<Integer, Integer> {

        private int max = Integer.MIN_VALUE;

        @Override
        public void aggregate(Integer value) {
            max = Math.max(max, value);
        }

        @Override
        public void combine(Aggregator<Integer, Integer> other) {
            max = Math.max(max, ((MaxIntegerAggregator) other).max);
        }

        @Override
        public Integer result() {
            return max;
        }
    }

    /** Aggregator for sum operation. */
    public static class SumIntegerAggregator implements Aggregator<Integer, Integer> {

        private int sum = 0;

        @Override
        public void aggregate(Integer value) {
            sum += value;
        }

        @Override
        public void combine(Aggregator<Integer, Integer> other) {
            sum += ((SumIntegerAggregator) other).sum;
        }

        @Override
        public Integer result() {
            return sum;
        }
    }

    @Override
    protected Aggregator<Integer, Integer> initMin() {
        return new MinIntegerAggregator();
    }

    @Override
    protected Aggregator<Integer, Integer> initMax() {
        return new MaxIntegerAggregator();
    }

    @Override
    protected Aggregator<Integer, Integer> initSum() {
        return new SumIntegerAggregator();
    }

    @Override
    protected boolean isNan(Integer number) {
        // NaN never applies here because only types like Float and Double have NaN
        return false;
    }

    @Override
    protected boolean isInfinite(Integer number) {
        // Infinity never applies here because only types like Float and Double have Infinity
        return false;
    }
}
