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

package org.apache.flink.metrics.testutils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

/** This class provides access to AssertJ assertions for {@link Metric}s. */
public class MetricAssertions {

    public static CounterAssert assertThatCounter(Metric actual) {
        assertThat(actual).isInstanceOf(Counter.class);
        return new CounterAssert((Counter) actual);
    }

    public static <T> GaugeAssert<T> assertThatGauge(Metric actual) {
        assertThat(actual).isInstanceOf(Gauge.class);
        return new GaugeAssert<>((Gauge<T>) actual);
    }

    /** This class provides AssertJ assertions for {@link Gauge}s. */
    public static class GaugeAssert<T> extends AbstractAssert<GaugeAssert<T>, Gauge<T>> {

        GaugeAssert(Gauge<T> actual) {
            super(actual, GaugeAssert.class);
        }

        /**
         * Verifies that the gauges value is equal to the expected one.
         *
         * @param expected the given value to compare the actual value to.
         * @return this assertion object
         */
        @Override
        public GaugeAssert<T> isEqualTo(Object expected) {
            assertThat(actual.getValue()).isEqualTo(expected);
            return this;
        }

        /**
         * Verifies that the gauges value is close to the expected value within a certain deviation.
         *
         * @param value the expected value
         * @param epsilon the maximum deviation from the expected value
         * @return this assertion object
         */
        public GaugeAssert<T> isCloseTo(long value, long epsilon) {
            assertThat((Long) actual.getValue())
                    .isGreaterThan(value - epsilon)
                    .isLessThan(value + epsilon);
            return this;
        }
    }

    /** This class provides AssertJ assertions for {@link Counter}s. */
    public static class CounterAssert extends AbstractAssert<CounterAssert, Counter> {

        CounterAssert(Counter actual) {
            super(actual, CounterAssert.class);
        }

        @Override
        public CounterAssert isEqualTo(Object expected) {
            assertThat(actual.getCount()).isEqualTo(expected);
            return this;
        }
    }
}
