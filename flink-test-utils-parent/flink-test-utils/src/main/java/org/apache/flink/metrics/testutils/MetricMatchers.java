/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsInstanceOf;

import java.util.function.Function;

/**
 * Provides hamcrest Matchers for metric assertions.
 *
 * @see org.apache.flink.metrics.reporter.TestReporter
 */
public enum MetricMatchers {
    ;

    public static <T extends Metric, V> Matcher<T> isGauge(Matcher<V> valueMatcher) {
        return new MetricMatcher<>(Gauge.class, Gauge<V>::getValue, valueMatcher);
    }

    public static <T extends Metric> Matcher<T> isCounter(Matcher<Long> valueMatcher) {
        return new MetricMatcher<>(Counter.class, Counter::getCount, valueMatcher);
    }

    private static class MetricMatcher<M extends Metric, V, T extends Metric>
            extends DiagnosingMatcher<T> {
        private final Matcher<?> typeMatcher;
        private final Function<M, V> valueExtractor;
        private final Matcher<V> valueMatcher;
        private final Class<M> expectedClass;

        MetricMatcher(
                Class<M> expectedClass, Function<M, V> valueExtractor, Matcher<V> valueMatcher) {
            this.expectedClass = expectedClass;
            this.typeMatcher = new IsInstanceOf(expectedClass);
            this.valueExtractor = valueExtractor;
            this.valueMatcher = valueMatcher;
        }

        @Override
        protected boolean matches(Object item, Description mismatchDescription) {
            if (!typeMatcher.matches(item)) {
                typeMatcher.describeMismatch(item, mismatchDescription);
                return false;
            }
            V value = valueExtractor.apply(expectedClass.cast(item));
            if (!valueMatcher.matches(value)) {
                mismatchDescription.appendText(expectedClass.getSimpleName()).appendText(" with ");
                valueMatcher.describeMismatch(value, mismatchDescription);
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText(expectedClass.getSimpleName())
                    .appendText(" with ")
                    .appendDescriptionOf(valueMatcher);
        }
    }
}
