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

package org.apache.flink.runtime.metrics.filter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricType;
import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.metrics.util.TestMeter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.CONCURRENT)
class DefaultMetricFilterTest {

    private static final Counter COUNTER = new TestCounter();
    private static final Meter METER = new TestMeter();
    private static final Gauge<Integer> GAUGE = () -> 4;

    @Test
    void testConvertToPatternWithoutWildcards() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecordsIn");
        assertThat(pattern.toString()).isEqualTo("(numRecordsIn)");
        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesOut").matches()).isFalse();
    }

    @Test
    void testConvertToPatternSingle() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecords*");
        assertThat(pattern.toString()).isEqualTo("(numRecords.*)");
        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesOut").matches()).isFalse();
    }

    @Test
    void testConvertToPatternMultiple() {
        final Pattern pattern = DefaultMetricFilter.convertToPattern("numRecords*,numBytes*");
        assertThat(pattern.toString()).isEqualTo("(numRecords.*|numBytes.*)");
        assertThat(pattern.matcher("numRecordsIn").matches()).isTrue();
        assertThat(pattern.matcher("numBytesOut").matches()).isTrue();
        assertThat(pattern.matcher("numBytes").matches()).isTrue();
        assertThat(pattern.matcher("hello").matches()).isFalse();
    }

    @Test
    void testParseMetricTypesSingle() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("meter");
        assertThat(types).containsExactly(MetricType.METER);
    }

    @Test
    void testParseMetricTypesMultiple() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("meter,counter");
        assertThat(types).containsExactlyInAnyOrder(MetricType.METER, MetricType.COUNTER);
    }

    @Test
    void testParseMetricTypesCaseIgnored() {
        final EnumSet<MetricType> types = DefaultMetricFilter.parseMetricTypes("meter,CoUnTeR");
        assertThat(types).containsExactlyInAnyOrder(MetricType.METER, MetricType.COUNTER);
    }

    @Test
    void testFromConfigurationIncludeByScope() {
        Configuration configuration = new Configuration();
        configuration.set(
                MetricOptions.REPORTER_INCLUDES, Arrays.asList("include1:*:*", "include2.*:*:*"));
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Collections.emptyList());

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "name", "include1")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "name", "include1.bar")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "name", "include2")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "name", "include2.bar")).isTrue();
    }

    @Test
    void testFromConfigurationIncludeByName() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:name:*"));
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Collections.emptyList());

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "name", "bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationIncludeByType() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:*:counter"));
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Collections.emptyList());

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeByScope() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:*:*"));
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Arrays.asList("include1", "include2.*"));

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "name", "include1")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "name", "include1.bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "name", "include2")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "name", "include2.bar")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeByName() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:*:*"));
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Arrays.asList("*:faa*", "*:foo"));

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "name", "bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isFalse();
        assertThat(metricFilter.filter(COUNTER, "foob", "bar")).isTrue();
        assertThat(metricFilter.filter(COUNTER, "faab", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeByType() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:*:*"));
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Arrays.asList("*:*:meter"));

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isFalse();
    }

    @Test
    void testFromConfigurationIncludeDefault() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_EXCLUDES, Arrays.asList("*:*:meter"));

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "foo", "hello")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "hello")).isFalse();
    }

    @Test
    void testFromConfigurationExcludeDefault() {
        Configuration configuration = new Configuration();
        configuration.set(MetricOptions.REPORTER_INCLUDES, Arrays.asList("*:*:*"));

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
    }

    @Test
    void testFromConfigurationAllDefault() {
        Configuration configuration = new Configuration();

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isTrue();
    }

    @Test
    void testFromConfigurationMultiplePatterns() {
        Configuration configuration = new Configuration();

        configuration.set(MetricOptions.REPORTER_EXCLUDES, Arrays.asList("*:*:*"));
        configuration.setString(
                MetricOptions.REPORTER_EXCLUDES.key(), "*:foo,bar:meter;*:foo,bar:gauge");

        final MetricFilter metricFilter = DefaultMetricFilter.fromConfiguration(configuration);

        assertThat(metricFilter.filter(COUNTER, "foo", "bar")).isTrue();
        assertThat(metricFilter.filter(METER, "foo", "bar")).isFalse();
        assertThat(metricFilter.filter(GAUGE, "foo", "bar")).isFalse();
    }
}
