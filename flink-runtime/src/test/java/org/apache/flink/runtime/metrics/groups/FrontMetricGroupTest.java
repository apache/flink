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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

/** Tests for {@link FrontMetricGroup}. */
public class FrontMetricGroupTest {

    @Test
    public void testDelimiterReplacement() {
        final char delimiter = '*';
        final String hostName = "some" + delimiter + "host";
        final String metricName = "hello" + delimiter + "world";

        final Configuration config = new Configuration();
        config.set(MetricOptions.SCOPE_NAMING_JM, ScopeFormat.SCOPE_HOST);

        final FrontMetricGroup<?> frontMetricGroup =
                new FrontMetricGroup<>(
                        new ReporterScopedSettings(0, delimiter, Collections.emptySet()),
                        new ProcessMetricGroup(
                                TestingMetricRegistry.builder()
                                        .setScopeFormats(ScopeFormats.fromConfig(config))
                                        .build(),
                                hostName));

        assertThat(
                frontMetricGroup.getMetricIdentifier(metricName),
                is(
                        hostName.replace(delimiter, FrontMetricGroup.DEFAULT_REPLACEMENT)
                                + delimiter
                                + metricName.replace(
                                        delimiter, FrontMetricGroup.DEFAULT_REPLACEMENT)));
        // delimiters in variables should not be filtered, because they are usually not used in a
        // context where the delimiter matters
        assertThat(frontMetricGroup.getAllVariables(), hasEntry(ScopeFormat.SCOPE_HOST, hostName));
    }

    @Test
    public void testDelimiterReplacementWithAlternative() {
        final char delimiter = FrontMetricGroup.DEFAULT_REPLACEMENT;
        final String hostName = "some" + delimiter + "host";
        final String metricName = "hello" + delimiter + "world";

        final Configuration config = new Configuration();
        config.set(MetricOptions.SCOPE_NAMING_JM, ScopeFormat.SCOPE_HOST);

        final FrontMetricGroup<?> frontMetricGroup =
                new FrontMetricGroup<>(
                        new ReporterScopedSettings(0, delimiter, Collections.emptySet()),
                        new ProcessMetricGroup(
                                TestingMetricRegistry.builder()
                                        .setScopeFormats(ScopeFormats.fromConfig(config))
                                        .build(),
                                hostName));

        assertThat(
                frontMetricGroup.getMetricIdentifier(metricName),
                is(
                        hostName.replace(
                                        delimiter, FrontMetricGroup.DEFAULT_REPLACEMENT_ALTERNATIVE)
                                + delimiter
                                + metricName.replace(
                                        delimiter,
                                        FrontMetricGroup.DEFAULT_REPLACEMENT_ALTERNATIVE)));
        // delimiters in variables should not be filtered, because they are usually not used in a
        // context where the delimiter matters
        assertThat(frontMetricGroup.getAllVariables(), hasEntry(ScopeFormat.SCOPE_HOST, hostName));
    }
}
