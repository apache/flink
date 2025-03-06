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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Default {@link MetricFilter} implementation that filters metrics based on {@link
 * MetricOptions#REPORTER_INCLUDES}/{@link MetricOptions#REPORTER_EXCLUDES}.
 */
public class DefaultMetricFilter implements MetricFilter {

    private static final EnumSet<MetricType> ALL_METRIC_TYPES = EnumSet.allOf(MetricType.class);
    @VisibleForTesting static final String LIST_DELIMITER = ",";

    private final List<FilterSpec> includes;
    private final List<FilterSpec> excludes;

    private DefaultMetricFilter(List<FilterSpec> includes, List<FilterSpec> excludes) {
        this.includes = includes;
        this.excludes = excludes;
    }

    @Override
    public boolean filter(Metric metric, String name, String logicalScope) {
        for (FilterSpec exclude : excludes) {
            if (exclude.namePattern.matcher(name).matches()
                    && exclude.scopePattern.matcher(logicalScope).matches()
                    && exclude.types.contains(metric.getMetricType())) {
                return false;
            }
        }
        for (FilterSpec include : includes) {
            if (include.namePattern.matcher(name).matches()
                    && include.scopePattern.matcher(logicalScope).matches()
                    && include.types.contains(metric.getMetricType())) {
                return true;
            }
        }
        return false;
    }

    public static MetricFilter fromConfiguration(Configuration configuration) {
        final List<String> includes = configuration.get(MetricOptions.REPORTER_INCLUDES);
        final List<String> excludes = configuration.get(MetricOptions.REPORTER_EXCLUDES);

        final List<FilterSpec> includeFilters =
                includes.stream().map(i -> parse(i)).collect(Collectors.toList());
        final List<FilterSpec> excludeFilters =
                excludes.stream().map(e -> parse(e)).collect(Collectors.toList());

        return new DefaultMetricFilter(includeFilters, excludeFilters);
    }

    private static FilterSpec parse(String filter) {
        final String[] split = filter.split(":");
        final Pattern scope = convertToPattern(split[0]);
        final Pattern name = split.length > 1 ? convertToPattern(split[1]) : convertToPattern("*");
        final EnumSet<MetricType> type =
                split.length > 2 ? parseMetricTypes(split[2]) : ALL_METRIC_TYPES;

        return new FilterSpec(scope, name, type);
    }

    @VisibleForTesting
    static Pattern convertToPattern(String scopeOrNameComponent) {
        final String[] split = scopeOrNameComponent.split(LIST_DELIMITER);

        final String rawPattern =
                Arrays.stream(split)
                        .map(s -> s.replaceAll("\\.", "\\."))
                        .map(s -> s.replaceAll("\\*", ".*"))
                        .collect(Collectors.joining("|", "(", ")"));

        return Pattern.compile(rawPattern);
    }

    @VisibleForTesting
    static EnumSet<MetricType> parseMetricTypes(String typeComponent) {
        final String[] split = typeComponent.split(LIST_DELIMITER);

        if (split.length == 1 && split[0].equals("*")) {
            return ALL_METRIC_TYPES;
        }

        return EnumSet.copyOf(
                Arrays.stream(split)
                        .map(s -> ConfigurationUtils.convertToEnum(s, MetricType.class))
                        .collect(Collectors.toSet()));
    }

    private static class FilterSpec {
        private final Pattern scopePattern;
        private final Pattern namePattern;
        private final EnumSet<MetricType> types;

        private FilterSpec(Pattern scopePattern, Pattern namePattern, EnumSet<MetricType> types) {
            this.scopePattern = scopePattern;
            this.namePattern = namePattern;
            this.types = types;
        }
    }
}
