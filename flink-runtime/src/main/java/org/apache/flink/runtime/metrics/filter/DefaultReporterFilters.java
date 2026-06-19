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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.EventOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricType;
import org.apache.flink.traces.SpanBuilder;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Default {@link ReporterFilter} implementation that filters metrics based on {@link
 * MetricOptions#REPORTER_INCLUDES}/{@link MetricOptions#REPORTER_EXCLUDES}.
 */
public class DefaultReporterFilters {

    private static final EnumSet<MetricType> ALL_METRIC_TYPES = EnumSet.allOf(MetricType.class);
    @VisibleForTesting static final String LIST_DELIMITER = ",";

    abstract static class AbstractReporterFilter<T> implements ReporterFilter<T> {

        protected final List<FilterSpec> includes;
        protected final List<FilterSpec> excludes;

        AbstractReporterFilter(List<FilterSpec> includes, List<FilterSpec> excludes) {
            this.includes = includes;
            this.excludes = excludes;
        }
    }

    static class MetricReporterFilter extends AbstractReporterFilter<Metric> {

        MetricReporterFilter(List<FilterSpec> includes, List<FilterSpec> excludes) {
            super(includes, excludes);
        }

        @Override
        public boolean filter(Metric reported, String name, String logicalScope) {
            for (FilterSpec exclude : excludes) {
                if (exclude.namePattern.matcher(name).matches()
                        && exclude.scopePattern.matcher(logicalScope).matches()
                        && exclude.types.contains(reported.getMetricType())) {
                    return false;
                }
            }
            for (FilterSpec include : includes) {
                if (include.namePattern.matcher(name).matches()
                        && include.scopePattern.matcher(logicalScope).matches()
                        && include.types.contains(reported.getMetricType())) {
                    return true;
                }
            }
            return false;
        }
    }

    static class TraceReporterFilter extends AbstractReporterFilter<SpanBuilder> {

        TraceReporterFilter(List<FilterSpec> includes, List<FilterSpec> excludes) {
            super(includes, excludes);
        }

        @Override
        public boolean filter(SpanBuilder reported, String name, String logicalScope) {
            for (FilterSpec exclude : excludes) {
                if (exclude.namePattern.matcher(name).matches()
                        && exclude.scopePattern.matcher(logicalScope).matches()) {
                    return false;
                }
            }
            for (FilterSpec include : includes) {
                if (include.namePattern.matcher(name).matches()
                        && include.scopePattern.matcher(logicalScope).matches()) {
                    return true;
                }
            }
            return false;
        }
    }

    static class EventReporterFilter extends AbstractReporterFilter<EventBuilder> {

        EventReporterFilter(List<FilterSpec> includes, List<FilterSpec> excludes) {
            super(includes, excludes);
        }

        @Override
        public boolean filter(EventBuilder reported, String name, String logicalScope) {
            for (FilterSpec exclude : excludes) {
                if (exclude.namePattern.matcher(name).matches()
                        && exclude.scopePattern.matcher(logicalScope).matches()) {
                    return false;
                }
            }
            for (FilterSpec include : includes) {
                if (include.namePattern.matcher(name).matches()
                        && include.scopePattern.matcher(logicalScope).matches()) {
                    return true;
                }
            }
            return false;
        }
    }

    public static ReporterFilter<Metric> metricsFromConfiguration(Configuration configuration) {
        return fromConfiguration(
                configuration,
                MetricOptions.REPORTER_INCLUDES,
                MetricOptions.REPORTER_EXCLUDES,
                MetricReporterFilter::new);
    }

    public static ReporterFilter<SpanBuilder> tracesFromConfiguration(Configuration configuration) {
        return fromConfiguration(
                configuration,
                TraceOptions.REPORTER_INCLUDES,
                TraceOptions.REPORTER_EXCLUDES,
                TraceReporterFilter::new);
    }

    public static ReporterFilter<EventBuilder> eventsFromConfiguration(
            Configuration configuration) {
        return fromConfiguration(
                configuration,
                EventOptions.REPORTER_INCLUDES,
                EventOptions.REPORTER_EXCLUDES,
                EventReporterFilter::new);
    }

    private static <REPORTED> ReporterFilter<REPORTED> fromConfiguration(
            Configuration configuration,
            ConfigOption<List<String>> optionIncludes,
            ConfigOption<List<String>> optionExcludes,
            BiFunction<List<FilterSpec>, List<FilterSpec>, ReporterFilter<REPORTED>> factory) {

        final List<String> includes = configuration.get(optionIncludes);
        final List<String> excludes = configuration.get(optionExcludes);

        final List<FilterSpec> includeFilters =
                includes.stream().map(DefaultReporterFilters::parse).collect(Collectors.toList());
        final List<FilterSpec> excludeFilters =
                excludes.stream().map(DefaultReporterFilters::parse).collect(Collectors.toList());

        return factory.apply(includeFilters, excludeFilters);
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
