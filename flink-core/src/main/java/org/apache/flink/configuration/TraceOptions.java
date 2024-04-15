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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for traces and trace reporters. */
@Experimental
public class TraceOptions {

    private static final String NAMED_REPORTER_CONFIG_PREFIX =
            ConfigConstants.TRACES_REPORTER_PREFIX + "<name>";

    /**
     * An optional list of reporter names. If configured, only reporters whose name matches any of
     * the names in the list will be started. Otherwise, all reporters that could be found in the
     * configuration will be started.
     *
     * <p>Example:
     *
     * <pre>{@code
     * traces.reporters = foo,bar
     *
     * traces.reporter.foo.class = org.apache.flink.traces.reporter.OpenTelemetryTraceReporter
     * traces.reporter.foo.endpoint = 127.0.0.1:4137
     * }</pre>
     */
    public static final ConfigOption<String> TRACE_REPORTERS_LIST =
            key("traces.reporters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional list of trace reporter names. If configured, only reporters whose name matches"
                                    + " any of the names in the list will be started. Otherwise, all reporters that could be found in"
                                    + " the configuration will be started.");

    /**
     * Temporary option to report events as span. This option will be removed once we support
     * reporting events.
     */
    @Deprecated
    public static final ConfigOption<Boolean> REPORT_EVENTS_AS_SPANS =
            key("traces.report-events-as-spans")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to report events as spans. This is a temporary parameter that "
                                    + "is in place until we have support for reporting events. "
                                    + "In the meantime, this can be activated to report them as spans instead.");

    /**
     * Returns a view over the given configuration via which options can be set/retrieved for the
     * given reporter.
     *
     * <pre>
     *     Configuration config = ...
     *     MetricOptions.forReporter(config, "my_reporter")
     *         .set(MetricOptions.REPORTER_INTERVAL, Duration.ofSeconds(10))
     *         ...
     * </pre>
     *
     * @param configuration backing configuration
     * @param reporterName reporter name
     * @return view over configuration
     */
    @Experimental
    public static Configuration forTraceReporter(Configuration configuration, String reporterName) {
        return new DelegatingConfiguration(
                configuration, ConfigConstants.TRACES_REPORTER_PREFIX + reporterName + ".");
    }

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 1)
    public static final ConfigOption<String> REPORTER_FACTORY_CLASS =
            key("factory.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The reporter factory class to use for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 6)
    public static final ConfigOption<String> REPORTER_CONFIG_PARAMETER =
            key("<parameter>")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Configures the parameter <parameter> for the reporter named <name>.");

    @Documentation.SuffixOption(NAMED_REPORTER_CONFIG_PREFIX)
    @Documentation.Section(value = Documentation.Sections.TRACE_REPORTERS, position = 3)
    public static final ConfigOption<Map<String, String>> REPORTER_ADDITIONAL_VARIABLES =
            key("scope.variables.additional")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "The map of additional variables that should be included for the reporter named <name>.");

    private TraceOptions() {}
}
