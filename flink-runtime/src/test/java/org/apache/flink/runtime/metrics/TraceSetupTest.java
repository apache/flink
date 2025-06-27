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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.filter.DefaultReporterFilters;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.TestTraceReporter;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.apache.flink.traces.reporter.TraceReporter;
import org.apache.flink.traces.reporter.TraceReporterFactory;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link ReporterSetup}. */
@ExtendWith(TestLoggerExtension.class)
class TraceSetupTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            TraceReporterFactory.class,
                            TestTraceReporter1.class.getName(),
                            TestTraceReporter2.class.getName(),
                            TestTraceReporter11.class.getName(),
                            TestTraceReporter12.class.getName(),
                            TestTraceReporter13.class.getName(),
                            TestTraceReporterFactory.class.getName(),
                            FailingFactory.class.getName(),
                            ConfigExposingReporterFactory.class.getName())
                    .build();

    /** TestReporter1 class only for type differentiation. */
    public static class TestTraceReporter1 extends TestTraceReporter {}

    /** TestReporter2 class only for type differentiation. */
    public static class TestTraceReporter2 extends TestTraceReporter {}

    /** Verifies that a reporter can be configured with all it's arguments being forwarded. */
    @Test
    void testReporterArgumentForwarding() {
        final Configuration config = new Configuration();

        configureReporter1(config);

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        Assert.assertEquals(1, reporterSetups.size());

        final TraceReporterSetup reporterSetup = reporterSetups.get(0);
        assertTraceReporter1Configured(reporterSetup);
    }

    /**
     * Verifies that multiple reporters can be configured with all their arguments being forwarded.
     */
    @Test
    void testSeveralReportersWithArgumentForwarding() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        Assert.assertEquals(2, reporterSetups.size());

        final Optional<TraceReporterSetup> reporter1Config =
                reporterSetups.stream().filter(c -> "reporter1".equals(c.getName())).findFirst();

        Assert.assertTrue(reporter1Config.isPresent());
        assertTraceReporter1Configured(reporter1Config.get());

        final Optional<TraceReporterSetup> reporter2Config =
                reporterSetups.stream().filter(c -> "reporter2".equals(c.getName())).findFirst();

        Assert.assertTrue(reporter2Config.isPresent());
        assertReporter2Configured(reporter2Config.get());
    }

    /**
     * Verifies that {@link TraceOptions#TRACE_REPORTERS_LIST} is correctly used to filter
     * configured reporters.
     */
    @Test
    void testActivateOneReporterAmongTwoDeclared() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        config.set(TraceOptions.TRACE_REPORTERS_LIST, "reporter2");

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        Assert.assertEquals(1, reporterSetups.size());

        final TraceReporterSetup setup = reporterSetups.get(0);
        assertReporter2Configured(setup);
    }

    @Test
    void testReporterSetupSupplier() throws Exception {
        final Configuration config = new Configuration();

        TraceOptions.forReporter(config, "reporter1")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporter1.class.getName());

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        Assert.assertEquals(1, reporterSetups.size());

        final TraceReporterSetup reporterSetup = reporterSetups.get(0);
        Assert.assertThat(reporterSetup.getReporter(), instanceOf(TestTraceReporter1.class));
    }

    /** Verifies that multiple reporters are instantiated correctly. */
    @Test
    void testMultipleReporterInstantiation() throws Exception {
        Configuration config = new Configuration();

        TraceOptions.forReporter(config, "test1")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporter11.class.getName());
        TraceOptions.forReporter(config, "test2")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporter12.class.getName());
        TraceOptions.forReporter(config, "test3")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporter13.class.getName());

        List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        assertEquals(3, reporterSetups.size());

        Assert.assertTrue(TestTraceReporter11.wasOpened);
        Assert.assertTrue(TestTraceReporter12.wasOpened);
        Assert.assertTrue(TestTraceReporter13.wasOpened);
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestTraceReporter11 extends TestTraceReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestTraceReporter12 extends TestTraceReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestTraceReporter13 extends TestTraceReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    private static void configureReporter1(Configuration config) {
        Configuration reporterConfig =
                TraceOptions.forReporter(config, "reporter1")
                        .set(
                                TraceOptions.REPORTER_FACTORY_CLASS,
                                TestTraceReporter1.class.getName());
        reporterConfig.setString("arg1", "value1");
        reporterConfig.setString("arg2", "value2");
    }

    private static void assertTraceReporter1Configured(TraceReporterSetup setup) {
        Assert.assertEquals("reporter1", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", ""));
        Assert.assertEquals("value2", setup.getConfiguration().getString("arg2", ""));
        Assert.assertEquals(
                TestTraceReporter1.class.getName(),
                setup.getConfiguration()
                        .getString(TraceOptions.REPORTER_FACTORY_CLASS.key(), null));
    }

    private static void configureReporter2(Configuration config) {
        Configuration reporterConfig =
                TraceOptions.forReporter(config, "reporter2")
                        .set(
                                TraceOptions.REPORTER_FACTORY_CLASS,
                                TestTraceReporter2.class.getName());
        reporterConfig.setString("arg1", "value1");
        reporterConfig.setString("arg3", "value3");
    }

    private static void assertReporter2Configured(TraceReporterSetup setup) {
        Assert.assertEquals("reporter2", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", null));
        Assert.assertEquals("value3", setup.getConfiguration().getString("arg3", null));
        Assert.assertEquals(
                TestTraceReporter2.class.getName(),
                setup.getConfiguration()
                        .getString(TraceOptions.REPORTER_FACTORY_CLASS.key(), null));
    }

    @Test
    void testVariableExclusionParsing() throws Exception {
        final String excludedVariable1 = "foo";
        final String excludedVariable2 = "foo";
        final Configuration config = new Configuration();
        TraceOptions.forReporter(config, "test")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporterFactory.class.getName())
                .set(
                        TraceOptions.REPORTER_EXCLUDED_VARIABLES,
                        excludedVariable1 + ";" + excludedVariable2);

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        assertEquals(1, reporterSetups.size());

        final TraceReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(
                reporterSetup.getExcludedVariables(),
                hasItems(
                        ScopeFormat.asVariable(excludedVariable1),
                        ScopeFormat.asVariable(excludedVariable2)));
    }

    /** Verifies that a factory configuration is correctly parsed. */
    @Test
    void testFactoryParsing() throws Exception {
        final Configuration config = new Configuration();
        TraceOptions.forReporter(config, "test")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporterFactory.class.getName());

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        assertEquals(1, reporterSetups.size());

        final TraceReporterSetup reporterSetup = reporterSetups.get(0);

        assertEquals(TestTraceReporterFactory.REPORTER, reporterSetup.getReporter());
    }

    /** Verifies that an error thrown by a factory does not affect the setup of other reporters. */
    @Test
    void testFactoryFailureIsolation() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.TRACES_REPORTER_PREFIX
                        + "test."
                        + TraceOptions.REPORTER_FACTORY_CLASS.key(),
                TestTraceReporterFactory.class.getName());
        config.setString(
                ConfigConstants.TRACES_REPORTER_PREFIX
                        + "fail."
                        + TraceOptions.REPORTER_FACTORY_CLASS.key(),
                FailingFactory.class.getName());

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        assertEquals(1, reporterSetups.size());
    }

    @Test
    void testFactoryArgumentForwarding() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.TRACES_REPORTER_PREFIX
                        + "test."
                        + TraceOptions.REPORTER_FACTORY_CLASS.key(),
                ConfigExposingReporterFactory.class.getName());
        config.setString(ConfigConstants.TRACES_REPORTER_PREFIX + "test.arg", "hello");

        ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                config, DefaultReporterFilters::tracesFromConfiguration, null);

        Properties passedConfig = ConfigExposingReporterFactory.lastConfig;
        assertEquals("hello", passedConfig.getProperty("arg"));
    }

    @Test
    void testAdditionalVariablesParsing() {
        final String tag1 = "foo";
        final String tagValue1 = "bar";
        final String tag2 = "fizz";
        final String tagValue2 = "buzz";
        final Configuration config = new Configuration();

        TraceOptions.forReporter(config, "test")
                .set(TraceOptions.REPORTER_FACTORY_CLASS, TestTraceReporterFactory.class.getName())
                .setString(
                        TraceOptions.REPORTER_ADDITIONAL_VARIABLES.key(),
                        String.join(",", tag1 + ":" + tagValue1, tag2 + ":" + tagValue2));

        final List<TraceReporterSetup> reporterSetups =
                ReporterSetupBuilder.TRACE_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::tracesFromConfiguration, null);

        assertEquals(1, reporterSetups.size());

        final TraceReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(
                reporterSetup.getAdditionalVariables(),
                hasEntry(ScopeFormat.asVariable(tag1), tagValue1));

        assertThat(
                reporterSetup.getAdditionalVariables(),
                hasEntry(ScopeFormat.asVariable(tag2), tagValue2));
    }

    /** Factory that exposed the last provided metric config. */
    public static class ConfigExposingReporterFactory implements TraceReporterFactory {

        static Properties lastConfig = null;

        @Override
        public TraceReporter createTraceReporter(Properties config) {
            lastConfig = config;
            return new TestTraceReporter();
        }
    }

    /** Factory that returns a static reporter. */
    public static class TestTraceReporterFactory implements TraceReporterFactory {

        static final TraceReporter REPORTER = new TestTraceReporter();

        @Override
        public TraceReporter createTraceReporter(Properties config) {
            return REPORTER;
        }
    }

    /** Factory that always throws an error. */
    public static class FailingFactory implements TraceReporterFactory {

        @Override
        public TraceReporter createTraceReporter(Properties config) {
            throw new RuntimeException();
        }
    }
}
