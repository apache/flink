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
import org.apache.flink.configuration.EventOptions;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.events.reporter.EventReporterFactory;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.runtime.metrics.filter.DefaultReporterFilters;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.TestEventReporter;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
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
class EventSetupTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            EventReporterFactory.class,
                            TestEventReporter1.class.getName(),
                            TestEventReporter2.class.getName(),
                            TestEventReporter11.class.getName(),
                            TestEventReporter12.class.getName(),
                            TestEventReporter13.class.getName(),
                            TestEventReporterFactory.class.getName(),
                            FailingFactory.class.getName(),
                            ConfigExposingReporterFactory.class.getName())
                    .build();

    /** TestReporter1 class only for type differentiation. */
    public static class TestEventReporter1 extends TestEventReporter {}

    /** TestReporter2 class only for type differentiation. */
    public static class TestEventReporter2 extends TestEventReporter {}

    /** Verifies that a reporter can be configured with all it's arguments being forwarded. */
    @Test
    void testReporterArgumentForwarding() {
        final Configuration config = new Configuration();

        configureReporter1(config);

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        Assert.assertEquals(1, reporterSetups.size());

        final EventReporterSetup reporterSetup = reporterSetups.get(0);
        assertEventReporter1Configured(reporterSetup);
    }

    /**
     * Verifies that multiple reporters can be configured with all their arguments being forwarded.
     */
    @Test
    void testSeveralReportersWithArgumentForwarding() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        Assert.assertEquals(2, reporterSetups.size());

        final Optional<EventReporterSetup> reporter1Config =
                reporterSetups.stream().filter(c -> "reporter1".equals(c.getName())).findFirst();

        Assert.assertTrue(reporter1Config.isPresent());
        assertEventReporter1Configured(reporter1Config.get());

        final Optional<EventReporterSetup> reporter2Config =
                reporterSetups.stream().filter(c -> "reporter2".equals(c.getName())).findFirst();

        Assert.assertTrue(reporter2Config.isPresent());
        assertReporter2Configured(reporter2Config.get());
    }

    /**
     * Verifies that {@link EventOptions#REPORTERS_LIST} is correctly used to filter configured
     * reporters.
     */
    @Test
    void testActivateOneReporterAmongTwoDeclared() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        config.set(EventOptions.REPORTERS_LIST, "reporter2");

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        Assert.assertEquals(1, reporterSetups.size());

        final EventReporterSetup setup = reporterSetups.get(0);
        assertReporter2Configured(setup);
    }

    @Test
    void testReporterSetupSupplier() throws Exception {
        final Configuration config = new Configuration();

        EventOptions.forReporter(config, "reporter1")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporter1.class.getName());

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        Assert.assertEquals(1, reporterSetups.size());

        final EventReporterSetup reporterSetup = reporterSetups.get(0);
        Assert.assertThat(reporterSetup.getReporter(), instanceOf(TestEventReporter1.class));
    }

    /** Verifies that multiple reporters are instantiated correctly. */
    @Test
    void testMultipleReporterInstantiation() throws Exception {
        Configuration config = new Configuration();

        EventOptions.forReporter(config, "test1")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporter11.class.getName());
        EventOptions.forReporter(config, "test2")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporter12.class.getName());
        EventOptions.forReporter(config, "test3")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporter13.class.getName());

        List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        assertEquals(3, reporterSetups.size());

        Assert.assertTrue(TestEventReporter11.wasOpened);
        Assert.assertTrue(TestEventReporter12.wasOpened);
        Assert.assertTrue(TestEventReporter13.wasOpened);
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestEventReporter11 extends TestEventReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestEventReporter12 extends TestEventReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestEventReporter13 extends TestEventReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    private static void configureReporter1(Configuration config) {
        Configuration reporterConfig =
                EventOptions.forReporter(config, "reporter1")
                        .set(
                                EventOptions.REPORTER_FACTORY_CLASS,
                                TestEventReporter1.class.getName());
        reporterConfig.setString("arg1", "value1");
        reporterConfig.setString("arg2", "value2");
    }

    private static void assertEventReporter1Configured(EventReporterSetup setup) {
        Assert.assertEquals("reporter1", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", ""));
        Assert.assertEquals("value2", setup.getConfiguration().getString("arg2", ""));
        Assert.assertEquals(
                TestEventReporter1.class.getName(),
                setup.getConfiguration()
                        .getString(EventOptions.REPORTER_FACTORY_CLASS.key(), null));
    }

    private static void configureReporter2(Configuration config) {
        Configuration reporterConfig =
                EventOptions.forReporter(config, "reporter2")
                        .set(
                                EventOptions.REPORTER_FACTORY_CLASS,
                                TestEventReporter2.class.getName());
        reporterConfig.setString("arg1", "value1");
        reporterConfig.setString("arg3", "value3");
    }

    private static void assertReporter2Configured(EventReporterSetup setup) {
        Assert.assertEquals("reporter2", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", null));
        Assert.assertEquals("value3", setup.getConfiguration().getString("arg3", null));
        Assert.assertEquals(
                TestEventReporter2.class.getName(),
                setup.getConfiguration()
                        .getString(EventOptions.REPORTER_FACTORY_CLASS.key(), null));
    }

    @Test
    void testVariableExclusionParsing() throws Exception {
        final String excludedVariable1 = "foo";
        final String excludedVariable2 = "foo";
        final Configuration config = new Configuration();
        EventOptions.forReporter(config, "test")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporterFactory.class.getName())
                .set(
                        EventOptions.REPORTER_EXCLUDED_VARIABLES,
                        excludedVariable1 + ";" + excludedVariable2);

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        assertEquals(1, reporterSetups.size());

        final EventReporterSetup reporterSetup = reporterSetups.get(0);

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
        EventOptions.forReporter(config, "test")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporterFactory.class.getName());

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        assertEquals(1, reporterSetups.size());

        final EventReporterSetup reporterSetup = reporterSetups.get(0);

        assertEquals(TestEventReporterFactory.REPORTER, reporterSetup.getReporter());
    }

    /** Verifies that an error thrown by a factory does not affect the setup of other reporters. */
    @Test
    void testFactoryFailureIsolation() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.EVENTS_REPORTER_PREFIX
                        + "test."
                        + EventOptions.REPORTER_FACTORY_CLASS.key(),
                TestEventReporterFactory.class.getName());
        config.setString(
                ConfigConstants.EVENTS_REPORTER_PREFIX
                        + "fail."
                        + EventOptions.REPORTER_FACTORY_CLASS.key(),
                FailingFactory.class.getName());

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        assertEquals(1, reporterSetups.size());
    }

    @Test
    void testFactoryArgumentForwarding() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.EVENTS_REPORTER_PREFIX
                        + "test."
                        + EventOptions.REPORTER_FACTORY_CLASS.key(),
                ConfigExposingReporterFactory.class.getName());
        config.setString(ConfigConstants.EVENTS_REPORTER_PREFIX + "test.arg", "hello");

        ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                config, DefaultReporterFilters::eventsFromConfiguration, null);

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

        EventOptions.forReporter(config, "test")
                .set(EventOptions.REPORTER_FACTORY_CLASS, TestEventReporterFactory.class.getName())
                .setString(
                        EventOptions.REPORTER_ADDITIONAL_VARIABLES.key(),
                        String.join(",", tag1 + ":" + tagValue1, tag2 + ":" + tagValue2));

        final List<EventReporterSetup> reporterSetups =
                ReporterSetupBuilder.EVENT_SETUP_BUILDER.fromConfiguration(
                        config, DefaultReporterFilters::eventsFromConfiguration, null);

        assertEquals(1, reporterSetups.size());

        final EventReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(
                reporterSetup.getAdditionalVariables(),
                hasEntry(ScopeFormat.asVariable(tag1), tagValue1));

        assertThat(
                reporterSetup.getAdditionalVariables(),
                hasEntry(ScopeFormat.asVariable(tag2), tagValue2));
    }

    /** Factory that exposed the last provided metric config. */
    public static class ConfigExposingReporterFactory implements EventReporterFactory {

        static Properties lastConfig = null;

        @Override
        public EventReporter createEventReporter(Properties config) {
            lastConfig = config;
            return new TestEventReporter();
        }
    }

    /** Factory that returns a static reporter. */
    public static class TestEventReporterFactory implements EventReporterFactory {

        static final EventReporter REPORTER = new TestEventReporter();

        @Override
        public EventReporter createEventReporter(Properties config) {
            return REPORTER;
        }
    }

    /** Factory that always throws an error. */
    public static class FailingFactory implements EventReporterFactory {

        @Override
        public EventReporter createEventReporter(Properties config) {
            throw new RuntimeException();
        }
    }
}
