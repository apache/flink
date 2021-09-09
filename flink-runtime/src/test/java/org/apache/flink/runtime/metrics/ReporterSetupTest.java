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
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ReporterSetup}. */
@ExtendWith(TestLoggerExtension.class)
class ReporterSetupTest {

    @RegisterExtension
    static final ContextClassLoaderExtension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            MetricReporterFactory.class,
                            TestReporter1.class.getName(),
                            TestReporter2.class.getName(),
                            TestReporter11.class.getName(),
                            TestReporter12.class.getName(),
                            TestReporter13.class.getName(),
                            TestReporterFactory.class.getName(),
                            FailingFactory.class.getName(),
                            InstantiationTypeTrackingTestReporterFactory.class.getName(),
                            ConfigExposingReporterFactory.class.getName())
                    .build();

    /** TestReporter1 class only for type differentiation. */
    public static class TestReporter1 extends TestReporter {}

    /** TestReporter2 class only for type differentiation. */
    public static class TestReporter2 extends TestReporter {}

    /** Verifies that a reporter can be configured with all it's arguments being forwarded. */
    @Test
    void testReporterArgumentForwarding() {
        final Configuration config = new Configuration();

        configureReporter1(config);

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        Assert.assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        assertReporter1Configured(reporterSetup);
    }

    /**
     * Verifies that multiple reporters can be configured with all their arguments being forwarded.
     */
    @Test
    void testSeveralReportersWithArgumentForwarding() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        Assert.assertEquals(2, reporterSetups.size());

        final Optional<ReporterSetup> reporter1Config =
                reporterSetups.stream().filter(c -> "reporter1".equals(c.getName())).findFirst();

        Assert.assertTrue(reporter1Config.isPresent());
        assertReporter1Configured(reporter1Config.get());

        final Optional<ReporterSetup> reporter2Config =
                reporterSetups.stream().filter(c -> "reporter2".equals(c.getName())).findFirst();

        Assert.assertTrue(reporter2Config.isPresent());
        assertReporter2Configured(reporter2Config.get());
    }

    /**
     * Verifies that {@link MetricOptions#REPORTERS_LIST} is correctly used to filter configured
     * reporters.
     */
    @Test
    void testActivateOneReporterAmongTwoDeclared() {
        final Configuration config = new Configuration();

        configureReporter1(config);
        configureReporter2(config);

        config.setString(MetricOptions.REPORTERS_LIST, "reporter2");

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        Assert.assertEquals(1, reporterSetups.size());

        final ReporterSetup setup = reporterSetups.get(0);
        assertReporter2Configured(setup);
    }

    @Test
    void testReporterSetupSupplier() throws Exception {
        final Configuration config = new Configuration();

        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "reporter1."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporter1.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        Assert.assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        final MetricReporter metricReporter = reporterSetup.getReporter();
        Assert.assertThat(metricReporter, instanceOf(TestReporter1.class));
    }

    /** Verifies that multiple reporters are instantiated correctly. */
    @Test
    void testMultipleReporterInstantiation() throws Exception {
        Configuration config = new Configuration();

        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporter11.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporter12.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test3."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporter13.class.getName());

        List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(3, reporterSetups.size());

        Assert.assertTrue(TestReporter11.wasOpened);
        Assert.assertTrue(TestReporter12.wasOpened);
        Assert.assertTrue(TestReporter13.wasOpened);
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestReporter11 extends TestReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestReporter12 extends TestReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    public static class TestReporter13 extends TestReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    private static void configureReporter1(Configuration config) {
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "reporter1."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporter1.class.getName());
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg1", "value1");
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter1.arg2", "value2");
    }

    private static void assertReporter1Configured(ReporterSetup setup) {
        Assert.assertEquals("reporter1", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", ""));
        Assert.assertEquals("value2", setup.getConfiguration().getString("arg2", ""));
        Assert.assertEquals(
                ReporterSetupTest.TestReporter1.class.getName(),
                setup.getConfiguration()
                        .getString(MetricOptions.REPORTER_FACTORY_CLASS.key(), null));
    }

    private static void configureReporter2(Configuration config) {
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "reporter2."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporter2.class.getName());
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg1", "value1");
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg3", "value3");
    }

    private static void assertReporter2Configured(ReporterSetup setup) {
        Assert.assertEquals("reporter2", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", null));
        Assert.assertEquals("value3", setup.getConfiguration().getString("arg3", null));
        Assert.assertEquals(
                TestReporter2.class.getName(),
                setup.getConfiguration()
                        .getString(MetricOptions.REPORTER_FACTORY_CLASS.key(), null));
    }

    @Test
    void testVariableExclusionParsing() throws Exception {
        final String excludedVariable1 = "foo";
        final String excludedVariable2 = "foo";
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_EXCLUDED_VARIABLES.key(),
                excludedVariable1 + ";" + excludedVariable2);

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);

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
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporterFactory.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);

        assertEquals(TestReporterFactory.REPORTER, reporterSetup.getReporter());
    }

    /**
     * Verifies that the factory approach is prioritized if both the factory and reflection approach
     * are configured.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testFactoryPrioritization() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                InstantiationTypeTrackingTestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_CLASS.key(),
                InstantiationTypeTrackingTestReporter.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        final InstantiationTypeTrackingTestReporter metricReporter =
                (InstantiationTypeTrackingTestReporter) reporterSetup.getReporter();

        assertTrue(metricReporter.createdByFactory);
    }

    /** Verifies that an error thrown by a factory does not affect the setup of other reporters. */
    @Test
    void testFactoryFailureIsolation() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "fail."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                FailingFactory.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());
    }

    /** Verifies that factory/reflection approaches can be mixed freely. */
    @Test
    @SuppressWarnings("deprecation")
    public void testMixedSetupsFactoryParsing() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                InstantiationTypeTrackingTestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + MetricOptions.REPORTER_CLASS.key(),
                InstantiationTypeTrackingTestReporter.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(2, reporterSetups.size());

        final ReporterSetup reporterSetup1 = reporterSetups.get(0);
        final ReporterSetup reporterSetup2 = reporterSetups.get(1);

        final InstantiationTypeTrackingTestReporter metricReporter1 =
                (InstantiationTypeTrackingTestReporter) reporterSetup1.getReporter();
        final InstantiationTypeTrackingTestReporter metricReporter2 =
                (InstantiationTypeTrackingTestReporter) reporterSetup2.getReporter();

        assertTrue(metricReporter1.createdByFactory ^ metricReporter2.createdByFactory);
    }

    @Test
    void testFactoryArgumentForwarding() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                ConfigExposingReporterFactory.class.getName());
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.arg", "hello");

        ReporterSetup.fromConfiguration(config, null);

        Properties passedConfig = ConfigExposingReporterFactory.lastConfig;
        assertEquals("hello", passedConfig.getProperty("arg"));
    }

    /**
     * Verifies that the factory approach is used if the factory is annotated with {@link
     * InstantiateViaFactory}.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testFactoryAnnotation() {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_CLASS.key(),
                InstantiationTypeTrackingTestReporter2.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        final InstantiationTypeTrackingTestReporter metricReporter =
                (InstantiationTypeTrackingTestReporter) reporterSetup.getReporter();

        assertTrue(metricReporter.createdByFactory);
    }

    /**
     * Verifies that the factory approach is used if the factory is annotated with {@link
     * org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection}.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testReflectionInterception() {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_CLASS.key(),
                InstantiationTypeTrackingTestReporter.class.getName());

        final List<ReporterSetup> reporterSetups =
                ReporterSetup.fromConfiguration(
                        config,
                        new TestingPluginManager(
                                Collections.singletonMap(
                                        MetricReporterFactory.class,
                                        Collections.singletonList(
                                                        new InterceptingInstantiationTypeTrackingTestReporterFactory())
                                                .iterator())));

        assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        final InstantiationTypeTrackingTestReporter metricReporter =
                (InstantiationTypeTrackingTestReporter) reporterSetup.getReporter();

        assertTrue(metricReporter.createdByFactory);
    }

    @Test
    void testAdditionalVariablesParsing() {
        final String tag1 = "foo";
        final String tagValue1 = "bar";
        final String tag2 = "fizz";
        final String tagValue2 = "buzz";
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_FACTORY_CLASS.key(),
                TestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + MetricOptions.REPORTER_ADDITIONAL_VARIABLES.key(),
                String.join(",", tag1 + ":" + tagValue1, tag2 + ":" + tagValue2));

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(
                reporterSetup.getAdditionalVariables(),
                hasEntry(ScopeFormat.asVariable(tag1), tagValue1));

        assertThat(
                reporterSetup.getAdditionalVariables(),
                hasEntry(ScopeFormat.asVariable(tag2), tagValue2));
    }

    /** Factory that exposed the last provided metric config. */
    public static class ConfigExposingReporterFactory implements MetricReporterFactory {

        static Properties lastConfig = null;

        @Override
        public MetricReporter createMetricReporter(Properties config) {
            lastConfig = config;
            return new TestReporter();
        }
    }

    /** Factory that returns a static reporter. */
    public static class TestReporterFactory implements MetricReporterFactory {

        static final MetricReporter REPORTER = new TestReporter();

        @Override
        public MetricReporter createMetricReporter(Properties config) {
            return REPORTER;
        }
    }

    /** Factory that always throws an error. */
    public static class FailingFactory implements MetricReporterFactory {

        @Override
        public MetricReporter createMetricReporter(Properties config) {
            throw new RuntimeException();
        }
    }

    /** Factory for {@link InstantiationTypeTrackingTestReporter}. */
    public static class InstantiationTypeTrackingTestReporterFactory
            implements MetricReporterFactory {

        @Override
        public MetricReporter createMetricReporter(Properties config) {
            return new InstantiationTypeTrackingTestReporter(true);
        }
    }

    /**
     * Factory for {@link InstantiationTypeTrackingTestReporter} that intercepts reflection-based
     * instantiation attempts.
     */
    @InterceptInstantiationViaReflection(
            reporterClassName =
                    "org.apache.flink.runtime.metrics.ReporterSetupTest$InstantiationTypeTrackingTestReporter")
    @SuppressWarnings("deprecation")
    public static class InterceptingInstantiationTypeTrackingTestReporterFactory
            implements MetricReporterFactory {

        @Override
        public MetricReporter createMetricReporter(Properties config) {
            return new InstantiationTypeTrackingTestReporter(true);
        }
    }

    /** Reporter that exposes which constructor was called. */
    protected static class InstantiationTypeTrackingTestReporter extends TestReporter {

        private final boolean createdByFactory;

        public InstantiationTypeTrackingTestReporter() {
            this(false);
        }

        InstantiationTypeTrackingTestReporter(boolean createdByFactory) {
            this.createdByFactory = createdByFactory;
        }

        public boolean isCreatedByFactory() {
            return createdByFactory;
        }
    }

    /** Annotated reporter that exposes which constructor was called. */
    @InstantiateViaFactory(
            factoryClassName =
                    "org.apache.flink.runtime.metrics.ReporterSetupTest$InstantiationTypeTrackingTestReporterFactory")
    @SuppressWarnings("deprecation")
    protected static class InstantiationTypeTrackingTestReporter2
            extends InstantiationTypeTrackingTestReporter {}
}
