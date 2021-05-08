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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ReporterSetup}. */
public class ReporterSetupTest extends TestLogger {

    /** TestReporter1 class only for type differentiation. */
    static class TestReporter1 extends TestReporter {}

    /** TestReporter2 class only for type differentiation. */
    static class TestReporter2 extends TestReporter {}

    /** Verifies that a reporter can be configured with all it's arguments being forwarded. */
    @Test
    public void testReporterArgumentForwarding() {
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
    public void testSeveralReportersWithArgumentForwarding() {
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
    public void testActivateOneReporterAmongTwoDeclared() {
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
    public void testReporterSetupSupplier() throws Exception {
        final Configuration config = new Configuration();

        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "reporter1."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                TestReporter1.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        Assert.assertEquals(1, reporterSetups.size());

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        final MetricReporter metricReporter = reporterSetup.getReporter();
        Assert.assertThat(metricReporter, instanceOf(TestReporter1.class));
    }

    /** Verifies that multiple reporters are instantiated correctly. */
    @Test
    public void testMultipleReporterInstantiation() throws Exception {
        Configuration config = new Configuration();

        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                TestReporter11.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                TestReporter12.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test3."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                TestReporter13.class.getName());

        List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(3, reporterSetups.size());

        Assert.assertTrue(TestReporter11.wasOpened);
        Assert.assertTrue(TestReporter12.wasOpened);
        Assert.assertTrue(TestReporter13.wasOpened);
    }

    /** Reporter that exposes whether open() was called. */
    protected static class TestReporter11 extends TestReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    protected static class TestReporter12 extends TestReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    /** Reporter that exposes whether open() was called. */
    protected static class TestReporter13 extends TestReporter {
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
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
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
                setup.getConfiguration().getString("class", null));
    }

    private static void configureReporter2(Configuration config) {
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "reporter2."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                TestReporter2.class.getName());
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg1", "value1");
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "reporter2.arg3", "value3");
    }

    private static void assertReporter2Configured(ReporterSetup setup) {
        Assert.assertEquals("reporter2", setup.getName());
        Assert.assertEquals("value1", setup.getConfiguration().getString("arg1", null));
        Assert.assertEquals("value3", setup.getConfiguration().getString("arg3", null));
        Assert.assertEquals(
                TestReporter2.class.getName(), setup.getConfiguration().getString("class", null));
    }

    @Test
    public void testVariableExclusionParsing() throws Exception {
        final String excludedVariable1 = "foo";
        final String excludedVariable2 = "foo";
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
                TestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_EXCLUDED_VARIABLES,
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
    public void testFactoryParsing() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
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
    public void testFactoryPrioritization() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
                InstantiationTypeTrackingTestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
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
    public void testFactoryFailureIsolation() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
                TestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "fail."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
                FailingFactory.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertEquals(1, reporterSetups.size());
    }

    /** Verifies that factory/reflection approaches can be mixed freely. */
    @Test
    public void testMixedSetupsFactoryParsing() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
                InstantiationTypeTrackingTestReporterFactory.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
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
    public void testFactoryArgumentForwarding() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX,
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
    public void testFactoryAnnotation() {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
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
    public void testReflectionInterception() {
        final Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
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
    protected static class InstantiationTypeTrackingTestReporter2
            extends InstantiationTypeTrackingTestReporter {}
}
