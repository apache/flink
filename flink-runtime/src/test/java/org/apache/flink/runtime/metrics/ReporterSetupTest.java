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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ReporterSetup}. */
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

        assertThat(reporterSetups).hasSize(1);

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

        assertThat(reporterSetups).hasSize(2);

        final Optional<ReporterSetup> reporter1Config =
                reporterSetups.stream().filter(c -> "reporter1".equals(c.getName())).findFirst();

        assertThat(reporter1Config).isPresent();
        assertReporter1Configured(reporter1Config.get());

        final Optional<ReporterSetup> reporter2Config =
                reporterSetups.stream().filter(c -> "reporter2".equals(c.getName())).findFirst();

        assertThat(reporter2Config).isPresent();
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

        assertThat(reporterSetups).hasSize(1);

        final ReporterSetup setup = reporterSetups.get(0);
        assertReporter2Configured(setup);
    }

    @Test
    void testReporterSetupSupplier() throws Exception {
        final Configuration config = new Configuration();

        MetricOptions.forReporter(config, "reporter1")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporter1.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertThat(reporterSetups).hasSize(1);

        final ReporterSetup reporterSetup = reporterSetups.get(0);
        final MetricReporter metricReporter = reporterSetup.getReporter();
        assertThat(metricReporter).isInstanceOf(TestReporter1.class);
    }

    /** Verifies that multiple reporters are instantiated correctly. */
    @Test
    void testMultipleReporterInstantiation() throws Exception {
        Configuration config = new Configuration();

        MetricOptions.forReporter(config, "test1")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporter11.class.getName());
        MetricOptions.forReporter(config, "test2")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporter12.class.getName());
        MetricOptions.forReporter(config, "test3")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporter13.class.getName());

        List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertThat(reporterSetups).hasSize(3);

        assertThat(TestReporter11.wasOpened).isTrue();
        assertThat(TestReporter12.wasOpened).isTrue();
        assertThat(TestReporter13.wasOpened).isTrue();
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
        Configuration reporterConfig =
                MetricOptions.forReporter(config, "reporter1")
                        .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporter1.class.getName());
        reporterConfig.setString("arg1", "value1");
        reporterConfig.setString("arg2", "value2");
    }

    private static void assertReporter1Configured(ReporterSetup setup) {
        assertThat(setup.getName()).isEqualTo("reporter1");
        assertThat(setup.getConfiguration().getString("arg1", "")).isEqualTo("value1");
        assertThat(setup.getConfiguration().getString("arg2", "")).isEqualTo("value2");
        assertThat(
                        setup.getConfiguration()
                                .getString(MetricOptions.REPORTER_FACTORY_CLASS.key(), null))
                .isEqualTo(ReporterSetupTest.TestReporter1.class.getName());
    }

    private static void configureReporter2(Configuration config) {
        Configuration reporterConfig =
                MetricOptions.forReporter(config, "reporter2")
                        .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporter2.class.getName());
        reporterConfig.setString("arg1", "value1");
        reporterConfig.setString("arg3", "value3");
    }

    private static void assertReporter2Configured(ReporterSetup setup) {
        assertThat(setup.getName()).isEqualTo("reporter2");
        assertThat(setup.getConfiguration().getString("arg1", null)).isEqualTo("value1");
        assertThat(setup.getConfiguration().getString("arg3", null)).isEqualTo("value3");
        assertThat(
                        setup.getConfiguration()
                                .getString(MetricOptions.REPORTER_FACTORY_CLASS.key(), null))
                .isEqualTo(TestReporter2.class.getName());
    }

    @Test
    void testVariableExclusionParsing() throws Exception {
        final String excludedVariable1 = "foo";
        final String excludedVariable2 = "foo";
        final Configuration config = new Configuration();
        MetricOptions.forReporter(config, "test")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporterFactory.class.getName())
                .set(
                        MetricOptions.REPORTER_EXCLUDED_VARIABLES,
                        excludedVariable1 + ";" + excludedVariable2);

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertThat(reporterSetups).hasSize(1);

        final ReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(reporterSetup.getExcludedVariables())
                .containsAnyOf(
                        ScopeFormat.asVariable(excludedVariable1),
                        ScopeFormat.asVariable(excludedVariable2));
    }

    /** Verifies that a factory configuration is correctly parsed. */
    @Test
    void testFactoryParsing() throws Exception {
        final Configuration config = new Configuration();
        MetricOptions.forReporter(config, "test")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporterFactory.class.getName());

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertThat(reporterSetups).hasSize(1);

        final ReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(reporterSetup.getReporter()).isEqualTo(TestReporterFactory.REPORTER);
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

        assertThat(reporterSetups).hasSize(1);
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
        assertThat(passedConfig.getProperty("arg")).isEqualTo("hello");
    }

    @Test
    void testAdditionalVariablesParsing() {
        final String tag1 = "foo";
        final String tagValue1 = "bar";
        final String tag2 = "fizz";
        final String tagValue2 = "buzz";
        final Configuration config = new Configuration();

        MetricOptions.forReporter(config, "test")
                .set(MetricOptions.REPORTER_FACTORY_CLASS, TestReporterFactory.class.getName())
                .setString(
                        MetricOptions.REPORTER_ADDITIONAL_VARIABLES.key(),
                        String.join(",", tag1 + ":" + tagValue1, tag2 + ":" + tagValue2));

        final List<ReporterSetup> reporterSetups = ReporterSetup.fromConfiguration(config, null);

        assertThat(reporterSetups).hasSize(1);

        final ReporterSetup reporterSetup = reporterSetups.get(0);

        assertThat(reporterSetup.getAdditionalVariables())
                .containsEntry(ScopeFormat.asVariable(tag1), tagValue1);

        assertThat(reporterSetup.getAdditionalVariables())
                .containsEntry(ScopeFormat.asVariable(tag2), tagValue2);
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
}
