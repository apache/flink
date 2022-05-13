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

package org.apache.flink.metrics.jmx;

import org.apache.flink.management.jmx.JMXService;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.metrics.util.TestMetricGroup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import static org.apache.flink.metrics.jmx.JMXReporter.JMX_DOMAIN_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the JMXReporter. */
class JMXReporterTest {

    private static final Map<String, String> variables;
    private static final MetricGroup metricGroup;

    static {
        variables = new HashMap<>();
        variables.put("<host>", "localhost");

        metricGroup =
                TestMetricGroup.newBuilder()
                        .setLogicalScopeFunction((characterFilter, character) -> "taskmanager")
                        .setVariables(variables)
                        .build();
    }

    @AfterEach
    void shutdownService() throws IOException {
        JMXService.stopInstance();
    }

    @Test
    void testReplaceInvalidChars() {
        assertThat(JMXReporter.replaceInvalidChars("")).isEqualTo("");
        assertThat(JMXReporter.replaceInvalidChars("abc")).isEqualTo("abc");
        assertThat(JMXReporter.replaceInvalidChars("abc\"")).isEqualTo("abc");
        assertThat(JMXReporter.replaceInvalidChars("\"abc")).isEqualTo("abc");
        assertThat(JMXReporter.replaceInvalidChars("\"abc\"")).isEqualTo("abc");
        assertThat(JMXReporter.replaceInvalidChars("\"a\"b\"c\"")).isEqualTo("abc");
        assertThat(JMXReporter.replaceInvalidChars("\"\"\"\"")).isEqualTo("");
        assertThat(JMXReporter.replaceInvalidChars("    ")).isEqualTo("____");
        assertThat(JMXReporter.replaceInvalidChars("\"ab ;(c)'")).isEqualTo("ab_-(c)-");
        assertThat(JMXReporter.replaceInvalidChars("a b c")).isEqualTo("a_b_c");
        assertThat(JMXReporter.replaceInvalidChars("a b c ")).isEqualTo("a_b_c_");
        assertThat(JMXReporter.replaceInvalidChars("a;b'c*")).isEqualTo("a-b-c-");
        assertThat(JMXReporter.replaceInvalidChars("a,=;:?'b,=;:?'c")).isEqualTo("a------b------c");
    }

    /** Verifies that the JMXReporter properly generates the JMX table. */
    @Test
    void testGenerateTable() {
        Map<String, String> vars = new HashMap<>();
        vars.put("key0", "value0");
        vars.put("key1", "value1");
        vars.put("\"key2,=;:?'", "\"value2 (test),=;:?'");

        Hashtable<String, String> jmxTable = JMXReporter.generateJmxTable(vars);

        assertThat(jmxTable).containsEntry("key0", "value0");
        assertThat(jmxTable).containsEntry("key0", "value0");
        assertThat(jmxTable).containsEntry("key1", "value1");
        assertThat(jmxTable).containsEntry("key2------", "value2_(test)------");
    }

    /**
     * Verifies that multiple JMXReporters can be started on the same machine and register metrics
     * at the MBeanServer.
     *
     * @throws Exception if the attribute/mbean could not be found or the test is broken
     */
    @Test
    void testPortConflictHandling() throws Exception {
        final MetricReporter rep1 = new JMXReporter("9020-9035");
        final MetricReporter rep2 = new JMXReporter("9020-9035");

        Gauge<Integer> g1 = () -> 1;
        Gauge<Integer> g2 = () -> 2;

        rep1.notifyOfAddedMetric(g1, "rep1", metricGroup);
        rep2.notifyOfAddedMetric(g2, "rep2", metricGroup);

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName objectName1 =
                new ObjectName(
                        JMX_DOMAIN_PREFIX + "taskmanager.rep1",
                        JMXReporter.generateJmxTable(metricGroup.getAllVariables()));
        ObjectName objectName2 =
                new ObjectName(
                        JMX_DOMAIN_PREFIX + "taskmanager.rep2",
                        JMXReporter.generateJmxTable(metricGroup.getAllVariables()));

        assertThat(mBeanServer.getAttribute(objectName1, "Value")).isEqualTo(1);
        assertThat(mBeanServer.getAttribute(objectName2, "Value")).isEqualTo(2);

        rep1.notifyOfRemovedMetric(g1, "rep1", null);
        rep1.notifyOfRemovedMetric(g2, "rep2", null);
    }

    /** Verifies that we can connect to multiple JMXReporters running on the same machine. */
    @Test
    void testJMXAvailability() throws Exception {
        final JMXReporter rep1 = new JMXReporter("9040-9055");
        final JMXReporter rep2 = new JMXReporter("9040-9055");

        Gauge<Integer> g1 = () -> 1;
        Gauge<Integer> g2 = () -> 2;

        rep1.notifyOfAddedMetric(g1, "rep1", metricGroup);
        rep2.notifyOfAddedMetric(g2, "rep2", metricGroup);

        ObjectName objectName1 =
                new ObjectName(
                        JMX_DOMAIN_PREFIX + "taskmanager.rep1",
                        JMXReporter.generateJmxTable(metricGroup.getAllVariables()));
        ObjectName objectName2 =
                new ObjectName(
                        JMX_DOMAIN_PREFIX + "taskmanager.rep2",
                        JMXReporter.generateJmxTable(metricGroup.getAllVariables()));

        JMXServiceURL url1 =
                new JMXServiceURL(
                        "service:jmx:rmi://localhost:"
                                + rep1.getPort().get()
                                + "/jndi/rmi://localhost:"
                                + rep1.getPort().get()
                                + "/jmxrmi");
        JMXConnector jmxCon1 = JMXConnectorFactory.connect(url1);
        MBeanServerConnection mCon1 = jmxCon1.getMBeanServerConnection();

        assertThat(mCon1.getAttribute(objectName1, "Value")).isEqualTo(1);
        assertThat(mCon1.getAttribute(objectName2, "Value")).isEqualTo(2);

        jmxCon1.close();

        JMXServiceURL url2 =
                new JMXServiceURL(
                        "service:jmx:rmi://localhost:"
                                + rep2.getPort().get()
                                + "/jndi/rmi://localhost:"
                                + rep2.getPort().get()
                                + "/jmxrmi");
        JMXConnector jmxCon2 = JMXConnectorFactory.connect(url2);
        MBeanServerConnection mCon2 = jmxCon2.getMBeanServerConnection();

        assertThat(mCon2.getAttribute(objectName1, "Value")).isEqualTo(1);
        assertThat(mCon2.getAttribute(objectName2, "Value")).isEqualTo(2);

        // JMX Server URL should be identical since we made it static.
        assertThat(url2).isEqualTo(url1);

        rep1.notifyOfRemovedMetric(g1, "rep1", null);
        rep1.notifyOfRemovedMetric(g2, "rep2", null);

        jmxCon2.close();

        rep1.close();
        rep2.close();
    }

    /** Tests that histograms are properly reported via the JMXReporter. */
    @Test
    void testHistogramReporting() throws Exception {
        String histogramName = "histogram";

        final JMXReporter reporter = new JMXReporter(null);

        TestHistogram histogram = new TestHistogram();

        reporter.notifyOfAddedMetric(histogram, histogramName, metricGroup);

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName objectName =
                new ObjectName(
                        JMX_DOMAIN_PREFIX + "taskmanager." + histogramName,
                        JMXReporter.generateJmxTable(metricGroup.getAllVariables()));

        MBeanInfo info = mBeanServer.getMBeanInfo(objectName);

        MBeanAttributeInfo[] attributeInfos = info.getAttributes();

        assertThat(attributeInfos).hasSize(11);

        assertThat(mBeanServer.getAttribute(objectName, "Count")).isEqualTo(histogram.getCount());
        HistogramStatistics statistics = histogram.getStatistics();
        assertThat(mBeanServer.getAttribute(objectName, "Mean")).isEqualTo(statistics.getMean());
        assertThat(mBeanServer.getAttribute(objectName, "StdDev"))
                .isEqualTo(statistics.getStdDev());
        assertThat(mBeanServer.getAttribute(objectName, "Max")).isEqualTo(statistics.getMax());
        assertThat(mBeanServer.getAttribute(objectName, "Min")).isEqualTo(statistics.getMin());
        assertThat(mBeanServer.getAttribute(objectName, "Median"))
                .isEqualTo(statistics.getQuantile(0.5));
        assertThat(mBeanServer.getAttribute(objectName, "75thPercentile"))
                .isEqualTo(statistics.getQuantile(0.75));
        assertThat(mBeanServer.getAttribute(objectName, "95thPercentile"))
                .isEqualTo(statistics.getQuantile(0.95));
        assertThat(mBeanServer.getAttribute(objectName, "98thPercentile"))
                .isEqualTo(statistics.getQuantile(0.98));
        assertThat(mBeanServer.getAttribute(objectName, "99thPercentile"))
                .isEqualTo(statistics.getQuantile(0.99));
        assertThat(mBeanServer.getAttribute(objectName, "999thPercentile"))
                .isEqualTo(statistics.getQuantile(0.999));
    }

    /** Tests that meters are properly reported via the JMXReporter. */
    @Test
    void testMeterReporting() throws Exception {
        String meterName = "meter";

        final JMXReporter reporter = new JMXReporter(null);

        TestMeter meter = new TestMeter();
        reporter.notifyOfAddedMetric(meter, meterName, metricGroup);

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName objectName =
                new ObjectName(
                        JMX_DOMAIN_PREFIX + "taskmanager." + meterName,
                        JMXReporter.generateJmxTable(metricGroup.getAllVariables()));

        MBeanInfo info = mBeanServer.getMBeanInfo(objectName);

        MBeanAttributeInfo[] attributeInfos = info.getAttributes();

        assertThat(attributeInfos).hasSize(2);

        assertThat(mBeanServer.getAttribute(objectName, "Rate")).isEqualTo(meter.getRate());
        assertThat(mBeanServer.getAttribute(objectName, "Count")).isEqualTo(meter.getCount());
    }
}
