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
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;

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
import static org.junit.Assert.assertEquals;

/** Tests for the JMXReporter. */
public class JMXReporterTest extends TestLogger {

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

    @After
    public void shutdownService() throws IOException {
        JMXService.stopInstance();
    }

    @Test
    public void testReplaceInvalidChars() {
        assertEquals("", JMXReporter.replaceInvalidChars(""));
        assertEquals("abc", JMXReporter.replaceInvalidChars("abc"));
        assertEquals("abc", JMXReporter.replaceInvalidChars("abc\""));
        assertEquals("abc", JMXReporter.replaceInvalidChars("\"abc"));
        assertEquals("abc", JMXReporter.replaceInvalidChars("\"abc\""));
        assertEquals("abc", JMXReporter.replaceInvalidChars("\"a\"b\"c\""));
        assertEquals("", JMXReporter.replaceInvalidChars("\"\"\"\""));
        assertEquals("____", JMXReporter.replaceInvalidChars("    "));
        assertEquals("ab_-(c)-", JMXReporter.replaceInvalidChars("\"ab ;(c)'"));
        assertEquals("a_b_c", JMXReporter.replaceInvalidChars("a b c"));
        assertEquals("a_b_c_", JMXReporter.replaceInvalidChars("a b c "));
        assertEquals("a-b-c-", JMXReporter.replaceInvalidChars("a;b'c*"));
        assertEquals("a------b------c", JMXReporter.replaceInvalidChars("a,=;:?'b,=;:?'c"));
    }

    /** Verifies that the JMXReporter properly generates the JMX table. */
    @Test
    public void testGenerateTable() {
        Map<String, String> vars = new HashMap<>();
        vars.put("key0", "value0");
        vars.put("key1", "value1");
        vars.put("\"key2,=;:?'", "\"value2 (test),=;:?'");

        Hashtable<String, String> jmxTable = JMXReporter.generateJmxTable(vars);

        assertEquals("value0", jmxTable.get("key0"));
        assertEquals("value1", jmxTable.get("key1"));
        assertEquals("value2_(test)------", jmxTable.get("key2------"));
    }

    /**
     * Verifies that multiple JMXReporters can be started on the same machine and register metrics
     * at the MBeanServer.
     *
     * @throws Exception if the attribute/mbean could not be found or the test is broken
     */
    @Test
    public void testPortConflictHandling() throws Exception {
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

        assertEquals(1, mBeanServer.getAttribute(objectName1, "Value"));
        assertEquals(2, mBeanServer.getAttribute(objectName2, "Value"));

        rep1.notifyOfRemovedMetric(g1, "rep1", null);
        rep1.notifyOfRemovedMetric(g2, "rep2", null);
    }

    /** Verifies that we can connect to multiple JMXReporters running on the same machine. */
    @Test
    public void testJMXAvailability() throws Exception {
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

        assertEquals(1, mCon1.getAttribute(objectName1, "Value"));
        assertEquals(2, mCon1.getAttribute(objectName2, "Value"));

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

        assertEquals(1, mCon2.getAttribute(objectName1, "Value"));
        assertEquals(2, mCon2.getAttribute(objectName2, "Value"));

        // JMX Server URL should be identical since we made it static.
        assertEquals(url1, url2);

        rep1.notifyOfRemovedMetric(g1, "rep1", null);
        rep1.notifyOfRemovedMetric(g2, "rep2", null);

        jmxCon2.close();

        rep1.close();
        rep2.close();
    }

    /** Tests that histograms are properly reported via the JMXReporter. */
    @Test
    public void testHistogramReporting() throws Exception {
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

        assertEquals(11, attributeInfos.length);

        assertEquals(histogram.getCount(), mBeanServer.getAttribute(objectName, "Count"));
        HistogramStatistics statistics = histogram.getStatistics();
        assertEquals(statistics.getMean(), mBeanServer.getAttribute(objectName, "Mean"));
        assertEquals(statistics.getStdDev(), mBeanServer.getAttribute(objectName, "StdDev"));
        assertEquals(statistics.getMax(), mBeanServer.getAttribute(objectName, "Max"));
        assertEquals(statistics.getMin(), mBeanServer.getAttribute(objectName, "Min"));
        assertEquals(statistics.getQuantile(0.5), mBeanServer.getAttribute(objectName, "Median"));
        assertEquals(
                statistics.getQuantile(0.75),
                mBeanServer.getAttribute(objectName, "75thPercentile"));
        assertEquals(
                statistics.getQuantile(0.95),
                mBeanServer.getAttribute(objectName, "95thPercentile"));
        assertEquals(
                statistics.getQuantile(0.98),
                mBeanServer.getAttribute(objectName, "98thPercentile"));
        assertEquals(
                statistics.getQuantile(0.99),
                mBeanServer.getAttribute(objectName, "99thPercentile"));
        assertEquals(
                statistics.getQuantile(0.999),
                mBeanServer.getAttribute(objectName, "999thPercentile"));
    }

    /** Tests that meters are properly reported via the JMXReporter. */
    @Test
    public void testMeterReporting() throws Exception {
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

        assertEquals(2, attributeInfos.length);

        assertEquals(meter.getRate(), mBeanServer.getAttribute(objectName, "Rate"));
        assertEquals(meter.getCount(), mBeanServer.getAttribute(objectName, "Count"));
    }
}
