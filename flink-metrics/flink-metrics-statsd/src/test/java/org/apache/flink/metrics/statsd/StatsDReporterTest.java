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

package org.apache.flink.metrics.statsd;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.metrics.util.TestMetricGroup;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the StatsDReporter. */
public class StatsDReporterTest extends TestLogger {

    @Test
    public void testReplaceInvalidChars() {
        StatsDReporter reporter = new StatsDReporter();

        assertEquals("", reporter.filterCharacters(""));
        assertEquals("abc", reporter.filterCharacters("abc"));
        assertEquals("a-b--", reporter.filterCharacters("a:b::"));
    }

    /** Tests that the registered metrics' names don't contain invalid characters. */
    @Test
    public void testAddingMetrics() {
        String counterName = "testCounter";

        final String scope = "scope";
        final char delimiter = '_';

        MetricGroup metricGroup =
                TestMetricGroup.newBuilder()
                        .setMetricIdentifierFunction(
                                (metricName, characterFilter) -> scope + delimiter + metricName)
                        .build();

        TestingStatsDReporter reporter = new TestingStatsDReporter();
        reporter.open(new MetricConfig());

        SimpleCounter myCounter = new SimpleCounter();
        reporter.notifyOfAddedMetric(myCounter, counterName, metricGroup);

        Map<Counter, String> counters = reporter.getCounters();

        assertTrue(counters.containsKey(myCounter));

        String expectedCounterName =
                reporter.filterCharacters(scope)
                        + delimiter
                        + reporter.filterCharacters(counterName);

        assertEquals(expectedCounterName, counters.get(myCounter));
    }

    /** Tests that histograms are properly reported via the StatsD reporter. */
    @Test
    public void testStatsDHistogramReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(6);
        expectedLines.add("metric.count:1|g");
        expectedLines.add("metric.mean:4.0|g");
        expectedLines.add("metric.min:7|g");
        expectedLines.add("metric.max:6|g");
        expectedLines.add("metric.stddev:5.0|g");
        expectedLines.add("metric.p75:0.75|g");
        expectedLines.add("metric.p98:0.98|g");
        expectedLines.add("metric.p99:0.99|g");
        expectedLines.add("metric.p999:0.999|g");
        expectedLines.add("metric.p95:0.95|g");
        expectedLines.add("metric.p50:0.5|g");

        testMetricAndAssert(new TestHistogram(), "metric", expectedLines);
    }

    @Test
    public void testStatsDHistogramReportingOfNegativeValues() throws Exception {
        TestHistogram histogram = new TestHistogram();
        histogram.setCount(-101);
        histogram.setMean(-104);
        histogram.setMin(-107);
        histogram.setMax(-106);
        histogram.setStdDev(-105);

        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric.count:0|g");
        expectedLines.add("metric.count:-101|g");
        expectedLines.add("metric.mean:0|g");
        expectedLines.add("metric.mean:-104.0|g");
        expectedLines.add("metric.min:0|g");
        expectedLines.add("metric.min:-107|g");
        expectedLines.add("metric.max:0|g");
        expectedLines.add("metric.max:-106|g");
        expectedLines.add("metric.stddev:0|g");
        expectedLines.add("metric.stddev:-105.0|g");
        expectedLines.add("metric.p75:0.75|g");
        expectedLines.add("metric.p98:0.98|g");
        expectedLines.add("metric.p99:0.99|g");
        expectedLines.add("metric.p999:0.999|g");
        expectedLines.add("metric.p95:0.95|g");
        expectedLines.add("metric.p50:0.5|g");

        testMetricAndAssert(histogram, "metric", expectedLines);
    }

    /** Tests that meters are properly reported via the StatsD reporter. */
    @Test
    public void testStatsDMetersReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(4);
        expectedLines.add("metric.rate:5.0|g");
        expectedLines.add("metric.count:100|g");

        testMetricAndAssert(new TestMeter(), "metric", expectedLines);
    }

    @Test
    public void testStatsDMetersReportingOfNegativeValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric.rate:0|g");
        expectedLines.add("metric.rate:-5.3|g");
        expectedLines.add("metric.count:0|g");
        expectedLines.add("metric.count:-50|g");

        testMetricAndAssert(new TestMeter(-50, -5.3), "metric", expectedLines);
    }

    /** Tests that counter are properly reported via the StatsD reporter. */
    @Test
    public void testStatsDCountersReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(2);
        expectedLines.add("metric:100|g");

        testMetricAndAssert(new TestCounter(100), "metric", expectedLines);
    }

    @Test
    public void testStatsDCountersReportingOfNegativeValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric:0|g");
        expectedLines.add("metric:-51|g");

        testMetricAndAssert(new TestCounter(-51), "metric", expectedLines);
    }

    @Test
    public void testStatsDGaugesReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(2);
        expectedLines.add("metric:75|g");

        testMetricAndAssert((Gauge<Integer>) () -> 75, "metric", expectedLines);
    }

    @Test
    public void testStatsDGaugesReportingOfNegativeValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric:0|g");
        expectedLines.add("metric:-12345|g");

        testMetricAndAssert((Gauge<Integer>) () -> -12345, "metric", expectedLines);
    }

    private void testMetricAndAssert(Metric metric, String metricName, Set<String> expectation)
            throws Exception {
        StatsDReporter reporter = null;
        DatagramSocketReceiver receiver = null;
        Thread receiverThread = null;
        long timeout = 5000;
        long joinTimeout = 30000;

        try {
            receiver = new DatagramSocketReceiver();

            receiverThread = new Thread(receiver);

            receiverThread.start();

            int port = receiver.getPort();

            MetricConfig config = new MetricConfig();
            config.setProperty("host", "localhost");
            config.setProperty("port", String.valueOf(port));

            reporter = new StatsDReporter();
            reporter.open(config);
            MetricGroup metricGroup = new UnregisteredMetricsGroup();

            reporter.notifyOfAddedMetric(metric, metricName, metricGroup);
            reporter.report();

            receiver.waitUntilNumLines(expectation.size(), timeout);
            assertEquals(expectation, receiver.getLines());

        } finally {
            if (reporter != null) {
                reporter.close();
            }

            if (receiver != null) {
                receiver.stop();
            }

            if (receiverThread != null) {
                receiverThread.join(joinTimeout);
            }
        }
    }

    /** Testing StatsDReporter which disables the socket creation. */
    public static class TestingStatsDReporter extends StatsDReporter {
        @Override
        public void open(MetricConfig configuration) {
            // disable the socket creation
        }

        public Map<Counter, String> getCounters() {
            return counters;
        }
    }

    private static class DatagramSocketReceiver implements Runnable {
        private static final Object obj = new Object();

        private final DatagramSocket socket;
        private final ConcurrentHashMap<String, Object> lines;

        private boolean running = true;

        public DatagramSocketReceiver() throws SocketException {
            socket = new DatagramSocket();
            lines = new ConcurrentHashMap<>();
        }

        public int getPort() {
            return socket.getLocalPort();
        }

        public void stop() {
            running = false;
            socket.close();
        }

        public void waitUntilNumLines(int numberLines, long timeout) throws TimeoutException {
            long endTimeout = System.currentTimeMillis() + timeout;
            long remainingTimeout = timeout;

            synchronized (lines) {
                while (numberLines > lines.size() && remainingTimeout > 0) {
                    try {
                        lines.wait(remainingTimeout);
                    } catch (InterruptedException e) {
                        // ignore interruption exceptions
                    }
                }

                remainingTimeout = endTimeout - System.currentTimeMillis();
            }

            if (remainingTimeout <= 0) {
                throw new TimeoutException("Have not received " + numberLines + " in time.");
            }
        }

        public Set<String> getLines() {
            return lines.keySet();
        }

        @Override
        public void run() {
            while (running) {
                try {
                    byte[] buffer = new byte[1024];

                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    socket.receive(packet);

                    String line =
                            new String(
                                    packet.getData(),
                                    0,
                                    packet.getLength(),
                                    ConfigConstants.DEFAULT_CHARSET);

                    lines.put(line, obj);

                    synchronized (lines) {
                        lines.notifyAll();
                    }
                } catch (IOException ex) {
                    // ignore the exceptions
                }
            }
        }
    }
}
