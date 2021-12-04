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

package org.apache.flink.metrics.dogstatsd;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the DogStatsDReporter. */
public class DogStatsDReporterTest extends TestLogger {

    @Test
    public void testReplaceInvalidChars() {
        DogStatsDReporter reporter = new DogStatsDReporter();
        String triggerWindowName =
                "TriggerWindow(TumblingProcessingTimeWindows(5000), ReducingStateDescriptor{serializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@f3395ffa, reduceFunction=org.apache.flink.streaming.examples.socket.SocketWindowWordCount$1@4201c465}, ProcessingTimeTrigger(), WindowedStream.reduce(WindowedStream.java-301))";
        String triggerWindowAltInstance =
                triggerWindowName
                        .replace("@f3395ffa", "@f3395ffb")
                        .replace("@4201c465", "@564c1024");

        assertEquals("", reporter.filterCharacters(""));
        assertEquals("abc", reporter.filterCharacters("abc"));
        assertEquals("a_b", reporter.filterCharacters("a:b::"));
        assertEquals("metric_name", reporter.filterCharacters(" (metric -> name) "));
        assertEquals("TriggerWin_c2910b88", reporter.filterCharacters(triggerWindowName));
        assertEquals("TriggerWin_c2910b88", reporter.filterCharacters(triggerWindowAltInstance));
    }

    @Test
    public void testShortIds() throws Exception {
        Configuration config = new Configuration();
        config.setString(MetricOptions.REPORTERS_LIST, "test");
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                DogStatsDReporter.class.getName());
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.shortids", "true");
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
        config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + 8125);

        MetricRegistryImpl metricRegistry = createRegistry(config);

        DogStatsDReporter reporter = (DogStatsDReporter) metricRegistry.getReporters().get(0);

        assertEquals("0c3d7952", reporter.filterCharacters("0c3d7952a3c76c029ecf80ef239d475b"));
        assertEquals("ad25ca60", reporter.filterCharacters("ad25ca6074f1ec1a20030e9b9e11c476"));
        assertEquals(
                "this_isn_t_an_id_but_same_length",
                reporter.filterCharacters("this_isn_t_an_id_but_same_length"));

        metricRegistry.shutdown().get();
    }

    /** Tests that the registered metrics' names don't contain invalid characters. */
    @Test
    public void testAddingMetrics() throws Exception {
        Configuration configuration = new Configuration();
        String taskName = "testTask";
        String jobName = "testJob:-!ax..?";
        String hostname = "local::host:";
        String taskManagerId = "tas:kMana::ger";
        String operatorName = "operator";
        String taskManager = "taskmanager";
        int subtaskIndex = 0;
        String counterName = "testCounter";

        configuration.setString(MetricOptions.REPORTERS_LIST, "test");
        configuration.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                "org.apache.flink.metrics.dogstatsd.DogStatsDReporterTest$TestingStatsDReporter");
        configuration.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
        configuration.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + 8125);

        configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");
        configuration.setString(MetricOptions.SCOPE_DELIMITER, "_");

        MetricRegistryImpl metricRegistry = createRegistry(configuration);

        char delimiter = metricRegistry.getDelimiter();

        InternalOperatorMetricGroup operatorGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                metricRegistry, hostname, new ResourceID(taskManagerId))
                        .addJob(new JobID(), jobName)
                        .addTask(
                                new JobVertexID(),
                                new ExecutionAttemptID(),
                                taskName,
                                subtaskIndex,
                                0)
                        .getOrAddOperator(operatorName);

        SimpleCounter myCounter = new SimpleCounter();

        operatorGroup.counter(counterName, myCounter);

        List<MetricReporter> reporters = metricRegistry.getReporters();

        assertEquals(1, reporters.size());

        MetricReporter metricReporter = reporters.get(0);

        assertTrue(
                "Reporter should be of type DogStatsDReporter",
                metricReporter instanceof DogStatsDReporter);

        TestingDogStatsDReporter reporter = (TestingDogStatsDReporter) metricReporter;

        Map<Counter, DogStatsDReporter.DMetric> counters = reporter.getCounters();

        assertTrue(counters.containsKey(myCounter));

        String expectedCounterName =
                reporter.filterCharacters(hostname)
                        + delimiter
                        + taskManager
                        + delimiter
                        + reporter.filterCharacters(taskManagerId)
                        + delimiter
                        + reporter.filterCharacters(jobName)
                        + delimiter
                        + operatorName
                        + delimiter
                        + subtaskIndex
                        + delimiter
                        + reporter.filterCharacters(counterName);

        assertEquals(expectedCounterName, counters.get(myCounter).getName());

        metricRegistry.shutdown().get();
    }

    /** Tests that statsd lines are valid. */
    @Test
    public void testIgnoreInvalidValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric:1.23|g");

        testMetricAndAssert((Gauge) () -> "n/a", "metric", new HashSet<>());
        testMetricAndAssert((Gauge) () -> Double.NaN, "metric", new HashSet<>());
        testMetricAndAssert((Gauge) () -> 1.23, "metric", expectedLines);
    }

    @Test
    public void testTags() throws Exception {
        MetricRegistryImpl registry = null;
        DatagramSocketReceiver receiver = null;
        Thread receiverThread = null;
        long timeout = 5000;
        long joinTimeout = 30000;

        try {
            receiver = new DatagramSocketReceiver();

            receiverThread = new Thread(receiver);

            receiverThread.start();

            int port = receiver.getPort();

            Configuration config = new Configuration();
            config.setString(MetricOptions.REPORTERS_LIST, "test");
            config.setString(
                    ConfigConstants.METRICS_REPORTER_PREFIX
                            + "test."
                            + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                    DogStatsDReporter.class.getName());
            config.setString(
                    ConfigConstants.METRICS_REPORTER_PREFIX
                            + "test."
                            + ConfigConstants.METRICS_REPORTER_INTERVAL_SUFFIX,
                    "1 SECONDS");
            config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.host", "localhost");
            config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.port", "" + port);
            config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test.shortids", "true");

            registry = createRegistry(config, new DogStatsDReporter());

            JobID jobID = new JobID();
            JobVertexID taskId = new JobVertexID();
            ExecutionAttemptID attemptId = new ExecutionAttemptID();
            OperatorID operatorID = new OperatorID();

            InternalOperatorMetricGroup operatorGroup =
                    TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                    registry, "hostName", new ResourceID("taskManagerId"))
                            .addJob(jobID, "jobName")
                            .addTask(taskId, attemptId, "taskName", 1, 2)
                            .getOrAddOperator(operatorID, "operatorName");

            Gauge<Double> sample =
                    new Gauge<Double>() {
                        @Override
                        public Double getValue() {
                            return 1.23;
                        }
                    };

            operatorGroup.gauge("sample", sample);

            receiver.waitUntilNumLines(10, timeout);
            Set<String> lines = receiver.getLines();

            Map<String, String> expectedTags = new HashMap<>();
            expectedTags.put("host", "hostName");
            expectedTags.put("job_id", jobID.toString().substring(0, 8));
            expectedTags.put("job_name", "jobName");
            expectedTags.put("operator_name", "operatorName");
            expectedTags.put("subtask_index", "1");
            expectedTags.put("task_attempt_id", attemptId.toString().substring(0, 8));
            expectedTags.put("task_attempt_num", "2");
            expectedTags.put("task_id", taskId.toString().substring(0, 8));
            expectedTags.put("task_name", "taskName");
            expectedTags.put("tm_id", "taskManagerId");

            String expectedName =
                    "hostName.taskmanager.taskManagerId.jobName.operatorName.1.sample";

            Boolean gotName = false;
            for (String line : lines) {
                if (line.startsWith(expectedName + ':')) {
                    gotName = true;
                    for (Map.Entry<String, String> tag : expectedTags.entrySet()) {
                        String tagLine = tag.getKey() + ':' + tag.getValue();
                        assertTrue(
                                "expecting to find " + tagLine + " in " + line,
                                line.contains(tagLine));
                    }
                    break;
                }
            }
            assertTrue("expecting to find " + expectedName, gotName);

        } finally {
            if (registry != null) {
                registry.shutdown().get();
            }

            if (receiver != null) {
                receiver.stop();
            }

            if (receiverThread != null) {
                receiverThread.join(joinTimeout);
            }
        }
    }

    /** Tests that histograms are properly reported via the StatsD reporter. */
    @Test
    public void testHistogramReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(6);
        expectedLines.add("metric.count:1|c");
        expectedLines.add("metric.avg:4.0|g");
        expectedLines.add("metric.min:7|g");
        expectedLines.add("metric.max:6|g");
        expectedLines.add("metric.95percentile:0.95|g");
        expectedLines.add("metric.median:0.5|g");

        testMetricAndAssert(new TestHistogram(), "metric", expectedLines);
    }

    @Test
    public void testHistogramReportingOfNegativeValues() throws Exception {
        TestHistogram histogram = new TestHistogram();
        histogram.setCount(-101);
        histogram.setMean(-104);
        histogram.setMin(-107);
        histogram.setMax(-106);
        histogram.setStdDev(-105);

        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric.count:-101|c");
        expectedLines.add("metric.avg:-104.0|g");
        expectedLines.add("metric.min:-107|g");
        expectedLines.add("metric.max:-106|g");
        expectedLines.add("metric.95percentile:0.95|g");
        expectedLines.add("metric.median:0.5|g");

        testMetricAndAssert(histogram, "metric", expectedLines);
    }

    /** Tests that meters are properly reported via the StatsD reporter. */
    @Test
    public void testMetersReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(4);
        expectedLines.add("metric.rate:5.0|g");
        expectedLines.add("metric.count:100|c");

        testMetricAndAssert(new TestMeter(), "metric", expectedLines);
    }

    @Test
    public void testMetersReportingOfNegativeValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric.rate:-5.3|g");
        expectedLines.add("metric.count:-50|c");

        testMetricAndAssert(new TestMeter(-50, -5.3), "metric", expectedLines);
    }

    /** Tests that counter are properly reported via the StatsD reporter. */
    @Test
    public void testCountersReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(2);
        expectedLines.add("metric:100|c");

        testMetricAndAssert(new TestCounter(100), "metric", expectedLines);
    }

    @Test
    public void testCountersReportingOfNegativeValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric:-51|c");

        testMetricAndAssert(new TestCounter(-51), "metric", expectedLines);
    }

    @Test
    public void testGaugesReporting() throws Exception {
        Set<String> expectedLines = new HashSet<>(2);
        expectedLines.add("metric:75|g");

        testMetricAndAssert((Gauge) () -> 75, "metric", expectedLines);
    }

    @Test
    public void testGaugesReportingOfNegativeValues() throws Exception {
        Set<String> expectedLines = new HashSet<>();
        expectedLines.add("metric:-12345|g");

        testMetricAndAssert((Gauge) () -> -12345, "metric", expectedLines);
    }

    private MetricRegistryImpl createRegistry(Configuration config) {
        return createRegistry(config, new TestingDogStatsDReporter());
    }

    private MetricRegistryImpl createRegistry(Configuration config, MetricReporter metricReporter) {
        String reporterName = "test";

        MetricConfig metricConfig = new MetricConfig();
        DelegatingConfiguration delegatingConfiguration =
                new DelegatingConfiguration(
                        config, ConfigConstants.METRICS_REPORTER_PREFIX + reporterName + '.');
        delegatingConfiguration.addAllToProperties(metricConfig);

        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(config, 10485760),
                Collections.singletonList(
                        ReporterSetup.forReporter(reporterName, metricConfig, metricReporter)));
    }

    private void testMetricAndAssert(Metric metric, String metricName, Set<String> expectation)
            throws Exception {
        DogStatsDReporter reporter = null;
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

            reporter = new DogStatsDReporter();
            ReporterSetup.forReporter("test", config, reporter);
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
    public static class TestingDogStatsDReporter extends DogStatsDReporter {
        @Override
        protected void establishConnection(String host, Integer port) {
            // disable the socket creation
        }

        public Map<Counter, DMetric> getCounters() {
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

                    remainingTimeout = endTimeout - System.currentTimeMillis();
                }
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
