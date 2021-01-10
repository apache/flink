/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Integration tests for proper initialization of the job manager metrics. */
public class JobManagerMetricsITCase extends TestLogger {

    private static final String JOB_MANAGER_METRICS_PREFIX = "localhost.jobmanager.";

    private static final BlockerSync sync = new BlockerSync();

    private CheckedThread jobExecuteThread;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Before
    public void setUp() throws Exception {
        jobExecuteThread =
                new CheckedThread() {

                    @Override
                    public void go() throws Exception {
                        StreamExecutionEnvironment env =
                                StreamExecutionEnvironment.getExecutionEnvironment();
                        env.addSource(
                                        new SourceFunction<String>() {

                                            @Override
                                            public void run(SourceContext<String> ctx)
                                                    throws Exception {
                                                sync.block();
                                            }

                                            @Override
                                            public void cancel() {
                                                sync.releaseBlocker();
                                            }
                                        })
                                .addSink(new PrintSinkFunction());

                        env.execute();
                    }
                };

        jobExecuteThread.start();
        sync.awaitBlocker();
    }

    @Test
    public void testJobManagerMetrics() throws Exception {
        assertEquals(1, TestReporter.OPENED_REPORTERS.size());
        TestReporter reporter = TestReporter.OPENED_REPORTERS.iterator().next();

        List<String> expectedPatterns = getExpectedPatterns();

        Collection<String> gaugeNames = reporter.getGauges().values();

        for (String expectedPattern : expectedPatterns) {
            boolean found = false;
            for (String gaugeName : gaugeNames) {
                if (gaugeName.matches(expectedPattern)) {
                    found = true;
                }
            }
            if (!found) {
                fail(
                        String.format(
                                "Failed to find gauge [%s] in registered gauges [%s]",
                                expectedPattern, gaugeNames));
            }
        }

        for (Map.Entry<Gauge<?>, String> entry : reporter.getGauges().entrySet()) {
            if (entry.getValue().contains(MetricNames.TASK_SLOTS_AVAILABLE)) {
                assertEquals(0L, entry.getKey().getValue());
            } else if (entry.getValue().contains(MetricNames.TASK_SLOTS_TOTAL)) {
                assertEquals(1L, entry.getKey().getValue());
            } else if (entry.getValue().contains(MetricNames.NUM_REGISTERED_TASK_MANAGERS)) {
                assertEquals(1L, entry.getKey().getValue());
            } else if (entry.getValue().contains(MetricNames.NUM_RUNNING_JOBS)) {
                assertEquals(1L, entry.getKey().getValue());
            }
        }

        sync.releaseBlocker();
        jobExecuteThread.sync();
    }

    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                "metrics.reporter.test_reporter.class", TestReporter.class.getName());
        return configuration;
    }

    private static List<String> getExpectedPatterns() {
        String[] expectedGauges =
                new String[] {
                    MetricNames.TASK_SLOTS_AVAILABLE,
                    MetricNames.TASK_SLOTS_TOTAL,
                    MetricNames.NUM_REGISTERED_TASK_MANAGERS,
                    MetricNames.NUM_RUNNING_JOBS
                };

        List<String> patterns = new ArrayList<>();
        for (String expectedGauge : expectedGauges) {
            patterns.add(JOB_MANAGER_METRICS_PREFIX + expectedGauge);
        }

        return patterns;
    }

    /** Test metric reporter that exposes registered metrics. */
    public static final class TestReporter extends AbstractReporter {
        public static final Set<TestReporter> OPENED_REPORTERS = ConcurrentHashMap.newKeySet();

        @Override
        public String filterCharacters(String input) {
            return input;
        }

        @Override
        public void open(MetricConfig config) {
            OPENED_REPORTERS.add(this);
        }

        @Override
        public void close() {
            OPENED_REPORTERS.remove(this);
        }

        public Map<Gauge<?>, String> getGauges() {
            return gauges;
        }
    }
}
