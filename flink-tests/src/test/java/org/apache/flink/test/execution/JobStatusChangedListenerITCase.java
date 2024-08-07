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

package org.apache.flink.test.execution;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.lineage.DefaultLineageDataset;
import org.apache.flink.streaming.api.lineage.DefaultLineageVertex;
import org.apache.flink.streaming.api.lineage.DefaultSourceLineageVertex;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.runtime.execution.DefaultJobCreatedEvent;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for job status changed listener. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JobStatusChangedListenerITCase extends TestLogger {
    private static final int PARALLELISM = 4;
    private static final String SOURCE_DATASET_NAME = "LineageSource";
    private static final String SOURCE_DATASET_NAMESPACE = "source://LineageSource";
    private static final String SINK_DATASET_NAME = "LineageSink";
    private static final String SINK_DATASET_NAMESPACE = "sink://LineageSink";

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    private static List<JobStatusChangedEvent> statusChangedEvents = new ArrayList<>();

    @Before
    public void setup() {
        statusChangedEvents.clear();
    }

    @Test
    public void testJobStatusChangedForSucceededApplication() throws Exception {
        Configuration configuration = createConfiguration();
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration)) {
            List<String> sourceValues = Arrays.asList("a", "b", "c");
            List<String> resultValues = new ArrayList<>();
            try (CloseableIterator<String> iterator =
                    env.fromCollection(sourceValues).executeAndCollect()) {
                while (iterator.hasNext()) {
                    resultValues.add(iterator.next());
                }
            }
            assertThat(resultValues).containsExactlyInAnyOrder(sourceValues.toArray(new String[0]));
        }

        verifyEventMetaData();
        statusChangedEvents.forEach(
                event -> {
                    if (event instanceof DefaultJobExecutionStatusEvent) {
                        JobExecutionStatusEvent status = (JobExecutionStatusEvent) event;
                        assertThat(
                                        (status.oldStatus() == JobStatus.CREATED
                                                        && status.newStatus() == JobStatus.RUNNING)
                                                || (status.oldStatus() == JobStatus.RUNNING
                                                        && status.newStatus()
                                                                == JobStatus.FINISHED))
                                .isTrue();
                    } else {
                        DefaultJobCreatedEvent createdEvent = (DefaultJobCreatedEvent) event;
                        assertThat(createdEvent.executionMode())
                                .isEqualTo(RuntimeExecutionMode.STREAMING);
                    }
                });
    }

    @Test
    public void testJobStatusChangedForFailedApplication() throws Exception {
        Configuration configuration = createConfiguration();

        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration)) {
            env.setParallelism(PARALLELISM);
            env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
            env.addSource(new FastFailureSourceFunction()).addSink(new SleepingSink());

            StreamGraph streamGraph = env.getStreamGraph();
            JobGraph jobGraph = streamGraph.getJobGraph();

            ClusterClient<?> client = MINI_CLUSTER.getClusterClient();
            JobID jobID = client.submitJob(jobGraph).get();
            while (!client.getJobStatus(jobID).get().equals(JobStatus.FAILED)) {}
        } catch (Exception e) {
            // Expected failure due to exception.
        }

        verifyEventMetaData();
        statusChangedEvents.forEach(
                event -> {
                    JobExecutionStatusEvent status = (JobExecutionStatusEvent) event;
                    assertThat(
                                    (status.oldStatus() == JobStatus.CREATED
                                                    && status.newStatus() == JobStatus.RUNNING)
                                            || (status.oldStatus() == JobStatus.RUNNING
                                                    && status.newStatus() == JobStatus.FAILING)
                                            || (status.oldStatus() == JobStatus.FAILING
                                                    && status.newStatus() == JobStatus.FAILED))
                            .isTrue();
                });
    }

    @Test
    public void testJobStatusChangedForCancelledApplication() throws Exception {
        Configuration configuration = createConfiguration();

        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration)) {
            final DataStream<Long> source = env.addSource(new InfiniteLongSourceFunction());
            source.addSink(new SleepingSink());

            StreamGraph streamGraph = env.getStreamGraph();
            JobGraph jobGraph = streamGraph.getJobGraph();

            verifyLineageGraph(streamGraph.getLineageGraph());
            ClusterClient<?> client = MINI_CLUSTER.getClusterClient();
            JobID jobID = client.submitJob(jobGraph).get();
            waitForAllTaskRunning(MINI_CLUSTER.getMiniCluster(), jobID, false);

            Thread.sleep(100);
            client.cancel(jobID).get();
            while (!client.getJobStatus(jobID).get().equals(JobStatus.CANCELED)) {}
        }

        verifyEventMetaData();
        statusChangedEvents.forEach(
                event -> {
                    JobExecutionStatusEvent status = (JobExecutionStatusEvent) event;
                    assertThat(
                                    (status.oldStatus() == JobStatus.CREATED
                                                    && status.newStatus() == JobStatus.RUNNING)
                                            || (status.oldStatus() == JobStatus.RUNNING
                                                    && status.newStatus() == JobStatus.CANCELLING)
                                            || (status.oldStatus() == JobStatus.CANCELLING
                                                    && status.newStatus() == JobStatus.CANCELED))
                            .isTrue();

                    if (event instanceof JobCreatedEvent) {
                        LineageGraph lineageGraph = ((JobCreatedEvent) event).lineageGraph();
                        assertThat(lineageGraph.sources().size()).isEqualTo(1);
                        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
                    }
                });
    }

    void verifyLineageGraph(LineageGraph lineageGraph) {
        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).boundedness())
                .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(lineageGraph.sources().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).name())
                .isEqualTo(SOURCE_DATASET_NAME);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).namespace())
                .isEqualTo(SOURCE_DATASET_NAMESPACE);

        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).name())
                .isEqualTo(SINK_DATASET_NAME);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).namespace())
                .isEqualTo(SINK_DATASET_NAMESPACE);

        assertThat(lineageGraph.relations().size()).isEqualTo(1);
    }

    void verifyEventMetaData() {
        assertThat(statusChangedEvents.size()).isEqualTo(3);
        assertThat(statusChangedEvents.get(0).jobId())
                .isEqualTo(statusChangedEvents.get(1).jobId());
        assertThat(statusChangedEvents.get(0).jobName())
                .isEqualTo(statusChangedEvents.get(1).jobName());

        assertThat(statusChangedEvents.get(1).jobId())
                .isEqualTo(statusChangedEvents.get(2).jobId());
        assertThat(statusChangedEvents.get(1).jobName())
                .isEqualTo(statusChangedEvents.get(2).jobName());
    }

    /** Testing job status changed listener factory. */
    public static class TestingJobStatusChangedListenerFactory
            implements JobStatusChangedListenerFactory {

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return new TestingJobStatusChangedListener();
        }
    }

    /** Testing job status changed listener. */
    private static class TestingJobStatusChangedListener implements JobStatusChangedListener {

        @Override
        public void onEvent(JobStatusChangedEvent event) {
            statusChangedEvents.add(event);
        }
    }

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                JOB_STATUS_CHANGED_LISTENERS,
                Collections.singletonList(TestingJobStatusChangedListenerFactory.class.getName()));

        return configuration;
    }

    private static class FastFailureSourceFunction implements SourceFunction<Long> {

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            throw new RuntimeException("Failed to execute.");
        }

        @Override
        public void cancel() {}
    }

    private static class InfiniteLongSourceFunction
            implements SourceFunction<Long>, LineageVertexProvider {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long next = 0;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next++);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE, new HashMap<>());
            DefaultSourceLineageVertex lineageVertex =
                    new DefaultSourceLineageVertex(Boundedness.CONTINUOUS_UNBOUNDED);
            lineageVertex.addDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class SleepingSink implements SinkFunction<Long>, LineageVertexProvider {
        @Override
        public void invoke(Long value, Context context) throws Exception {
            Thread.sleep(1_000);
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            SINK_DATASET_NAME, SINK_DATASET_NAMESPACE, new HashMap<>());
            DefaultLineageVertex lineageVertex = new DefaultLineageVertex();
            lineageVertex.addLineageDataset(lineageDataset);
            return lineageVertex;
        }
    }
}
