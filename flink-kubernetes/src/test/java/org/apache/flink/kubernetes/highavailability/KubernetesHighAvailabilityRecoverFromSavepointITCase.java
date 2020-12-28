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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.KubernetesResource;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for recovering from savepoint when Kubernetes HA is enabled. The savepoint will be
 * persisted as a checkpoint and stored in the ConfigMap when recovered successfully.
 */
public class KubernetesHighAvailabilityRecoverFromSavepointITCase extends TestLogger {

    private static final long TIMEOUT = 60 * 1000;

    private static final String CLUSTER_ID = "flink-on-k8s-cluster-" + System.currentTimeMillis();

    private static final String FLAT_MAP_UID = "my-flat-map";

    @ClassRule public static KubernetesResource kubernetesResource = new KubernetesResource();

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private FlinkKubeClient flinkKubeClient;

    private ClusterClient<?> clusterClient;

    private String savepointPath;

    @Before
    public void setup() throws Exception {
        clusterClient = miniClusterResource.getClusterClient();
        flinkKubeClient = kubernetesResource.getFlinkKubeClient();
        savepointPath = temporaryFolder.newFolder("savepoints").getAbsolutePath();
    }

    @After
    public void teardown() throws Exception {
        flinkKubeClient
                .deleteConfigMapsByLabels(
                        KubernetesUtils.getConfigMapLabels(
                                CLUSTER_ID, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY))
                .get();
    }

    @Test
    public void testRecoverFromSavepoint() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        clusterClient.submitJob(jobGraph).get(TIMEOUT, TimeUnit.MILLISECONDS);

        // Wait until all tasks running and getting a successful savepoint
        CommonTestUtils.waitUntilCondition(
                () -> triggerSavepoint(clusterClient, jobGraph.getJobID(), savepointPath) != null,
                Deadline.fromNow(Duration.ofMillis(TIMEOUT)),
                1000);

        // Trigger savepoint 2
        final String savepoint2Path =
                triggerSavepoint(clusterClient, jobGraph.getJobID(), savepointPath);

        // Cancel the old job
        clusterClient.cancel(jobGraph.getJobID());
        CommonTestUtils.waitUntilCondition(
                () -> clusterClient.getJobStatus(jobGraph.getJobID()).get() == JobStatus.CANCELED,
                Deadline.fromNow(Duration.ofMillis(TIMEOUT)),
                1000);

        // Start a new job with savepoint 2
        final JobGraph jobGraphWithSavepoint = createJobGraph();
        final JobID jobId = jobGraphWithSavepoint.getJobID();
        jobGraphWithSavepoint.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepoint2Path));
        clusterClient.submitJob(jobGraphWithSavepoint).get(TIMEOUT, TimeUnit.MILLISECONDS);
        CommonTestUtils.waitUntilCondition(
                () -> clusterClient.getJobStatus(jobId).get() == JobStatus.RUNNING,
                Deadline.fromNow(Duration.ofMillis(TIMEOUT)),
                1000);

        // The savepoint 2 should be added to jobmanager leader ConfigMap
        final String jobManagerConfigMapName = CLUSTER_ID + "-" + jobId + "-jobmanager-leader";
        final Optional<KubernetesConfigMap> optional =
                flinkKubeClient.getConfigMap(jobManagerConfigMapName);
        assertThat(optional.isPresent(), is(true));
        final String checkpointIdKey =
                KubernetesCheckpointStoreUtil.INSTANCE.checkpointIDToName(2L);
        assertThat(optional.get().getData().get(checkpointIdKey), is(notNullValue()));
        assertThat(optional.get().getData().get(Constants.CHECKPOINT_COUNTER_KEY), is("3"));
    }

    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
        configuration.set(
                HighAvailabilityOptions.HA_MODE,
                KubernetesHaServicesFactory.class.getCanonicalName());
        try {
            configuration.set(
                    HighAvailabilityOptions.HA_STORAGE_PATH,
                    temporaryFolder.newFolder().getAbsolutePath());
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to create HA storage", e);
        }
        return configuration;
    }

    private String triggerSavepoint(ClusterClient<?> clusterClient, JobID jobID, String path) {
        try {
            return String.valueOf(
                    clusterClient
                            .triggerSavepoint(jobID, path)
                            .get(TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception ex) {
            // ignore
        }
        return null;
    }

    private JobGraph createJobGraph() throws Exception {
        final StreamExecutionEnvironment sEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        final StateBackend stateBackend =
                new FsStateBackend(temporaryFolder.newFolder().toURI(), 1);
        sEnv.setStateBackend(stateBackend);

        sEnv.addSource(new InfiniteSourceFunction())
                .keyBy(e -> e)
                .flatMap(
                        new RichFlatMapFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;
                            ValueState<Integer> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                ValueStateDescriptor<Integer> descriptor =
                                        new ValueStateDescriptor<>("total", Types.INT);
                                state = getRuntimeContext().getState(descriptor);
                            }

                            @Override
                            public void flatMap(Integer value, Collector<Integer> out)
                                    throws Exception {
                                final Integer current = state.value();
                                if (current != null) {
                                    value += current;
                                }

                                state.update(value);
                                out.collect(value);
                            }
                        })
                .uid(FLAT_MAP_UID)
                .addSink(new DiscardingSink<>());

        return sEnv.getStreamGraph().getJobGraph();
    }

    private static final class InfiniteSourceFunction extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            final Random random = new Random();
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(random.nextInt());
                }

                Thread.sleep(5L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
