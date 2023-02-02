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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendTestUtils.SerializableFunctionWithException;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.testutils.ExceptionallyDoneFuture;
import org.apache.flink.state.common.ChangelogMaterializationMetricGroup;
import org.apache.flink.state.common.PeriodicMaterializationManager;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.runtime.state.StateBackendTestUtils.wrapStateBackendWithSnapshotFunction;
import static org.apache.flink.state.changelog.ChangelogStateBackendMetricGroup.LATEST_FULL_SIZE_OF_MATERIALIZATION;
import static org.apache.flink.state.changelog.ChangelogStateBackendMetricGroup.LATEST_FULL_SIZE_OF_NON_MATERIALIZATION;
import static org.apache.flink.state.changelog.ChangelogStateBackendMetricGroup.LATEST_INC_SIZE_OF_MATERIALIZATION;
import static org.apache.flink.state.changelog.ChangelogStateBackendMetricGroup.LATEST_INC_SIZE_OF_NON_MATERIALIZATION;
import static org.apache.flink.state.common.ChangelogMaterializationMetricGroup.COMPLETED_MATERIALIZATION;
import static org.apache.flink.state.common.ChangelogMaterializationMetricGroup.FAILED_MATERIALIZATION;
import static org.apache.flink.state.common.ChangelogMaterializationMetricGroup.LAST_DURATION_OF_MATERIALIZATION;
import static org.apache.flink.state.common.ChangelogMaterializationMetricGroup.STARTED_MATERIALIZATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Test the {@link MetricGroup} Changelog used. e.g.{@link ChangelogStateBackendMetricGroup}, {@link
 * ChangelogMaterializationMetricGroup}
 */
class ChangelogMetricGroupTest {

    private ChangelogKeyedStateBackend<Integer> changelogKeyedStateBackend;
    private PeriodicMaterializationManager periodicMaterializationManager;
    private ValueState<Integer> state;

    private Counter startedMaterializationCounter;
    private Counter completedMaterializationCounter;
    private Counter failedMaterializationCounter;
    private Gauge<Long> lastDurationOfMaterializationGauge;
    private Gauge<Long> lastFullSizeOfMaterializationGauge;
    private Gauge<Long> lastIncSizeOfMaterializationGauge;
    private Gauge<Long> lastFullSizeOfNonMaterializationGauge;
    private Gauge<Long> lastIncSizeOfNonMaterializationGauge;

    @Test
    void testCompletedMaterialization() throws Exception {
        setup(snapshotResult -> snapshotResult);

        // The materialization will be skipped if no data updated.
        assertThat(lastDurationOfMaterializationGauge.getValue().longValue()).isEqualTo(-1L);
        periodicMaterializationManager.triggerMaterialization();
        runSnapshot(1L);
        assertThat(startedMaterializationCounter.getCount()).isEqualTo(1L);
        assertThat(completedMaterializationCounter.getCount()).isEqualTo(1L);
        assertThat(lastDurationOfMaterializationGauge.getValue().longValue()).isNotEqualTo(-1L);
        assertThat(lastFullSizeOfMaterializationGauge.getValue().longValue()).isEqualTo(0L);
        assertThat(lastIncSizeOfMaterializationGauge.getValue().longValue()).isEqualTo(0L);
        assertThat(lastFullSizeOfNonMaterializationGauge.getValue().longValue()).isEqualTo(0L);
        assertThat(lastIncSizeOfNonMaterializationGauge.getValue().longValue()).isEqualTo(0L);
        changelogKeyedStateBackend.setCurrentKey(1);
        state.update(1);
        periodicMaterializationManager.triggerMaterialization();
        runSnapshot(2L);
        assertThat(startedMaterializationCounter.getCount()).isEqualTo(2L);
        assertThat(completedMaterializationCounter.getCount()).isEqualTo(2L);
        Long lastFullSizeOfMaterialization = lastFullSizeOfMaterializationGauge.getValue();
        Long lastIncSizeOfMaterialization = lastIncSizeOfMaterializationGauge.getValue();
        Long lastFullSizeOfNonMaterialization = lastFullSizeOfNonMaterializationGauge.getValue();
        Long lastIncSizeOfNonMaterialization = lastIncSizeOfNonMaterializationGauge.getValue();
        assertNotEquals(0L, lastFullSizeOfMaterialization.longValue());
        assertNotEquals(0L, lastIncSizeOfMaterialization.longValue());
        assertNotEquals(-1L, lastDurationOfMaterializationGauge.getValue().longValue());
        // The non-materialization size will be zero if no data updated between completed
        // materialization and checkpoint.
        assertThat(lastFullSizeOfNonMaterialization.longValue()).isEqualTo(0L);
        assertThat(lastIncSizeOfNonMaterialization.longValue()).isEqualTo(0L);
        changelogKeyedStateBackend.setCurrentKey(2);
        state.update(2);
        runSnapshot(3L);
        // The materialization size will not be updated if no materialization triggered.
        assertThat(lastFullSizeOfMaterializationGauge.getValue())
                .isEqualTo(lastFullSizeOfMaterialization);
        assertThat(lastIncSizeOfMaterializationGauge.getValue())
                .isEqualTo(lastIncSizeOfMaterialization);
        assertThat(lastFullSizeOfNonMaterializationGauge.getValue())
                .isNotEqualTo(lastFullSizeOfNonMaterialization);
        assertThat(lastFullSizeOfNonMaterializationGauge.getValue())
                .isNotEqualTo(lastIncSizeOfNonMaterialization);
        assertThat(failedMaterializationCounter.getCount()).isEqualTo(0L);
    }

    @Test
    void testFailedMaterialization() throws Exception {
        setup(snapshotResult -> ExceptionallyDoneFuture.of(new RuntimeException()));
        changelogKeyedStateBackend.setCurrentKey(1);
        state.update(1);
        assertThat(lastDurationOfMaterializationGauge.getValue().longValue()).isEqualTo(-1L);
        periodicMaterializationManager.triggerMaterialization();
        runSnapshot(1L);
        assertThat(completedMaterializationCounter.getCount()).isEqualTo(0L);
        assertThat(failedMaterializationCounter.getCount()).isEqualTo(1L);
        assertThat(startedMaterializationCounter.getCount()).isEqualTo(1L);
        assertThat(lastDurationOfMaterializationGauge.getValue().longValue()).isEqualTo(-1L);
        assertThat(lastFullSizeOfMaterializationGauge.getValue().longValue()).isEqualTo(0L);
        assertThat(lastIncSizeOfMaterializationGauge.getValue().longValue()).isEqualTo(0L);
        assertThat(lastFullSizeOfNonMaterializationGauge.getValue().longValue()).isNotEqualTo(0L);
        assertThat(lastIncSizeOfNonMaterializationGauge.getValue().longValue()).isNotEqualTo(0L);
    }

    @SuppressWarnings("unchecked")
    private void setup(
            SerializableFunctionWithException<RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                    snapshotResultFunction)
            throws Exception {
        Map<String, Counter> counterMap = new HashMap<>();
        Map<String, Gauge<?>> gaugeMap = new HashMap<>();
        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <C extends Counter> C counter(String name, C counter) {
                        counterMap.put(name, counter);
                        return counter;
                    }

                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        gaugeMap.put(name, gauge);
                        return gauge;
                    }
                };
        changelogKeyedStateBackend =
                createChangelogKeyedStateBackend(metricGroup, snapshotResultFunction);
        periodicMaterializationManager =
                periodicMaterializationManager(changelogKeyedStateBackend, metricGroup);
        state =
                changelogKeyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("id", Integer.class));
        startedMaterializationCounter =
                Preconditions.checkNotNull(counterMap.get(STARTED_MATERIALIZATION));
        completedMaterializationCounter =
                Preconditions.checkNotNull(counterMap.get(COMPLETED_MATERIALIZATION));
        failedMaterializationCounter =
                Preconditions.checkNotNull(counterMap.get(FAILED_MATERIALIZATION));
        lastDurationOfMaterializationGauge =
                Preconditions.checkNotNull(
                        (Gauge<Long>) gaugeMap.get(LAST_DURATION_OF_MATERIALIZATION));
        lastFullSizeOfMaterializationGauge =
                Preconditions.checkNotNull(
                        (Gauge<Long>) gaugeMap.get(LATEST_FULL_SIZE_OF_MATERIALIZATION));
        lastIncSizeOfMaterializationGauge =
                Preconditions.checkNotNull(
                        (Gauge<Long>) gaugeMap.get(LATEST_INC_SIZE_OF_MATERIALIZATION));
        lastFullSizeOfNonMaterializationGauge =
                Preconditions.checkNotNull(
                        (Gauge<Long>) gaugeMap.get(LATEST_FULL_SIZE_OF_NON_MATERIALIZATION));
        lastIncSizeOfNonMaterializationGauge =
                Preconditions.checkNotNull(
                        (Gauge<Long>) gaugeMap.get(LATEST_INC_SIZE_OF_NON_MATERIALIZATION));
    }

    private void runSnapshot(long checkpointId) throws Exception {
        changelogKeyedStateBackend
                .snapshot(
                        checkpointId,
                        System.currentTimeMillis(),
                        new MemCheckpointStreamFactory(1024),
                        CheckpointOptions.forCheckpointWithDefaultLocation())
                .get();
    }

    private PeriodicMaterializationManager periodicMaterializationManager(
            ChangelogKeyedStateBackend<Integer> changelogKeyedStateBackend,
            MetricGroup metricGroup) {
        return ChangelogStateBackendTestUtils.periodicMaterializationManager(
                changelogKeyedStateBackend, metricGroup, new CompletableFuture<>());
    }

    private ChangelogKeyedStateBackend<Integer> createChangelogKeyedStateBackend(
            MetricGroup metricGroup,
            SerializableFunctionWithException<RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                    snapshotResultFunction)
            throws Exception {
        return (ChangelogKeyedStateBackend<Integer>)
                ChangelogStateBackendTestUtils.createKeyedBackend(
                        new ChangelogStateBackend(
                                wrapStateBackendWithSnapshotFunction(
                                        new HashMapStateBackend(), snapshotResultFunction)),
                        buildEnv(),
                        metricGroup);
    }

    private Environment buildEnv() throws IOException {
        MockEnvironment env =
                MockEnvironment.builder()
                        .setTaskStateManager(TestTaskStateManager.builder().build())
                        .build();
        env.setCheckpointStorageAccess(
                new JobManagerCheckpointStorage().createCheckpointStorage(new JobID()));
        return env;
    }
}
