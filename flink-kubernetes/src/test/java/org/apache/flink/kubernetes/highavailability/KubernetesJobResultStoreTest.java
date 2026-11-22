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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreContractTest;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link KubernetesJobResultStore}. */
class KubernetesJobResultStoreTest implements JobResultStoreContractTest {

    private static final String CLUSTER_ID = "job-result-store-test-cluster";
    private static final String CONFIGMAP_NAME = CLUSTER_ID + "-job-result-store-config-map";

    private KubernetesTestFixture fixture;
    private FlinkKubeClient kubeClient;

    @BeforeEach
    void setup() {
        fixture =
                new KubernetesTestFixture(
                        CLUSTER_ID,
                        CLUSTER_ID + "-cluster-config-map",
                        UUID.randomUUID().toString());
        kubeClient = fixture.getFlinkKubeClient();
    }

    @AfterEach
    void teardown() {
        fixture.close();
    }

    @Override
    public JobResultStore createJobResultStore() {
        return createStore(() -> true, false);
    }

    @Test
    void testDirtyEntryHoldsTheJobStatusAndTheApplicationId() {
        final JobResultStore store = createJobResultStore();
        final JobResultEntry failedEntry = new JobResultEntry(createFailedJobResult());

        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        store.createDirtyResultAsync(failedEntry).join();

        assertThat(getData())
                .containsOnly(
                        entry(
                                DUMMY_JOB_RESULT_ENTRY.getJobId().toHexString(),
                                "FINISHED:" + applicationIdOf(DUMMY_JOB_RESULT_ENTRY)),
                        entry(
                                failedEntry.getJobId().toHexString(),
                                "FAILED:" + applicationIdOf(failedEntry)));
    }

    @Test
    void testDirtyResultIsRecoveredWithJobIdStatusAndApplicationIdOnly() throws IOException {
        final JobResultStore store = createJobResultStore();
        final JobResult failedResult = createFailedJobResult();
        store.createDirtyResultAsync(new JobResultEntry(failedResult)).join();

        assertThat(store.getDirtyResults())
                .singleElement()
                .satisfies(
                        recovered -> {
                            assertThat(recovered.getJobId()).isEqualTo(failedResult.getJobId());
                            assertThat(recovered.getJobStatus()).hasValue(JobStatus.FAILED);
                            assertThat(recovered.getApplicationId())
                                    .isEqualTo(failedResult.getApplicationId());
                            assertThat(recovered.getSerializedThrowable()).isEmpty();
                            assertThat(recovered.getAccumulatorResults()).isEmpty();
                            assertThat(recovered.getNetRuntime()).isZero();
                        });
    }

    @Test
    void testDirtyResultWithoutApplicationIdIsRecoveredWithoutOne() throws IOException {
        final JobResultStore store = createJobResultStore();
        final JobResult result =
                new JobResult.Builder()
                        .jobId(new JobID())
                        .jobStatus(JobStatus.CANCELED)
                        .netRuntime(0)
                        .build();
        store.createDirtyResultAsync(new JobResultEntry(result)).join();

        assertThat(getStoredValue(result.getJobId())).contains("CANCELED");
        assertThat(store.getDirtyResults())
                .singleElement()
                .satisfies(
                        recovered -> {
                            assertThat(recovered.getJobId()).isEqualTo(result.getJobId());
                            assertThat(recovered.getJobStatus()).hasValue(JobStatus.CANCELED);
                            assertThat(recovered.getApplicationId()).isEmpty();
                        });
    }

    @Test
    void testMarkingCleanReplacesTheStatusWithTheCleanValue() {
        final JobResultStore store = createStore(() -> true, false);
        final JobID jobId = DUMMY_JOB_RESULT_ENTRY.getJobId();
        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();

        store.markResultAsCleanAsync(jobId).join();

        assertThat(getData())
                .containsOnly(entry(jobId.toHexString(), KubernetesJobResultStore.CLEAN_VALUE));
    }

    @Test
    void testDeleteOnCommitRemovesTheEntry() {
        final JobResultStore store = createStore(() -> true, true);
        final JobID jobId = DUMMY_JOB_RESULT_ENTRY.getJobId();
        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();

        store.markResultAsCleanAsync(jobId).join();

        assertThat(getData()).isEmpty();
        assertThat(store.hasJobResultEntryAsync(jobId).join()).isFalse();
    }

    @Test
    void testDirtyEntryIsRejectedWithoutLeadership() {
        final JobResultStore store = createStore(() -> false, false);

        assertThatThrownBy(() -> store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join())
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("leadership");
        assertThat(getStoredValue(DUMMY_JOB_RESULT_ENTRY.getJobId())).isEmpty();
    }

    @Test
    void testMarkingCleanIsRejectedWithoutLeadership() {
        final AtomicBoolean isLeader = new AtomicBoolean(true);
        final JobResultStore store = createStore(isLeader::get, false);
        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();

        isLeader.set(false);

        assertThatThrownBy(
                        () ->
                                store.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId())
                                        .join())
                .hasCauseInstanceOf(IOException.class);
        assertThat(store.hasDirtyJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join())
                .isTrue();
    }

    @Test
    void testConfigMapIsCreatedOnFirstWriteWithClusterLabels() {
        final JobResultStore store = createJobResultStore();

        assertThat(store.hasJobResultEntryAsync(DUMMY_JOB_RESULT_ENTRY.getJobId()).join())
                .isFalse();
        assertThat(kubeClient.getConfigMap(CONFIGMAP_NAME)).isEmpty();

        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();

        final Optional<KubernetesConfigMap> configMap = kubeClient.getConfigMap(CONFIGMAP_NAME);
        assertThat(configMap).isPresent();
        assertThat(configMap.get().getLabels())
                .containsExactlyInAnyOrderEntriesOf(KubernetesUtils.getConfigMapLabels(CLUSTER_ID));
        assertThat(configMap.get().getData())
                .containsOnlyKeys(DUMMY_JOB_RESULT_ENTRY.getJobId().toHexString());
    }

    @Test
    void testGetDirtyResultsWithoutConfigMapIsEmpty() throws IOException {
        final JobResultStore store = createJobResultStore();

        assertThat(store.getDirtyResults()).isEmpty();
        assertThat(kubeClient.getConfigMap(CONFIGMAP_NAME)).isEmpty();
    }

    @Test
    void testUnparsableEntryFailsRecovery() {
        final JobResultStore store = createJobResultStore();
        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();
        final String corruptedKey = new JobID().toHexString();
        kubeClient.getConfigMap(CONFIGMAP_NAME).get().getData().put(corruptedKey, "not a status");

        assertThatThrownBy(store::getDirtyResults)
                .isInstanceOf(IOException.class)
                .hasMessageContaining(corruptedKey)
                .hasMessageContaining(CONFIGMAP_NAME);
    }

    @Test
    void testDirtyWriteIsIdempotentWhenTheClientRetriesIt() {
        final JobResultStore store = createStore(retryingKubeClient(), () -> true, false);

        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();

        assertThat(getStoredValue(DUMMY_JOB_RESULT_ENTRY.getJobId()))
                .contains("FINISHED:" + applicationIdOf(DUMMY_JOB_RESULT_ENTRY));
    }

    @ParameterizedTest(name = "deleteOnCommit = {0}")
    @ValueSource(booleans = {false, true})
    void testMarkingCleanIsIdempotentWhenTheClientRetriesIt(boolean deleteOnCommit) {
        final JobResultStore store = createStore(retryingKubeClient(), () -> true, deleteOnCommit);
        final JobID jobId = DUMMY_JOB_RESULT_ENTRY.getJobId();
        store.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY).join();

        store.markResultAsCleanAsync(jobId).join();

        if (deleteOnCommit) {
            assertThat(getStoredValue(jobId)).isEmpty();
        } else {
            assertThat(getStoredValue(jobId)).contains(KubernetesJobResultStore.CLEAN_VALUE);
        }
    }

    /**
     * A client that applies every update twice, like the real client does when it retries an update
     * whose response was lost: the first attempt lands, the second one sees its result.
     */
    private FlinkKubeClient retryingKubeClient() {
        return fixture.createFlinkKubeClientBuilder()
                .setCheckAndUpdateConfigMapFunction(
                        (configMapName, updateFunction) ->
                                kubeClient
                                        .checkAndUpdateConfigMap(configMapName, updateFunction)
                                        .thenCompose(
                                                ignored ->
                                                        kubeClient.checkAndUpdateConfigMap(
                                                                configMapName, updateFunction)))
                .build();
    }

    private static JobResult createFailedJobResult() {
        return new JobResult.Builder()
                .jobId(new JobID())
                .applicationId(new ApplicationID())
                .jobStatus(JobStatus.FAILED)
                .netRuntime(42)
                .serializedThrowable(new SerializedThrowable(new RuntimeException("boom")))
                .build();
    }

    private static String applicationIdOf(JobResultEntry entry) {
        return entry.getJobResult().getApplicationId().get().toHexString();
    }

    private KubernetesJobResultStore createStore(
            BooleanSupplier leadershipCheck, boolean deleteOnCommit) {
        return createStore(kubeClient, leadershipCheck, deleteOnCommit);
    }

    private KubernetesJobResultStore createStore(
            FlinkKubeClient kubeClient, BooleanSupplier leadershipCheck, boolean deleteOnCommit) {
        return new KubernetesJobResultStore(
                kubeClient,
                CONFIGMAP_NAME,
                CLUSTER_ID,
                leadershipCheck,
                deleteOnCommit,
                Executors.directExecutor());
    }

    private Map<String, String> getData() {
        return kubeClient.getConfigMap(CONFIGMAP_NAME).get().getData();
    }

    private Optional<String> getStoredValue(JobID jobId) {
        return kubeClient
                .getConfigMap(CONFIGMAP_NAME)
                .map(configMap -> configMap.getData().get(jobId.toHexString()));
    }
}
