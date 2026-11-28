/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.highavailability.ApplicationResult;
import org.apache.flink.runtime.highavailability.ApplicationResultEntry;
import org.apache.flink.runtime.highavailability.ApplicationResultStore;
import org.apache.flink.runtime.highavailability.ApplicationResultStoreContractTest;
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

/** Tests for {@link KubernetesApplicationResultStore}. */
class KubernetesApplicationResultStoreTest implements ApplicationResultStoreContractTest {

    private static final String CLUSTER_ID = "application-result-store-test-cluster";
    private static final String CONFIGMAP_NAME =
            CLUSTER_ID + "-application-result-store-config-map";

    /** The dummy result of the contract test has no timestamps. */
    private static final String DUMMY_RESULT_VALUE = "FINISHED:-1:-1:test-application";

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
    public ApplicationResultStore createApplicationResultStore() {
        return createStore(() -> true, false);
    }

    @Test
    void testDirtyEntryHoldsTheDirtyMarkerAndTheResult() {
        final ApplicationResultStore store = createApplicationResultStore();
        final ApplicationResult failedResult =
                createResult(ApplicationState.FAILED, "failed-application", 100, 250);

        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        store.createDirtyResultAsync(new ApplicationResultEntry(failedResult)).join();

        assertThat(getData())
                .containsOnly(
                        entry(
                                keyOf(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId()),
                                KubernetesApplicationResultStore.DIRTY_PREFIX + DUMMY_RESULT_VALUE),
                        entry(
                                keyOf(failedResult.getApplicationId()),
                                KubernetesApplicationResultStore.DIRTY_PREFIX
                                        + "FAILED:100:250:failed-application"));
    }

    @Test
    void testDirtyResultIsRecoveredWithAllItsFields() throws IOException {
        final ApplicationResultStore store = createApplicationResultStore();
        final ApplicationResult result =
                createResult(ApplicationState.CANCELED, "canceled-application", 100, 250);
        store.createDirtyResultAsync(new ApplicationResultEntry(result)).join();

        assertThat(store.getDirtyResults())
                .singleElement()
                .satisfies(recovered -> assertSameResult(recovered, result));
    }

    @Test
    void testMarkingCleanKeepsTheResultUnderTheCleanMarker() {
        final ApplicationResultStore store = createStore(() -> true, false);
        final ApplicationID applicationId = DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId();
        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();

        store.markResultAsCleanAsync(applicationId).join();

        assertThat(getData())
                .containsOnly(
                        entry(
                                keyOf(applicationId),
                                KubernetesApplicationResultStore.CLEAN_PREFIX
                                        + DUMMY_RESULT_VALUE));
        assertThat(store.getCleanApplicationResultAsync(applicationId).join())
                .satisfies(
                        recovered ->
                                assertSameResult(
                                        recovered,
                                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationResult()));
    }

    @Test
    void testDeleteOnCommitRemovesTheEntry() {
        final ApplicationResultStore store = createStore(() -> true, true);
        final ApplicationID applicationId = DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId();
        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();

        store.markResultAsCleanAsync(applicationId).join();

        assertThat(getData()).isEmpty();
        assertThat(store.hasApplicationResultEntryAsync(applicationId).join()).isFalse();
        assertThat(store.getCleanApplicationResultAsync(applicationId).join()).isNull();
    }

    @Test
    void testApplicationNameMayContainTheFieldSeparator() throws IOException {
        final ApplicationResultStore store = createApplicationResultStore();
        final ApplicationResult result =
                createResult(ApplicationState.FINISHED, "SingleJobApplication(a:b::c)", 1, 2);
        store.createDirtyResultAsync(new ApplicationResultEntry(result)).join();

        assertThat(store.getDirtyResults())
                .singleElement()
                .satisfies(recovered -> assertSameResult(recovered, result));

        store.markResultAsCleanAsync(result.getApplicationId()).join();

        assertThat(store.getCleanApplicationResultAsync(result.getApplicationId()).join())
                .satisfies(recovered -> assertSameResult(recovered, result));
    }

    @Test
    void testDirtyEntryIsRejectedWithoutLeadership() {
        final ApplicationResultStore store = createStore(() -> false, false);

        assertThatThrownBy(
                        () -> store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join())
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("leadership");
        assertThat(getStoredValue(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())).isEmpty();
    }

    @Test
    void testMarkingCleanIsRejectedWithoutLeadership() {
        final AtomicBoolean isLeader = new AtomicBoolean(true);
        final ApplicationResultStore store = createStore(isLeader::get, false);
        final ApplicationID applicationId = DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId();
        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();

        isLeader.set(false);

        assertThatThrownBy(() -> store.markResultAsCleanAsync(applicationId).join())
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("leadership");
        assertThat(store.hasDirtyApplicationResultEntryAsync(applicationId).join()).isTrue();
    }

    @Test
    void testConfigMapIsCreatedOnFirstWriteWithClusterLabels() {
        final ApplicationResultStore store = createApplicationResultStore();
        final ApplicationID applicationId = DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId();

        assertThat(store.hasApplicationResultEntryAsync(applicationId).join()).isFalse();
        assertThat(store.getCleanApplicationResultAsync(applicationId).join()).isNull();
        assertThat(kubeClient.getConfigMap(CONFIGMAP_NAME)).isEmpty();

        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();

        final Optional<KubernetesConfigMap> configMap = kubeClient.getConfigMap(CONFIGMAP_NAME);
        assertThat(configMap).isPresent();
        assertThat(configMap.get().getLabels())
                .containsExactlyInAnyOrderEntriesOf(KubernetesUtils.getConfigMapLabels(CLUSTER_ID));
        assertThat(configMap.get().getData()).containsOnlyKeys(keyOf(applicationId));
    }

    @Test
    void testGetDirtyResultsWithoutConfigMapIsEmpty() throws IOException {
        final ApplicationResultStore store = createApplicationResultStore();

        assertThat(store.getDirtyResults()).isEmpty();
        assertThat(kubeClient.getConfigMap(CONFIGMAP_NAME)).isEmpty();
    }

    @ParameterizedTest(name = "value = {0}")
    @ValueSource(strings = {"not a result", "DIRTY:FINISHED", "DIRTY:NO_SUCH_STATE:1:2:name"})
    void testUnparsableDirtyEntryFailsRecovery(String corruptedValue) {
        final ApplicationResultStore store = createApplicationResultStore();
        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        final String corruptedKey = new ApplicationID().toHexString();
        getData().put(corruptedKey, corruptedValue);

        assertThatThrownBy(store::getDirtyResults)
                .isInstanceOf(IOException.class)
                .hasMessageContaining(corruptedKey)
                .hasMessageContaining(CONFIGMAP_NAME);
    }

    @Test
    void testUnparsableCleanEntryFailsLookup() {
        final ApplicationResultStore store = createApplicationResultStore();
        final ApplicationID applicationId = DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId();
        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();
        getData().put(keyOf(applicationId), KubernetesApplicationResultStore.CLEAN_PREFIX + "?");

        assertThatThrownBy(() -> store.getCleanApplicationResultAsync(applicationId).join())
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining(keyOf(applicationId));
    }

    @Test
    void testDirtyWriteIsIdempotentWhenTheClientRetriesIt() {
        final ApplicationResultStore store = createStore(retryingKubeClient(), () -> true, false);

        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();

        assertThat(getStoredValue(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId()))
                .contains(KubernetesApplicationResultStore.DIRTY_PREFIX + DUMMY_RESULT_VALUE);
    }

    @ParameterizedTest(name = "deleteOnCommit = {0}")
    @ValueSource(booleans = {false, true})
    void testMarkingCleanIsIdempotentWhenTheClientRetriesIt(boolean deleteOnCommit) {
        final ApplicationResultStore store =
                createStore(retryingKubeClient(), () -> true, deleteOnCommit);
        final ApplicationID applicationId = DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId();
        store.createDirtyResultAsync(DUMMY_APPLICATION_RESULT_ENTRY).join();

        store.markResultAsCleanAsync(applicationId).join();

        if (deleteOnCommit) {
            assertThat(getStoredValue(applicationId)).isEmpty();
        } else {
            assertThat(getStoredValue(applicationId))
                    .contains(KubernetesApplicationResultStore.CLEAN_PREFIX + DUMMY_RESULT_VALUE);
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

    private static void assertSameResult(ApplicationResult actual, ApplicationResult expected) {
        assertThat(actual.getApplicationId()).isEqualTo(expected.getApplicationId());
        assertThat(actual.getApplicationState()).isEqualTo(expected.getApplicationState());
        assertThat(actual.getApplicationName()).isEqualTo(expected.getApplicationName());
        assertThat(actual.getStartTime()).isEqualTo(expected.getStartTime());
        assertThat(actual.getEndTime()).isEqualTo(expected.getEndTime());
    }

    private static ApplicationResult createResult(
            ApplicationState state, String name, long startTime, long endTime) {
        return new ApplicationResult.Builder()
                .applicationId(new ApplicationID())
                .applicationState(state)
                .applicationName(name)
                .startTime(startTime)
                .endTime(endTime)
                .build();
    }

    private static String keyOf(ApplicationID applicationId) {
        return applicationId.toHexString();
    }

    private KubernetesApplicationResultStore createStore(
            BooleanSupplier leadershipCheck, boolean deleteOnCommit) {
        return createStore(kubeClient, leadershipCheck, deleteOnCommit);
    }

    private KubernetesApplicationResultStore createStore(
            FlinkKubeClient kubeClient, BooleanSupplier leadershipCheck, boolean deleteOnCommit) {
        return new KubernetesApplicationResultStore(
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

    private Optional<String> getStoredValue(ApplicationID applicationId) {
        return kubeClient
                .getConfigMap(CONFIGMAP_NAME)
                .map(configMap -> configMap.getData().get(keyOf(applicationId)));
    }
}
