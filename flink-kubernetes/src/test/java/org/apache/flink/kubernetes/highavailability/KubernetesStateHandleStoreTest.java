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
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesStateHandleStore.StateHandleWithDeleteMarker;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.StringResourceVersion;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;

import io.fabric8.kubernetes.client.KubernetesClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link KubernetesStateHandleStore} operations. */
class KubernetesStateHandleStoreTest extends KubernetesHighAvailabilityTestBase {

    private static final String PREFIX = "test-prefix-";

    private final String key = PREFIX + JobID.generate();
    private final Predicate<String> filter = k -> k.startsWith(PREFIX);
    private final TestingLongStateHandleHelper longStateStorage =
            new TestingLongStateHandleHelper();

    private TestingLongStateHandleHelper.LongStateHandle state;

    @BeforeEach
    void setup() {
        super.setup();
        state = new TestingLongStateHandleHelper.LongStateHandle(12345L);
        TestingLongStateHandleHelper.clearGlobalState();
    }

    @Test
    void testAdd() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            store.addAndLock(key, state);
                            assertThat(store.getAllAndLock()).hasSize(1);
                            assertThat(store.getAndLock(key).retrieveState()).isEqualTo(state);
                        });
            }
        };
    }

    @Test
    void testAddAndLockWithExistingKey() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final TestingLongStateHandleHelper.LongStateHandle oldState =
                                    addRegularEntry(getLeaderConfigMap(), key, 1337L);

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(12345L);

                            final String msg =
                                    String.format(
                                            "%s already exists in ConfigMap %s",
                                            key, LEADER_CONFIGMAP_NAME);
                            assertThatThrownBy(
                                            () -> store.addAndLock(key, newState),
                                            "Exception should be thrown.")
                                    .satisfies(
                                            anyCauseMatches(
                                                    StateHandleStore.AlreadyExistException.class,
                                                    msg));

                            // Both initial & new handles should be in the storage (we never clean
                            // it for testing).
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            // Only the new one (second entry in the store) should have been
                            // discarded.
                            assertThat(oldState.isDiscarded()).isFalse();
                            assertThat(newState.isDiscarded()).isTrue();
                        });
            }
        };
    }

    @Test
    void testAddAndLockWithDeletingEntry() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final TestingLongStateHandleHelper.LongStateHandle oldState =
                                    addDeletingEntry(getLeaderConfigMap(), key, 1337L);

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(3L);
                            store.addAndLock(
                                    key, new TestingLongStateHandleHelper.LongStateHandle(3L));

                            assertThat(store.getAndLock(key).retrieveState().getValue())
                                    .isEqualTo(3L);

                            // Both initial & new handles should be in the storage (we never clean
                            // it for testing).
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            // Only the old one (first entry in the store) should have been
                            // discarded.
                            assertThat(oldState.getNumberOfSuccessfulDiscardCalls()).isEqualTo(1);
                            assertThat(newState.getNumberOfSuccessfulDiscardCalls()).isEqualTo(0);
                        });
            }
        };
    }

    @Test
    void testAddWithPossiblyInconsistentStateHandling() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient anotherFlinkKubeClient =
                                    createFlinkKubeClientBuilder()
                                            .setCheckAndUpdateConfigMapFunction(
                                                    (configMapName, function) ->
                                                            FutureUtils.completedExceptionally(
                                                                    new PossibleInconsistentStateException()))
                                            .build();
                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    anotherFlinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            try {
                                store.addAndLock(key, state);
                                fail("PossibleInconsistentStateException should have been thrown.");
                            } catch (PossibleInconsistentStateException ex) {
                                // PossibleInconsistentStateException is expected
                            }
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(1);
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(0);
                        });
            }
        };
    }

    @Test
    void testAddAndLockShouldNotThrowAlreadyExistExceptionWithSameContents() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final FlinkKubeClient anotherFlinkKubeClient =
                                    createFlinkKubeClientBuilder()
                                            .setCheckAndUpdateConfigMapFunction(
                                                    (configMapName, function) ->
                                                            retryWithFirstFailedK8sOperation(
                                                                    function, getLeaderConfigMap()))
                                            .build();
                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    anotherFlinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            store.addAndLock(key, state);
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(1);
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(0);
                            assertThat(store.getAllHandles()).hasSize(1);
                            assertThat(store.getAllHandles()).contains(key);
                            assertThat(store.getAndLock(key).retrieveState().getValue())
                                    .isEqualTo(state.getValue());
                        });
            }
        };
    }

    @Test
    void testAddFailedWhenConfigMapNotExistAndDiscardState() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final String msg =
                                    String.format(
                                            "ConfigMap %s does not exist.", LEADER_CONFIGMAP_NAME);
                            assertThatThrownBy(
                                            () -> store.addAndLock(key, state),
                                            "Exception should be thrown.")
                                    .satisfies(anyCauseMatches(msg));

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(1);
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(0))
                                    .isEqualTo(1);
                        });
            }
        };
    }

    @Test
    void testReplace() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            store.addAndLock(key, state);

                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(23456L);
                            final StringResourceVersion resourceVersion = store.exists(key);
                            store.replace(key, resourceVersion, newState);

                            assertThat(store.getAllAndLock()).hasSize(1);
                            assertThat(store.getAndLock(key).retrieveState()).isEqualTo(newState);
                        });
            }
        };
    }

    @Test
    void testReplaceWithKeyNotExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(23456L);

                            assertThat(store.exists(key))
                                    .isEqualByComparingTo(StringResourceVersion.notExisting());
                            final String msg =
                                    String.format(
                                            "Could not find %s in ConfigMap %s",
                                            key, LEADER_CONFIGMAP_NAME);
                            assertThatThrownBy(
                                            () ->
                                                    store.replace(
                                                            key,
                                                            StringResourceVersion.notExisting(),
                                                            newState),
                                            "Exception should be thrown.")
                                    .satisfies(
                                            anyCauseMatches(
                                                    StateHandleStore.NotExistException.class, msg));
                            assertThat(store.getAllAndLock()).isEmpty();
                        });
            }
        };
    }

    @Test
    void testReplaceWithDeletingKey() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final TestingLongStateHandleHelper.LongStateHandle oldState =
                                    addDeletingEntry(getLeaderConfigMap(), key, 1337);

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(23456L);

                            assertThat(store.exists(key))
                                    .isEqualByComparingTo(StringResourceVersion.notExisting());
                            assertThatThrownBy(
                                            () ->
                                                    store.replace(
                                                            key,
                                                            StringResourceVersion.notExisting(),
                                                            newState))
                                    .satisfies(
                                            anyCauseMatches(
                                                    StateHandleStore.NotExistException.class));
                            // Both handles should have been stored and discarded.
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            assertThat(oldState.isDiscarded()).isTrue();
                            assertThat(newState.isDiscarded()).isTrue();
                        });
            }
        };
    }

    @Test
    void testReplaceWithDeletingKeyWithFailingDiscard() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final Exception discardException = new Exception("Unable to discard.");
                            final TestingLongStateHandleHelper.LongStateHandle oldState =
                                    addDeletingEntry(
                                            getLeaderConfigMap(),
                                            key,
                                            new TestingLongStateHandleHelper.LongStateHandle(
                                                    1337,
                                                    discardIdx -> {
                                                        throw discardException;
                                                    }));

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(23456L);

                            assertThat(store.exists(key))
                                    .isEqualByComparingTo(StringResourceVersion.notExisting());
                            assertThatThrownBy(
                                            () ->
                                                    store.replace(
                                                            key,
                                                            StringResourceVersion.notExisting(),
                                                            newState))
                                    .satisfies(
                                            cause -> {
                                                assertThat(cause)
                                                        .isInstanceOf(
                                                                StateHandleStore.NotExistException
                                                                        .class);
                                                assertThat(cause.getSuppressed()).hasSize(1);
                                                assertThatChainOfCauses(cause.getSuppressed()[0])
                                                        .anySatisfy(
                                                                ex ->
                                                                        assertThat(ex)
                                                                                .isInstanceOf(
                                                                                        discardException
                                                                                                .getClass())
                                                                                .hasMessageContaining(
                                                                                        discardException
                                                                                                .getMessage()));
                                            });

                            // Only the new handle should have been discarded.
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            assertThat(oldState.isDiscarded()).isFalse();
                            assertThat(newState.isDiscarded()).isTrue();
                        });
            }
        };
    }

    @Test
    void testReplaceWithNoLeadershipAndDiscardState() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(23456L);

                            store.addAndLock(key, state);
                            // Lost leadership
                            getLeaderCallback().notLeader();
                            electionEventHandler.await(LeaderElectionEvent.NotLeaderEvent.class);

                            getLeaderConfigMap()
                                    .getAnnotations()
                                    .remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

                            final StringResourceVersion resourceVersion = store.exists(key);
                            store.replace(key, resourceVersion, newState);
                            assertThat(store.getAllAndLock()).hasSize(1);
                            // The state do not change
                            assertThat(store.getAndLock(key).retrieveState()).isEqualTo(state);

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(0))
                                    .isEqualTo(0);
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(1))
                                    .isEqualTo(1);
                        });
            }
        };
    }

    @Test
    void testReplaceFailedAndDiscardState() throws Exception {
        final FlinkRuntimeException updateException = new FlinkRuntimeException("Failed to update");
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            store.addAndLock(key, state);

                            final FlinkKubeClient anotherFlinkKubeClient =
                                    createFlinkKubeClientBuilder()
                                            .setCheckAndUpdateConfigMapFunction(
                                                    (configMapName, function) ->
                                                            FutureUtils.completedExceptionally(
                                                                    updateException))
                                            .build();
                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    anotherStore =
                                            new KubernetesStateHandleStore<>(
                                                    anotherFlinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            final TestingLongStateHandleHelper.LongStateHandle newState =
                                    new TestingLongStateHandleHelper.LongStateHandle(23456L);
                            final StringResourceVersion resourceVersion = anotherStore.exists(key);
                            assertThat(resourceVersion.isExisting()).isTrue();
                            assertThatThrownBy(
                                            () ->
                                                    anotherStore.replace(
                                                            key, resourceVersion, newState),
                                            "We should get an exception when kube client failed to update.")
                                    .satisfies(
                                            anyCauseMatches(
                                                    updateException.getClass(),
                                                    updateException.getMessage()));
                            assertThat(anotherStore.getAllAndLock()).hasSize(1);
                            // The state do not change
                            assertThat(anotherStore.getAndLock(key).retrieveState())
                                    .isEqualTo(state);

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(0))
                                    .isEqualTo(0);
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(1))
                                    .isEqualTo(1);
                        });
            }
        };
    }

    @Test
    void testReplaceFailedWithPossiblyInconsistentState() throws Exception {
        final PossibleInconsistentStateException updateException =
                new PossibleInconsistentStateException();
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            store.addAndLock(key, state);

                            final FlinkKubeClient anotherFlinkKubeClient =
                                    createFlinkKubeClientBuilder()
                                            .setCheckAndUpdateConfigMapFunction(
                                                    (configMapName, function) ->
                                                            FutureUtils.completedExceptionally(
                                                                    updateException))
                                            .build();
                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    anotherStore =
                                            new KubernetesStateHandleStore<>(
                                                    anotherFlinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            final StringResourceVersion resourceVersion = anotherStore.exists(key);
                            assertThat(resourceVersion.isExisting()).isTrue();
                            assertThatThrownBy(
                                            () ->
                                                    anotherStore.replace(
                                                            key,
                                                            resourceVersion,
                                                            new TestingLongStateHandleHelper
                                                                    .LongStateHandle(23456L)),
                                            "An exception having a PossibleInconsistentStateException as its cause should have been thrown.")
                                    .isEqualTo(updateException);
                            assertThat(anotherStore.getAllAndLock()).hasSize(1);
                            // The state does not change
                            assertThat(anotherStore.getAndLock(key).retrieveState())
                                    .isEqualTo(state);

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize())
                                    .isEqualTo(2);
                            // no state was discarded
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(0))
                                    .isEqualTo(0);
                            assertThat(
                                            TestingLongStateHandleHelper
                                                    .getDiscardCallCountForStateHandleByIndex(1))
                                    .isEqualTo(0);
                        });
            }
        };
    }

    @Test
    void testGetAndExist() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            store.addAndLock(key, state);
                            assertThat(store.exists(key))
                                    .isNotEqualByComparingTo(StringResourceVersion.notExisting());
                            assertThat(store.getAndLock(key).retrieveState()).isEqualTo(state);
                        });
            }
        };
    }

    @Test
    void testExistsWithDeletingEntry() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final TestingLongStateHandleHelper.LongStateHandle state =
                                    addDeletingEntry(getLeaderConfigMap(), key, 1337L);

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            final StringResourceVersion resourceVersion = store.exists(key);
                            assertThat(resourceVersion.isExisting()).isFalse();
                            // We don't try to pro-actively remove the entry here.
                            assertThat(state.isDiscarded()).isFalse();
                        });
            }
        };
    }

    @Test
    void testGetNonExistingKey() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final String nonExistingKey = "non-existing-key";
                            store.addAndLock(key, state);
                            assertThat(store.exists(nonExistingKey))
                                    .isEqualByComparingTo(StringResourceVersion.notExisting());

                            final String msg =
                                    String.format(
                                            "Could not find %s in ConfigMap %s",
                                            nonExistingKey, LEADER_CONFIGMAP_NAME);
                            assertThatThrownBy(
                                            () -> store.getAndLock(nonExistingKey),
                                            "Exception should be thrown.")
                                    .satisfies(anyCauseMatches(msg));
                        });
            }
        };
    }

    @Test
    void testGetAll() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final List<Long> expected = Arrays.asList(3L, 2L, 1L);

                            for (Long each : expected) {
                                store.addAndLock(
                                        key + each,
                                        new TestingLongStateHandleHelper.LongStateHandle(each));
                            }
                            final TestingLongStateHandleHelper.LongStateHandle[] actual =
                                    store.getAllAndLock().stream()
                                            .map(
                                                    FunctionUtils.uncheckedFunction(
                                                            e -> e.f0.retrieveState()))
                                            .toArray(
                                                    TestingLongStateHandleHelper.LongStateHandle[]
                                                            ::new);
                            assertThat(
                                            Arrays.stream(actual)
                                                    .map(
                                                            TestingLongStateHandleHelper
                                                                            .LongStateHandle
                                                                    ::getValue))
                                    .containsExactlyInAnyOrderElementsOf(expected);
                        });
            }
        };
    }

    @Test
    void testGetAllHandles() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            final List<String> expected = Arrays.asList(key + 3, key + 2, key + 1);

                            for (String each : expected) {
                                store.addAndLock(each, state);
                            }
                            final String[] actual = store.getAllHandles().toArray(new String[0]);
                            expected.sort(Comparator.comparing(e -> e));
                            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
                        });
            }
        };
    }

    @Test
    void testRemove() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            store.addAndLock(key, state);
                            assertThat(store.getAllAndLock()).hasSize(1);
                            assertThat(store.releaseAndTryRemove(key)).isTrue();
                            assertThat(store.getAllAndLock()).hasSize(0);

                            // State should also be discarded.
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(1);
                        });
            }
        };
    }

    @Test
    void testRemoveOfNonExistingState() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            assertThat(store.getAllAndLock()).hasSize(0);
                            assertThat(store.releaseAndTryRemove(key)).isTrue();
                            assertThat(store.getAllAndLock()).hasSize(0);

                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(0);
                        });
            }
        };
    }

    @Test
    void testRemoveFailedShouldNotDiscardState() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            store.addAndLock(key, state);
                            // Lost leadership
                            getLeaderCallback().notLeader();
                            electionEventHandler.await(LeaderElectionEvent.NotLeaderEvent.class);

                            getLeaderConfigMap()
                                    .getAnnotations()
                                    .remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

                            assertThat(store.releaseAndTryRemove(key)).isFalse();
                            assertThat(store.getAllAndLock()).hasSize(1);

                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(0);
                        });
            }
        };
    }

    @Test
    void testRemoveAllHandles() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final String anotherKey = "key-not-with-prefix";
                            getLeaderConfigMap().getData().put(anotherKey, "value");

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);
                            store.addAndLock(key, state);
                            store.addAndLock(
                                    key + "1",
                                    new TestingLongStateHandleHelper.LongStateHandle(2L));
                            assertThat(store.getAllAndLock()).hasSize(2);
                            store.clearEntries();
                            assertThat(store.getAllAndLock()).hasSize(0);
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(0);

                            // Should only remove the key with specified prefix.
                            assertThat(getLeaderConfigMap().getData().containsKey(anotherKey))
                                    .isTrue();
                        });
            }
        };
    }

    @Test
    void testReleaseAndTryRemoveIsIdempotent() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();

                            final KubernetesStateHandleStore<
                                            TestingLongStateHandleHelper.LongStateHandle>
                                    store =
                                            new KubernetesStateHandleStore<>(
                                                    flinkKubeClient,
                                                    LEADER_CONFIGMAP_NAME,
                                                    longStateStorage,
                                                    filter,
                                                    LOCK_IDENTITY);

                            final RuntimeException discardException =
                                    new RuntimeException("Test exception.");
                            store.addAndLock(
                                    key,
                                    new TestingLongStateHandleHelper.LongStateHandle(
                                            2L,
                                            discardIdx -> {
                                                if (discardIdx == 0) {
                                                    throw discardException;
                                                }
                                            }));

                            assertThat(store.getAllAndLock()).hasSize(1);
                            assertThat(store.getAndLock(key)).isNotNull();

                            // First remove attempt should fail when we're discarding the underlying
                            // state.
                            assertThatThrownBy(() -> store.releaseAndTryRemove(key))
                                    .satisfies(
                                            anyCauseMatches(
                                                    discardException.getClass(),
                                                    discardException.getMessage()));

                            // Now we should see that the node is "soft-deleted". This means it can
                            // no longer be accessed by the get methods, but the underlying state
                            // still exists.
                            assertThat(store.getAllAndLock()).hasSize(0);
                            assertThatThrownBy(() -> store.getAndLock(key));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(0);
                            assertThat(getLeaderConfigMap().getData().containsKey(key)).isTrue();

                            // Second retry should succeed and remove the underlying state and the
                            // reference in config map.
                            assertThat(store.releaseAndTryRemove(key)).isTrue();
                            assertThat(store.getAllAndLock()).hasSize(0);
                            assertThatThrownBy(() -> store.getAndLock(key));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                                    .isEqualTo(1);
                            assertThat(getLeaderConfigMap().getData().containsKey(key)).isFalse();
                        });
            }
        };
    }

    private TestingLongStateHandleHelper.LongStateHandle addRegularEntry(
            KubernetesConfigMap configMap, String key, long value) throws IOException {
        final TestingLongStateHandleHelper.LongStateHandle state =
                new TestingLongStateHandleHelper.LongStateHandle(value);
        final String deleting =
                KubernetesStateHandleStore.serializeStateHandle(
                        new StateHandleWithDeleteMarker<>(longStateStorage.store(state)));
        configMap.getData().put(key, deleting);
        return state;
    }

    private TestingLongStateHandleHelper.LongStateHandle addDeletingEntry(
            KubernetesConfigMap configMap, String key, long value) throws IOException {
        return addDeletingEntry(
                configMap, key, new TestingLongStateHandleHelper.LongStateHandle(value));
    }

    private TestingLongStateHandleHelper.LongStateHandle addDeletingEntry(
            KubernetesConfigMap configMap,
            String key,
            TestingLongStateHandleHelper.LongStateHandle state)
            throws IOException {
        final String deleting =
                KubernetesStateHandleStore.serializeStateHandle(
                        new StateHandleWithDeleteMarker<>(longStateStorage.store(state))
                                .toDeleting());
        configMap.getData().put(key, deleting);
        return state;
    }

    private static CompletableFuture<Boolean> retryWithFirstFailedK8sOperation(
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> function,
            KubernetesConfigMap leaderConfigMap) {
        final AtomicInteger callbackInvocationCount = new AtomicInteger(0);
        final CompletableFuture<Boolean> result =
                FutureUtils.retry(
                        () ->
                                CompletableFuture.supplyAsync(
                                        () -> {
                                            callbackInvocationCount.incrementAndGet();
                                            function.apply(leaderConfigMap);
                                            if (callbackInvocationCount.get() == 1) {
                                                throw new KubernetesClientException(
                                                        "Expected exception to simulate unstable "
                                                                + "kubernetes client operation");
                                            }
                                            return true;
                                        },
                                        Executors.newDirectExecutorService()),
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES
                                .defaultValue(),
                        t ->
                                ExceptionUtils.findThrowable(t, KubernetesClientException.class)
                                        .isPresent(),
                        Executors.newDirectExecutorService());
        assertThat(callbackInvocationCount.get()).isEqualTo(2);
        assertThat(result).isCompleted();
        return result;
    }
}
