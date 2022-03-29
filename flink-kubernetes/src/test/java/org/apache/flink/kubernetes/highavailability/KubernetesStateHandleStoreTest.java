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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.kubernetes.highavailability.KubernetesStateHandleStore.StateHandleWithDeleteMarker;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.StringResourceVersion;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/** Tests for {@link KubernetesStateHandleStore} operations. */
public class KubernetesStateHandleStoreTest extends KubernetesHighAvailabilityTestBase {

    private static final String PREFIX = "test-prefix-";

    private final String key = PREFIX + JobID.generate();
    private final Predicate<String> filter = k -> k.startsWith(PREFIX);
    private final TestingLongStateHandleHelper longStateStorage =
            new TestingLongStateHandleHelper();

    private TestingLongStateHandleHelper.LongStateHandle state;

    @Before
    public void setup() {
        super.setup();
        state = new TestingLongStateHandleHelper.LongStateHandle(12345L);
        TestingLongStateHandleHelper.clearGlobalState();
    }

    @Test
    public void testAdd() throws Exception {
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
                            assertThat(store.getAllAndLock().size(), is(1));
                            assertThat(store.getAndLock(key).retrieveState(), is(state));
                        });
            }
        };
    }

    @Test
    public void testAddAndLockWithExistingKey() throws Exception {
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

                            try {
                                store.addAndLock(key, newState);
                                fail("Exception should be thrown.");
                            } catch (StateHandleStore.AlreadyExistException ex) {
                                final String msg =
                                        String.format(
                                                "%s already exists in ConfigMap %s",
                                                key, LEADER_CONFIGMAP_NAME);
                                assertThat(ex, FlinkMatchers.containsMessage(msg));
                            }

                            // Both initial & new handles should be in the storage (we never clean
                            // it for testing).
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            // Only the new one (second entry in the store) should have been
                            // discarded.
                            assertThat(oldState.isDiscarded(), is(false));
                            assertThat(newState.isDiscarded(), is(true));
                        });
            }
        };
    }

    @Test
    public void testAddAndLockWithDeletingEntry() throws Exception {
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

                            assertThat(store.getAndLock(key).retrieveState().getValue(), is(3L));

                            // Both initial & new handles should be in the storage (we never clean
                            // it for testing).
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            // Only the old one (first entry in the store) should have been
                            // discarded.
                            assertThat(oldState.getNumberOfSuccessfulDiscardCalls(), is(1));
                            assertThat(newState.getNumberOfSuccessfulDiscardCalls(), is(0));
                        });
            }
        };
    }

    @Test
    public void testAddWithPossiblyInconsistentStateHandling() throws Exception {
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
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(1));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(0));
                        });
            }
        };
    }

    @Test
    public void testAddFailedWhenConfigMapNotExistAndDiscardState() throws Exception {
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

                            try {
                                store.addAndLock(key, state);
                                fail("Exception should be thrown.");
                            } catch (Exception ex) {
                                final String msg =
                                        String.format(
                                                "ConfigMap %s does not exist.",
                                                LEADER_CONFIGMAP_NAME);
                                assertThat(ex, FlinkMatchers.containsMessage(msg));
                            }
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(1));
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(0),
                                    is(1));
                        });
            }
        };
    }

    @Test
    public void testReplace() throws Exception {
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

                            assertThat(store.getAllAndLock().size(), is(1));
                            assertThat(store.getAndLock(key).retrieveState(), is(newState));
                        });
            }
        };
    }

    @Test
    public void testReplaceWithKeyNotExist() throws Exception {
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

                            try {
                                assertThat(
                                        store.exists(key), is(StringResourceVersion.notExisting()));
                                store.replace(key, StringResourceVersion.notExisting(), newState);
                                fail("Exception should be thrown.");
                            } catch (StateHandleStore.NotExistException e) {
                                final String msg =
                                        String.format(
                                                "Could not find %s in ConfigMap %s",
                                                key, LEADER_CONFIGMAP_NAME);
                                assertThat(e, FlinkMatchers.containsMessage(msg));
                            }
                            assertThat(store.getAllAndLock().size(), is(0));
                        });
            }
        };
    }

    @Test
    public void testReplaceWithDeletingKey() throws Exception {
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

                            assertThat(store.exists(key), is(StringResourceVersion.notExisting()));
                            assertThrows(
                                    StateHandleStore.NotExistException.class,
                                    () ->
                                            store.replace(
                                                    key,
                                                    StringResourceVersion.notExisting(),
                                                    newState));

                            // Both handles should have been stored and discarded.
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            assertThat(oldState.isDiscarded(), is(true));
                            assertThat(newState.isDiscarded(), is(true));
                        });
            }
        };
    }

    @Test
    public void testReplaceWithDeletingKeyWithFailingDiscard() throws Exception {
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

                            assertThat(store.exists(key), is(StringResourceVersion.notExisting()));
                            final StateHandleStore.NotExistException exception =
                                    assertThrows(
                                            StateHandleStore.NotExistException.class,
                                            () ->
                                                    store.replace(
                                                            key,
                                                            StringResourceVersion.notExisting(),
                                                            newState));
                            assertThat(exception.getSuppressed().length, is(1));
                            assertThat(
                                    exception.getSuppressed()[0],
                                    FlinkMatchers.containsCause(discardException));

                            // Only the new handle should have been discarded.
                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            assertThat(oldState.isDiscarded(), is(false));
                            assertThat(newState.isDiscarded(), is(true));
                        });
            }
        };
    }

    @Test
    public void testReplaceWithNoLeadershipAndDiscardState() throws Exception {
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
                            electionEventHandler.waitForRevokeLeader();
                            getLeaderConfigMap()
                                    .getAnnotations()
                                    .remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

                            final StringResourceVersion resourceVersion = store.exists(key);
                            store.replace(key, resourceVersion, newState);
                            assertThat(store.getAllAndLock().size(), is(1));
                            // The state do not change
                            assertThat(store.getAndLock(key).retrieveState(), is(state));

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(0),
                                    is(0));
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(1),
                                    is(1));
                        });
            }
        };
    }

    @Test
    public void testReplaceFailedAndDiscardState() throws Exception {
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
                                                    (configMapName, function) -> {
                                                        throw updateException;
                                                    })
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
                            assertThat(resourceVersion.isExisting(), is(true));
                            try {
                                anotherStore.replace(key, resourceVersion, newState);
                                fail(
                                        "We should get an exception when kube client failed to update.");
                            } catch (Exception ex) {
                                assertThat(ex, FlinkMatchers.containsCause(updateException));
                            }
                            assertThat(anotherStore.getAllAndLock().size(), is(1));
                            // The state do not change
                            assertThat(anotherStore.getAndLock(key).retrieveState(), is(state));

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(0),
                                    is(0));
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(1),
                                    is(1));
                        });
            }
        };
    }

    @Test
    public void testReplaceFailedWithPossiblyInconsistentState() throws Exception {
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
                            assertThat(resourceVersion.isExisting(), is(true));
                            try {
                                anotherStore.replace(
                                        key,
                                        resourceVersion,
                                        new TestingLongStateHandleHelper.LongStateHandle(23456L));
                                fail(
                                        "An exception having a PossibleInconsistentStateException as its cause should have been thrown.");
                            } catch (Exception ex) {
                                assertThat(ex, is(updateException));
                            }
                            assertThat(anotherStore.getAllAndLock().size(), is(1));
                            // The state does not change
                            assertThat(anotherStore.getAndLock(key).retrieveState(), is(state));

                            assertThat(TestingLongStateHandleHelper.getGlobalStorageSize(), is(2));
                            // no state was discarded
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(0),
                                    is(0));
                            assertThat(
                                    TestingLongStateHandleHelper
                                            .getDiscardCallCountForStateHandleByIndex(1),
                                    is(0));
                        });
            }
        };
    }

    @Test
    public void testGetAndExist() throws Exception {
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
                            assertThat(
                                    store.exists(key),
                                    is(not(StringResourceVersion.notExisting())));
                            assertThat(store.getAndLock(key).retrieveState(), is(state));
                        });
            }
        };
    }

    @Test
    public void testExistsWithDeletingEntry() throws Exception {
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
                            assertThat(resourceVersion.isExisting(), is(false));
                            // We don't try to pro-actively remove the entry here.
                            assertThat(state.isDiscarded(), is(false));
                        });
            }
        };
    }

    @Test
    public void testGetNonExistingKey() throws Exception {
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
                            assertThat(
                                    store.exists(nonExistingKey),
                                    is(StringResourceVersion.notExisting()));
                            try {
                                store.getAndLock(nonExistingKey);
                                fail("Exception should be thrown.");
                            } catch (StateHandleStore.NotExistException e) {
                                final String msg =
                                        String.format(
                                                "Could not find %s in ConfigMap %s",
                                                nonExistingKey, LEADER_CONFIGMAP_NAME);
                                assertThat(e, FlinkMatchers.containsMessage(msg));
                            }
                        });
            }
        };
    }

    @Test
    public void testGetAll() throws Exception {
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
                                                    TestingLongStateHandleHelper.LongStateHandle
                                                            ::getValue)
                                            .collect(Collectors.toList()),
                                    containsInAnyOrder(expected.toArray()));
                        });
            }
        };
    }

    @Test
    public void testGetAllHandles() throws Exception {
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
                            assertThat(
                                    Arrays.asList(actual), containsInAnyOrder(expected.toArray()));
                        });
            }
        };
    }

    @Test
    public void testRemove() throws Exception {
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
                            assertThat(store.getAllAndLock().size(), is(1));
                            assertThat(store.releaseAndTryRemove(key), is(true));
                            assertThat(store.getAllAndLock().size(), is(0));

                            // State should also be discarded.
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(1));
                        });
            }
        };
    }

    @Test
    public void testRemoveOfNonExistingState() throws Exception {
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
                            assertThat(store.getAllAndLock().size(), is(0));
                            assertThat(store.releaseAndTryRemove(key), is(true));
                            assertThat(store.getAllAndLock().size(), is(0));

                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(0));
                        });
            }
        };
    }

    @Test
    public void testRemoveFailedShouldNotDiscardState() throws Exception {
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
                            electionEventHandler.waitForRevokeLeader();
                            getLeaderConfigMap()
                                    .getAnnotations()
                                    .remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

                            assertThat(store.releaseAndTryRemove(key), is(false));
                            assertThat(store.getAllAndLock().size(), is(1));

                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(0));
                        });
            }
        };
    }

    @Test
    public void testRemoveAllHandlesAndDiscardState() throws Exception {
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
                            store.addAndLock(
                                    key + "1",
                                    new TestingLongStateHandleHelper.LongStateHandle(2L));
                            assertThat(store.getAllAndLock().size(), is(2));
                            store.releaseAndTryRemoveAll();
                            assertThat(store.getAllAndLock().size(), is(0));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(2));
                        });
            }
        };
    }

    @Test
    public void testRemoveAllHandles() throws Exception {
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
                            assertThat(store.getAllAndLock().size(), is(2));
                            store.clearEntries();
                            assertThat(store.getAllAndLock().size(), is(0));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(0));

                            // Should only remove the key with specified prefix.
                            assertThat(
                                    getLeaderConfigMap().getData().containsKey(anotherKey),
                                    is(true));
                        });
            }
        };
    }

    @Test
    public void testReleaseAndTryRemoveIsIdempotent() throws Exception {
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

                            assertThat(store.getAllAndLock().size(), is(1));
                            assertThat(
                                    store.getAndLock(key),
                                    is(notNullValue(RetrievableStateHandle.class)));

                            // First remove attempt should fail when we're discarding the underlying
                            // state.
                            final Exception exception =
                                    assertThrows(
                                            Exception.class, () -> store.releaseAndTryRemove(key));
                            assertThat(exception, FlinkMatchers.containsCause(discardException));

                            // Now we should see that the node is "soft-deleted". This means it can
                            // no longer be accessed by the get methods, but the underlying state
                            // still exists.
                            assertThat(store.getAllAndLock().size(), is(0));
                            assertThrows(Exception.class, () -> store.getAndLock(key));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(0));
                            assertThat(getLeaderConfigMap().getData().containsKey(key), is(true));

                            // Second retry should succeed and remove the underlying state and the
                            // reference in config map.
                            assertThat(store.releaseAndTryRemove(key), is(true));
                            assertThat(store.getAllAndLock().size(), is(0));
                            assertThrows(Exception.class, () -> store.getAndLock(key));
                            assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount(), is(1));
                            assertThat(getLeaderConfigMap().getData().containsKey(key), is(false));
                        });
            }
        };
    }

    @Test
    public void testReleaseAndTryRemoveAllIsIdempotent() throws Exception {
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
                            final int numKeys = 10;

                            final RuntimeException discardException =
                                    new RuntimeException("Test exception.");
                            final List<TestingLongStateHandleHelper.LongStateHandle> states =
                                    new ArrayList<>();
                            for (int idx = 0; idx < numKeys; idx++) {
                                final boolean failFirstDiscard = idx % 2 == 0;
                                final TestingLongStateHandleHelper.LongStateHandle state =
                                        new TestingLongStateHandleHelper.LongStateHandle(
                                                idx + 1,
                                                discardIdx -> {
                                                    if (failFirstDiscard && discardIdx == 0) {
                                                        throw discardException;
                                                    }
                                                });
                                states.add(state);
                                store.addAndLock(key + "_" + idx, state);
                            }

                            // All keys should be retrievable
                            assertThat(store.getAllAndLock().size(), is(numKeys));
                            for (int idx = 0; idx < numKeys; idx++) {
                                assertThat(
                                        store.getAndLock(key + "_" + idx),
                                        is(notNullValue(RetrievableStateHandle.class)));
                            }

                            // First remove attempt should fail when we're discarding the underlying
                            // state.
                            final Exception exception =
                                    assertThrows(Exception.class, store::releaseAndTryRemoveAll);
                            assertThat(exception, FlinkMatchers.containsCause(discardException));

                            // Now we should see that the all nodes are "soft-deleted". This means
                            // it can no longer be accessed by the get methods, but the underlying
                            // state still exists.
                            assertThat(store.getAllAndLock().size(), is(0));
                            for (int idx = 0; idx < numKeys; idx++) {
                                final String indexKey = key + "_" + idx;
                                assertThrows(Exception.class, () -> store.getAndLock(indexKey));
                                assertThat(
                                        getLeaderConfigMap().getData().containsKey(indexKey),
                                        is(true));
                            }
                            // Half of the state handles should have been discarded.
                            assertThat(
                                    states.stream()
                                            .filter(
                                                    TestingLongStateHandleHelper.LongStateHandle
                                                            ::isDiscarded)
                                            .count(),
                                    is((long) numKeys / 2));

                            // Second retry should succeed and remove the underlying state and the
                            // reference in config map.
                            store.releaseAndTryRemoveAll();
                            assertThat(store.getAllAndLock().size(), is(0));
                            for (int idx = 0; idx < numKeys; idx++) {
                                final String indexKey = key + "_" + idx;
                                assertThrows(Exception.class, () -> store.getAndLock(indexKey));
                                assertThat(
                                        getLeaderConfigMap().getData().containsKey(indexKey),
                                        is(false));
                            }
                            // All handles should have been discarded.
                            assertThat(
                                    states.stream()
                                            .filter(
                                                    TestingLongStateHandleHelper.LongStateHandle
                                                            ::isDiscarded)
                                            .count(),
                                    is((long) numKeys));
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
}
