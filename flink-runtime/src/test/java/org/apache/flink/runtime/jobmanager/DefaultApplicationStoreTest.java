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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DefaultApplicationStore} with {@link TestingStateHandleStore} and {@link
 * TestingApplicationStoreEntry}.
 */
public class DefaultApplicationStoreTest extends TestLogger {

    private final ApplicationStoreEntry testingApplication =
            TestingApplicationStoreEntry.newBuilder().build();
    private final long timeout = 100L;

    private TestingStateHandleStore.Builder<ApplicationStoreEntry> builder;
    private TestingRetrievableStateStorageHelper<ApplicationStoreEntry> storageHelper;

    @Before
    public void setup() {
        builder = TestingStateHandleStore.newBuilder();
        storageHelper = new TestingRetrievableStateStorageHelper<>();
    }

    @Test
    public void testRecoverApplication() throws Exception {
        final RetrievableStateHandle<ApplicationStoreEntry> stateHandle =
                storageHelper.store(testingApplication);
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setGetFunction(ignore -> stateHandle).build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);

        final Optional<ApplicationStoreEntry> recoveredApplication =
                applicationStore.recoverApplication(testingApplication.getApplicationId());
        assertThat(recoveredApplication).isPresent();
        assertThat(recoveredApplication.get().getApplicationId())
                .isEqualTo(testingApplication.getApplicationId());
    }

    @Test
    public void testRecoverApplicationWhenNotExist() throws Exception {
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw new StateHandleStore.NotExistException("Not exist.");
                                })
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);

        final Optional<ApplicationStoreEntry> recoveredApplication =
                applicationStore.recoverApplication(testingApplication.getApplicationId());
        assertThat(recoveredApplication).isEmpty();
    }

    @Test
    public void testRecoverApplicationFailedShouldReleaseHandle() throws Exception {
        final CompletableFuture<String> releaseFuture = new CompletableFuture<>();
        final FlinkException testException = new FlinkException("Test exception.");
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setGetFunction(
                                ignore -> {
                                    throw testException;
                                })
                        .setReleaseConsumer(releaseFuture::complete)
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);

        assertThatThrownBy(
                        () ->
                                applicationStore.recoverApplication(
                                        testingApplication.getApplicationId()))
                .hasCause(testException);
        String actual = releaseFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(testingApplication.getApplicationId()).hasToString(actual);
    }

    @Test
    public void testPutApplicationWhenNotExist() throws Exception {
        final CompletableFuture<ApplicationStoreEntry> addFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setExistsFunction(ignore -> IntegerResourceVersion.notExisting())
                        .setAddFunction(
                                (ignore, state) -> {
                                    addFuture.complete(state);
                                    return storageHelper.store(state);
                                })
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);
        applicationStore.putApplication(testingApplication);

        final ApplicationStoreEntry actual = addFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.getApplicationId()).isEqualTo(testingApplication.getApplicationId());
    }

    @Test
    public void testPutApplicationWhenAlreadyExist() throws Exception {
        final CompletableFuture<Tuple3<String, IntegerResourceVersion, ApplicationStoreEntry>>
                replaceFuture = new CompletableFuture<>();
        final int resourceVersion = 100;
        final AtomicBoolean alreadyExist = new AtomicBoolean(false);
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setExistsFunction(
                                ignore -> {
                                    if (alreadyExist.get()) {
                                        return IntegerResourceVersion.valueOf(resourceVersion);
                                    } else {
                                        alreadyExist.set(true);
                                        return IntegerResourceVersion.notExisting();
                                    }
                                })
                        .setAddFunction((ignore, state) -> storageHelper.store(state))
                        .setReplaceConsumer(replaceFuture::complete)
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);
        applicationStore.putApplication(testingApplication);
        // Replace
        applicationStore.putApplication(testingApplication);

        final Tuple3<String, IntegerResourceVersion, ApplicationStoreEntry> actual =
                replaceFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual.f0).isEqualTo(testingApplication.getApplicationId().toString());
        assertThat(actual.f1).isEqualTo(IntegerResourceVersion.valueOf(resourceVersion));
        assertThat(actual.f2.getApplicationId()).isEqualTo(testingApplication.getApplicationId());
    }

    @Test
    public void testGlobalCleanup() throws Exception {
        final CompletableFuture<ApplicationID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setAddFunction((ignore, state) -> storageHelper.store(state))
                        .setRemoveFunction(
                                name -> {
                                    removeFuture.complete(ApplicationID.fromHexString(name));
                                    return true;
                                })
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);

        applicationStore.putApplication(testingApplication);
        applicationStore
                .globalCleanupAsync(
                        testingApplication.getApplicationId(), Executors.directExecutor())
                .join();
        final ApplicationID actual = removeFuture.get(timeout, TimeUnit.MILLISECONDS);
        assertThat(actual).isEqualTo(testingApplication.getApplicationId());
    }

    @Test
    public void testGlobalCleanupWithNonExistName() throws Exception {
        final CompletableFuture<ApplicationID> removeFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setRemoveFunction(
                                name -> {
                                    removeFuture.complete(ApplicationID.fromHexString(name));
                                    return true;
                                })
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);
        applicationStore
                .globalCleanupAsync(
                        testingApplication.getApplicationId(), Executors.directExecutor())
                .join();

        assertThat(removeFuture).isDone();
    }

    @Test
    public void testGlobalCleanupFailsIfRemovalReturnsFalse() throws Exception {
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setRemoveFunction(name -> false).build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);
        assertThatThrownBy(
                        () ->
                                applicationStore
                                        .globalCleanupAsync(
                                                testingApplication.getApplicationId(),
                                                Executors.directExecutor())
                                        .get())
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    public void testGetApplicationIds() throws Exception {
        final Collection<ApplicationID> existingApplicationIds =
                Arrays.asList(new ApplicationID(), new ApplicationID());
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setGetAllHandlesSupplier(
                                () ->
                                        existingApplicationIds.stream()
                                                .map(ApplicationID::toString)
                                                .collect(Collectors.toList()))
                        .build();

        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);
        final Collection<ApplicationID> applicationIds = applicationStore.getApplicationIds();
        assertThat(applicationIds).containsExactlyInAnyOrderElementsOf(existingApplicationIds);
    }

    @Test
    public void testStoppingApplicationStoreShouldReleaseAllHandles() throws Exception {
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        final TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore =
                builder.setReleaseAllHandlesRunnable(() -> completableFuture.complete(null))
                        .build();
        final ApplicationStore applicationStore = createAndStartApplicationStore(stateHandleStore);
        applicationStore.stop();

        assertThat(completableFuture).isDone();
    }

    private ApplicationStore createAndStartApplicationStore(
            TestingStateHandleStore<ApplicationStoreEntry> stateHandleStore) throws Exception {
        final ApplicationStore applicationStore =
                new DefaultApplicationStore<>(
                        stateHandleStore,
                        new ApplicationStoreUtil() {
                            @Override
                            public String applicationIdToName(ApplicationID applicationId) {
                                return applicationId.toString();
                            }

                            @Override
                            public ApplicationID nameToApplicationId(String name) {
                                return ApplicationID.fromHexString(name);
                            }
                        });
        applicationStore.start();
        return applicationStore;
    }
}
