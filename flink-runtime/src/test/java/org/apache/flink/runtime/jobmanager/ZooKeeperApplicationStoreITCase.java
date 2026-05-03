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
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * IT tests for {@link DefaultApplicationStore} with all ZooKeeper components(e.g. {@link
 * ZooKeeperStateHandleStore}, {@link ZooKeeperApplicationStoreUtil}).
 */
public class ZooKeeperApplicationStoreITCase extends TestLogger {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    private static final RetrievableStateStorageHelper<ApplicationStoreEntry> localStateStorage =
            applicationStoreEntry -> {
                ByteStreamStateHandle byteStreamStateHandle =
                        new ByteStreamStateHandle(
                                String.valueOf(java.util.UUID.randomUUID()),
                                InstantiationUtil.serializeObject(applicationStoreEntry));
                return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
            };

    @Test
    public void testPutAndRemoveApplication() throws Exception {
        ApplicationStore applicationStore =
                createZooKeeperApplicationStore("/testPutAndRemoveApplication");

        try {
            applicationStore.start();

            ApplicationStoreEntry applicationEntry =
                    createApplicationStoreEntry(new ApplicationID(), "AppName");

            // Empty state
            assertThat(applicationStore.getApplicationIds()).isEmpty();

            // Add initial
            applicationStore.putApplication(applicationEntry);

            // Verify initial application
            Collection<ApplicationID> applicationIds = applicationStore.getApplicationIds();
            assertThat(applicationIds).hasSize(1);

            ApplicationID applicationId = applicationIds.iterator().next();

            Optional<ApplicationStoreEntry> recoveredEntry =
                    applicationStore.recoverApplication(applicationId);
            assertThat(recoveredEntry).isPresent();
            verifyApplicationStoreEntries(applicationEntry, recoveredEntry.get());

            // Update (same ID)
            applicationEntry =
                    createApplicationStoreEntry(
                            applicationEntry.getApplicationId(), "Updated AppName");
            applicationStore.putApplication(applicationEntry);

            // Verify updated
            applicationIds = applicationStore.getApplicationIds();
            assertThat(applicationIds).hasSize(1);

            applicationId = applicationIds.iterator().next();

            recoveredEntry = applicationStore.recoverApplication(applicationId);
            assertThat(recoveredEntry).isPresent();
            verifyApplicationStoreEntries(applicationEntry, recoveredEntry.get());

            // Remove
            applicationStore
                    .globalCleanupAsync(
                            applicationEntry.getApplicationId(), Executors.directExecutor())
                    .join();

            // Empty state
            assertThat(applicationStore.getApplicationIds()).isEmpty();

            // Don't fail if called again
            applicationStore
                    .globalCleanupAsync(
                            applicationEntry.getApplicationId(), Executors.directExecutor())
                    .join();
        } finally {
            applicationStore.stop();
        }
    }

    @Nonnull
    private ApplicationStore createZooKeeperApplicationStore(String fullPath) throws Exception {
        final CuratorFramework client =
                zooKeeperExtension.getZooKeeperClient(
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
        // Ensure that the applications path exists
        client.newNamespaceAwareEnsurePath(fullPath).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + fullPath);
        final ZooKeeperStateHandleStore<ApplicationStoreEntry> zooKeeperStateHandleStore =
                new ZooKeeperStateHandleStore<>(facade, localStateStorage);
        return new DefaultApplicationStore<>(
                zooKeeperStateHandleStore, ZooKeeperApplicationStoreUtil.INSTANCE);
    }

    @Test
    public void testRecoverApplications() throws Exception {
        ApplicationStore applicationStore =
                createZooKeeperApplicationStore("/testRecoverApplications");

        try {
            applicationStore.start();

            HashMap<ApplicationID, ApplicationStoreEntry> expected = new HashMap<>();
            ApplicationID[] applicationIds =
                    new ApplicationID[] {
                        new ApplicationID(), new ApplicationID(), new ApplicationID()
                    };

            expected.put(applicationIds[0], createApplicationStoreEntry(applicationIds[0]));
            expected.put(applicationIds[1], createApplicationStoreEntry(applicationIds[1]));
            expected.put(applicationIds[2], createApplicationStoreEntry(applicationIds[2]));

            // Add all
            for (ApplicationStoreEntry applicationStoreEntry : expected.values()) {
                applicationStore.putApplication(applicationStoreEntry);
            }

            Collection<ApplicationID> actual = applicationStore.getApplicationIds();

            assertThat(actual).hasSameSizeAs(expected.entrySet());

            for (ApplicationID applicationId : actual) {
                Optional<ApplicationStoreEntry> applicationStoreEntry =
                        applicationStore.recoverApplication(applicationId);
                assertThat(applicationStoreEntry).isPresent();
                assertThat(expected).containsKey(applicationStoreEntry.get().getApplicationId());

                verifyApplicationStoreEntries(
                        expected.get(applicationStoreEntry.get().getApplicationId()),
                        applicationStoreEntry.get());

                applicationStore
                        .globalCleanupAsync(
                                applicationStoreEntry.get().getApplicationId(),
                                Executors.directExecutor())
                        .join();
            }

            // Empty state
            assertThat(applicationStore.getApplicationIds()).isEmpty();
        } finally {
            applicationStore.stop();
        }
    }

    @Test
    public void testUpdateApplicationYouDidNotGetOrAdd() throws Exception {
        ApplicationStore applicationStore =
                createZooKeeperApplicationStore("/testUpdateApplicationYouDidNotGetOrAdd");

        ApplicationStore otherApplicationStore =
                createZooKeeperApplicationStore("/testUpdateApplicationYouDidNotGetOrAdd");

        applicationStore.start();
        otherApplicationStore.start();

        ApplicationStoreEntry applicationEntry = createApplicationStoreEntry(new ApplicationID());

        applicationStore.putApplication(applicationEntry);

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> otherApplicationStore.putApplication(applicationEntry));

        applicationStore.stop();
        otherApplicationStore.stop();
    }

    /**
     * Tests that we fail with an exception if the application cannot be removed from the
     * ZooKeeperApplicationStore.
     *
     * <p>Tests that a close ZooKeeperApplicationStore no longer holds any locks.
     */
    @Test
    public void testApplicationRemovalFailureAndLockRelease() throws Exception {
        final ApplicationStore submittedApplicationStore =
                createZooKeeperApplicationStore("/testApplicationRemovalFailureAndLockRelease");
        final ApplicationStore otherSubmittedApplicationStore =
                createZooKeeperApplicationStore("/testApplicationRemovalFailureAndLockRelease");

        submittedApplicationStore.start();
        otherSubmittedApplicationStore.start();

        final ApplicationStoreEntry applicationEntry =
                createApplicationStoreEntry(new ApplicationID());
        submittedApplicationStore.putApplication(applicationEntry);

        final Optional<ApplicationStoreEntry> recoveredApplicationEntry =
                otherSubmittedApplicationStore.recoverApplication(
                        applicationEntry.getApplicationId());

        assertThat(recoveredApplicationEntry).isPresent();

        assertThatExceptionOfType(Exception.class)
                .as(
                        "It should not be possible to remove the ApplicationStoreEntry since the first store still has a lock on it.")
                .isThrownBy(
                        () ->
                                otherSubmittedApplicationStore
                                        .globalCleanupAsync(
                                                recoveredApplicationEntry.get().getApplicationId(),
                                                Executors.directExecutor())
                                        .join());

        submittedApplicationStore.stop();

        // now we should be able to delete the application entry
        otherSubmittedApplicationStore
                .globalCleanupAsync(
                        recoveredApplicationEntry.get().getApplicationId(),
                        Executors.directExecutor())
                .join();

        assertThat(
                        otherSubmittedApplicationStore.recoverApplication(
                                recoveredApplicationEntry.get().getApplicationId()))
                .isEmpty();

        otherSubmittedApplicationStore.stop();
    }

    // ---------------------------------------------------------------------------------------------

    private ApplicationStoreEntry createApplicationStoreEntry(ApplicationID applicationId) {
        return createApplicationStoreEntry(applicationId, "Test Application");
    }

    private ApplicationStoreEntry createApplicationStoreEntry(
            ApplicationID applicationId, String name) {
        return TestingApplicationStoreEntry.newBuilder()
                .setApplicationId(applicationId)
                .setName(name)
                .build();
    }

    private void verifyApplicationStoreEntries(
            ApplicationStoreEntry expected, ApplicationStoreEntry actual) {
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getApplicationId()).isEqualTo(expected.getApplicationId());
    }
}
