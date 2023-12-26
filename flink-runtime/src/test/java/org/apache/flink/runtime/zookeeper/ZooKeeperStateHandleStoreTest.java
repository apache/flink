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

package org.apache.flink.runtime.zookeeper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.runtime.util.ZooKeeperUtils.generateZookeeperPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for basic {@link ZooKeeperStateHandleStore} behaviour.
 *
 * <p>Tests include:
 *
 * <ul>
 *   <li>Expected usage of operations
 *   <li>Correct ordering of ZooKeeper and state handle operations
 * </ul>
 */
class ZooKeeperStateHandleStoreTest {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    final EachCallbackWrapper<ZooKeeperExtension> zooKeeperResource =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    @BeforeEach
    void cleanUp() {
        TestingLongStateHandleHelper.clearGlobalState();
    }

    private CuratorFramework getZooKeeperClient() {
        return zooKeeperExtension.getZooKeeperClient(
                testingFatalErrorHandlerResource.getTestingFatalErrorHandler());
    }

    /** Tests add operation with lock. */
    @Test
    void testAddAndLock() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();
        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        // Config
        final String pathInZooKeeper = "/testAdd";
        final long state = 1239712317L;

        // Test
        store.addAndLock(pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(state));

        // Verify
        // State handle created
        assertThat(store.getAllAndLock()).hasSize(1);
        assertThat(store.getAndLock(pathInZooKeeper).retrieveState().getValue()).isEqualTo(state);

        // Path created and is persistent
        Stat stat = getZooKeeperClient().checkExists().forPath(pathInZooKeeper);
        assertThat(stat).isNotNull();
        assertThat(stat.getEphemeralOwner()).isZero();

        List<String> children = getZooKeeperClient().getChildren().forPath(pathInZooKeeper);

        // There should be one child which is the locks subfolder
        final String locksSubfolderChild = Iterables.getOnlyElement(children);
        stat =
                getZooKeeperClient()
                        .checkExists()
                        .forPath(generateZookeeperPath(pathInZooKeeper, locksSubfolderChild));
        assertThat(stat).isNotNull();

        assertThat(stat.getEphemeralOwner())
                .as("The lock subfolder shouldn't be ephemeral")
                .isZero();

        List<String> lockChildren =
                getZooKeeperClient()
                        .getChildren()
                        .forPath(generateZookeeperPath(pathInZooKeeper, locksSubfolderChild));
        // Only one lock is expected
        final String lockChild = Iterables.getOnlyElement(lockChildren);
        stat =
                getZooKeeperClient()
                        .checkExists()
                        .forPath(
                                generateZookeeperPath(
                                        pathInZooKeeper, locksSubfolderChild, lockChild));

        assertThat(stat.getEphemeralOwner()).as("The lock node should be ephemeral").isNotZero();

        // Data is equal
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        getZooKeeperClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertThat(actual).isEqualTo(state);
    }

    @Test
    void testAddAndLockOnMarkedForDeletionNode() throws Exception {
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testAddAndLockOnMarkedForDeletionNode");
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(client, new TestingLongStateHandleHelper());

        final long oldStateValue = 1L;
        final TestingLongStateHandleHelper.LongStateHandle oldStateHandle =
                new TestingLongStateHandleHelper.LongStateHandle(oldStateValue);
        final String markedForDeletionNode = "marked-for-deletion";
        final String markedForDeletionNodePath = generateZookeeperPath(markedForDeletionNode);
        zkStore.addAndLock(markedForDeletionNodePath, oldStateHandle);

        markNodeForDeletion(client, markedForDeletionNode);

        final long updatedStateValue = oldStateValue + 2;
        final TestingLongStateHandleHelper.LongStateHandle updatedStateHandle =
                new TestingLongStateHandleHelper.LongStateHandle(updatedStateValue);
        zkStore.addAndLock(markedForDeletionNodePath, updatedStateHandle);

        assertThat(zkStore.getAndLock(markedForDeletionNodePath).retrieveState().getValue())
                .isEqualTo(updatedStateValue);
        assertThat(oldStateHandle.isDiscarded()).isTrue();
        assertThat(updatedStateHandle.isDiscarded()).isFalse();
    }

    @Test
    void testRepeatableCleanup() throws Exception {
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> testInstance =
                new ZooKeeperStateHandleStore<>(
                        getZooKeeperClient(), new TestingLongStateHandleHelper());

        final String pathInZooKeeper = "/testRepeatableCleanup";

        final RuntimeException expectedException =
                new RuntimeException("Expected RuntimeException");
        final TestingLongStateHandleHelper.LongStateHandle stateHandle =
                new TestingLongStateHandleHelper.LongStateHandle(
                        12354L, throwExceptionOnce(expectedException));

        testInstance.addAndLock(pathInZooKeeper, stateHandle);

        assertThatThrownBy(() -> testInstance.releaseAndTryRemove(pathInZooKeeper))
                .as("Exception should have been thrown.")
                .hasCause(expectedException);

        assertThatExceptionOfType(StateHandleStore.NotExistException.class)
                .isThrownBy(() -> testInstance.getAndLock(pathInZooKeeper));
        assertThat(stateHandle.isDiscarded()).isFalse();

        assertThat(testInstance.releaseAndTryRemove(pathInZooKeeper)).isTrue();
        assertThat(testInstance.exists(pathInZooKeeper).isExisting()).isFalse();
        assertThat(stateHandle.isDiscarded()).isTrue();
    }

    @Test
    void testCleanupOfNonExistingState() throws Exception {
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> testInstance =
                new ZooKeeperStateHandleStore<>(
                        getZooKeeperClient(), new TestingLongStateHandleHelper());

        final String pathInZooKeeper = "/testCleanupOfNonExistingState";

        assertThat(testInstance.releaseAndTryRemove(pathInZooKeeper)).isTrue();
        assertThat(testInstance.exists(pathInZooKeeper).isExisting()).isFalse();
    }

    @Test
    void testRepeatableCleanupWithLockOnNode() throws Exception {
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testRepeatableCleanupWithLockOnNode");

        final TestingLongStateHandleHelper storage = new TestingLongStateHandleHelper();
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>
                storeForCreation = new ZooKeeperStateHandleStore<>(client, storage);
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>
                storeForDeletion = new ZooKeeperStateHandleStore<>(client, storage);

        final String pathInZooKeeper = "/testRepeatableCleanupWithLock";

        final long actualValue = 12345L;
        final RuntimeException expectedException =
                new RuntimeException("RuntimeException for testing the failing discardState call.");
        final TestingLongStateHandleHelper.LongStateHandle stateHandle =
                new TestingLongStateHandleHelper.LongStateHandle(
                        actualValue, throwExceptionOnce(expectedException));

        final RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>
                handleForCreation = storeForCreation.addAndLock(pathInZooKeeper, stateHandle);

        final RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>
                handleForDeletion = storeForDeletion.getAndLock(pathInZooKeeper);

        assertThat(actualValue).isEqualTo(handleForCreation.retrieveState().getValue());
        assertThat(actualValue).isEqualTo(handleForDeletion.retrieveState().getValue());

        assertThat(storeForCreation.releaseAndTryRemove(pathInZooKeeper))
                .as(
                        "Deletion by the first StateHandleStore shouldn't be successful because there's still a lock from StateHandleStore #1.")
                .isFalse();

        assertThat(
                        client.checkExists()
                                .forPath(
                                        ZooKeeperStateHandleStore.getRootLockPath(pathInZooKeeper)))
                .as("StateHandle should not be marked for deletion, yet.")
                .isNotNull();
        assertThat(
                        client.checkExists()
                                .forPath(storeForCreation.getInstanceLockPath(pathInZooKeeper)))
                .as("The lock for storeForCreation should have been removed")
                .isNull();
        assertThat(stateHandle.getNumberOfDiscardCalls())
                .as("discardState shouldn't have been called, yet.")
                .isZero();

        assertThatThrownBy(() -> storeForDeletion.releaseAndTryRemove(pathInZooKeeper))
                .as("Exception should have been thrown.")
                .hasCause(expectedException);

        assertThat(
                        client.checkExists()
                                .forPath(
                                        ZooKeeperStateHandleStore.getRootLockPath(pathInZooKeeper)))
                .as("StateHandle should be marked for deletion.")
                .isNull();
        assertThat(client.checkExists().forPath(pathInZooKeeper))
                .as("StateHandle should not be deleted, yet.")
                .isNotNull();
        assertThat(stateHandle.isDiscarded())
                .as("The StateHandle should not be discarded, yet.")
                .isFalse();

        assertThat(storeForDeletion.releaseAndTryRemove(pathInZooKeeper)).isTrue();
        assertThat(client.checkExists().forPath(pathInZooKeeper))
                .as("The StateHandle node should have been removed")
                .isNull();
        assertThat(stateHandle.isDiscarded())
                .as("The StateHandle should have been discarded.")
                .isTrue();
    }

    /**
     * Tests that the created state handle is not discarded if ZooKeeper create fails with an
     * generic exception.
     */
    @Test
    void testFailingAddWithPossiblyInconsistentState() {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        CuratorFramework client = spy(getZooKeeperClient());
        when(client.inTransaction()).thenThrow(new RuntimeException("Expected test Exception."));

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(client, stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testAddDiscardStateHandleAfterFailure";
        final long state = 81282227L;

        assertThatExceptionOfType(PossibleInconsistentStateException.class)
                .as("PossibleInconsistentStateException should have been thrown.")
                .isThrownBy(
                        () ->
                                store.addAndLock(
                                        pathInZooKeeper,
                                        new TestingLongStateHandleHelper.LongStateHandle(state)));

        // State handle created and not discarded
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isOne();
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0)).isEqualTo(state);
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0))
                .isZero();
    }

    @Test
    void testAddAndLockExistingNode() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client = getZooKeeperClient();
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(client, stateHandleProvider);
        final String path = "/test";
        final long firstState = 1337L;
        final long secondState = 7331L;
        store.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(firstState));
        assertThatExceptionOfType(StateHandleStore.AlreadyExistException.class)
                .isThrownBy(
                        () ->
                                store.addAndLock(
                                        path,
                                        new TestingLongStateHandleHelper.LongStateHandle(
                                                secondState)));
        // There should be only single state handle from the first successful attempt.
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isOne();
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0))
                .isEqualTo(firstState);
        // No state should have been discarded.
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0))
                .isZero();
        // Get state handle from zookeeper.
        assertThat(store.getAndLock(path).retrieveState().getValue()).isEqualTo(firstState);
    }

    /**
     * Transactions are not idempotent in the Curator version we're currently using, therefore we
     * may end up retrying the transaction that has already (eg. in case of connection failure).
     * Retry of a successful transaction would result in {@link KeeperException.NodeExistsException}
     * in this case.
     *
     * @see <a href="https://issues.apache.org/jira/browse/CURATOR-584">CURATOR-584</a>
     */
    @Test
    void testAddAndLockRetrySuccessfulTransaction() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client = getZooKeeperClient();
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>(
                        client, stateHandleProvider) {

                    @Override
                    protected void writeStoreHandleTransactionally(
                            String path, byte[] serializedStoreHandle) throws Exception {
                        super.writeStoreHandleTransactionally(path, serializedStoreHandle);
                        throw new KeeperException.NodeExistsException(
                                "Committed transaction has been retried.");
                    }
                };
        final String path = "/test";
        final long firstState = 1337L;
        store.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(firstState));
        // There should be only single state handle from the first successful attempt.
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isOne();
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0))
                .isEqualTo(firstState);
        // No state should have been discarded.
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0))
                .isZero();
        // Get state handle from zookeeper.
        assertThat(store.getAndLock(path).retrieveState().getValue()).isEqualTo(firstState);
    }

    @Test
    void testAddFailureHandlingForBadArgumentsException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.BadArgumentsException());
    }

    @Test
    void testAddFailureHandlingForNoNodeException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.NoNodeException());
    }

    @Test
    void testAddFailureHandlingForNoAuthException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.NoAuthException());
    }

    @Test
    void testAddFailureHandlingForBadVersionException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.BadVersionException());
    }

    @Test
    void testAddFailureHandlingForAuthFailedException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.AuthFailedException());
    }

    @Test
    void testAddFailureHandlingForInvalidACLException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.InvalidACLException());
    }

    @Test
    void testAddFailureHandlingForSessionMovedException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.SessionMovedException());
    }

    @Test
    void testAddFailureHandlingForNotReadOnlyException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.NotReadOnlyException());
    }

    private void testFailingAddWithStateDiscardTriggeredFor(Exception actualException) {
        testFailingAddWithStateDiscardTriggeredFor(actualException, actualException.getClass());
    }

    private void testFailingAddWithStateDiscardTriggeredFor(
            Exception actualException, Class<? extends Throwable> expectedException) {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>(
                        getZooKeeperClient(), stateHandleProvider) {
                    @Override
                    protected void writeStoreHandleTransactionally(
                            String path, byte[] serializedStoreHandle) throws Exception {
                        throw actualException;
                    }
                };

        // Config
        final String pathInZooKeeper =
                "/testAddDiscardStateHandleAfterFailure-" + expectedException.getSimpleName();
        final long state = 81282227L;

        assertThatExceptionOfType(expectedException)
                .as(expectedException.getSimpleName() + " should have been thrown.")
                .isThrownBy(
                        () ->
                                store.addAndLock(
                                        pathInZooKeeper,
                                        new TestingLongStateHandleHelper.LongStateHandle(state)));

        // State handle created and discarded
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isOne();
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0)).isEqualTo(state);
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0))
                .isOne();
    }

    /** Tests that a state handle is replaced. */
    @Test
    void testReplace() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testReplace";
        final long initialState = 30968470898L;
        final long replaceState = 88383776661L;

        // Test
        store.addAndLock(
                pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(initialState));
        store.replace(
                pathInZooKeeper,
                IntegerResourceVersion.valueOf(0),
                new TestingLongStateHandleHelper.LongStateHandle(replaceState));

        // Verify
        // State handles created
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isEqualTo(2);
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0))
                .isEqualTo(initialState);
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(1))
                .isEqualTo(replaceState);

        // Path created and is persistent
        Stat stat = getZooKeeperClient().checkExists().forPath(pathInZooKeeper);
        assertThat(stat).isNotNull();
        assertThat(stat.getEphemeralOwner()).isZero();

        // Data is equal
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        getZooKeeperClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertThat(actual).isEqualTo(replaceState);
    }

    @Test
    void testReplaceRequiringALock() throws Exception {
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testReplaceOnMarkedForDeletionNode");
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(client, new TestingLongStateHandleHelper());

        final long oldState = 1L;
        final TestingLongStateHandleHelper.LongStateHandle oldStateHandle =
                new TestingLongStateHandleHelper.LongStateHandle(oldState);
        final String nodeName = "node";
        final String path = generateZookeeperPath(nodeName);

        zkStore.addAndLock(path, oldStateHandle);
        zkStore.release(path);

        final IntegerResourceVersion versionBeforeDeletion = zkStore.exists(path);
        final long updatedState = oldState + 2;
        final TestingLongStateHandleHelper.LongStateHandle updatedStateHandle =
                new TestingLongStateHandleHelper.LongStateHandle(updatedState);

        assertThatThrownBy(() -> zkStore.replace(path, versionBeforeDeletion, updatedStateHandle))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Tests that a non existing path throws an Exception. */
    @Test
    void testReplaceNonExistingPath() {
        final RetrievableStateStorageHelper<TestingLongStateHandleHelper.LongStateHandle>
                stateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateStorage);

        assertThatThrownBy(
                        () ->
                                store.replace(
                                        "/testReplaceNonExistingPath",
                                        IntegerResourceVersion.valueOf(0),
                                        new TestingLongStateHandleHelper.LongStateHandle(1L)))
                .isInstanceOf(Exception.class);
    }

    /** Tests that the replace state handle is discarded if ZooKeeper setData fails. */
    @Test
    void testReplaceDiscardStateHandleAfterFailure() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        CuratorFramework client = spy(getZooKeeperClient());
        when(client.setData()).thenThrow(new RuntimeException("Expected test Exception."));

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(client, stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testReplaceDiscardStateHandleAfterFailure";
        final long initialState = 30968470898L;
        final long replaceState = 88383776661L;

        // Test
        store.addAndLock(
                pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(initialState));

        assertThatExceptionOfType(Exception.class)
                .as("Did not throw expected exception")
                .isThrownBy(
                        () ->
                                store.replace(
                                        pathInZooKeeper,
                                        IntegerResourceVersion.valueOf(0),
                                        new TestingLongStateHandleHelper.LongStateHandle(
                                                replaceState)));

        // Verify
        // State handle created and discarded
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isEqualTo(2);
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0))
                .isEqualTo(initialState);
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(1))
                .isEqualTo(replaceState);
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0))
                .isZero();
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(1))
                .isZero();

        // Initial value
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        getZooKeeperClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertThat(actual).isEqualTo(initialState);
    }

    @Test
    void testDiscardAfterReplaceFailureWithNoNodeException() throws Exception {
        testDiscardAfterReplaceFailureWith(
                new KeeperException.NoNodeException(), StateHandleStore.NotExistException.class);
    }

    @Test
    void testDiscardAfterReplaceFailureWithNodeExistsException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.NodeExistsException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithBadArgumentsException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.BadArgumentsException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithNoAuthException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.NoAuthException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithBadVersionException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.BadVersionException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithAuthFailedException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.AuthFailedException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithInvalidACLException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.InvalidACLException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithSessionMovedException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.SessionMovedException());
    }

    @Test
    void testDiscardAfterReplaceFailureWithNotReadOnlyException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.NotReadOnlyException());
    }

    private void testDiscardAfterReplaceFailureWith(Exception actualException) throws Exception {
        testDiscardAfterReplaceFailureWith(actualException, actualException.getClass());
    }

    private void testDiscardAfterReplaceFailureWith(
            Exception actualException, Class<? extends Throwable> expectedExceptionClass)
            throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>(
                        getZooKeeperClient(), stateHandleProvider) {
                    @Override
                    protected void setStateHandle(
                            String path, byte[] serializedStateHandle, int expectedVersion)
                            throws Exception {
                        throw actualException;
                    }
                };

        // Config
        final String pathInZooKeeper =
                "/testReplaceDiscardStateHandleAfterFailure-"
                        + expectedExceptionClass.getSimpleName();
        final long initialState = 30968470898L;
        final long replaceState = 88383776661L;

        // Test
        store.addAndLock(
                pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(initialState));

        assertThatThrownBy(
                        () ->
                                store.replace(
                                        pathInZooKeeper,
                                        IntegerResourceVersion.valueOf(0),
                                        new TestingLongStateHandleHelper.LongStateHandle(
                                                replaceState)))
                .as("Did not throw expected exception")
                .isInstanceOf(expectedExceptionClass);

        // State handle created and discarded
        assertThat(TestingLongStateHandleHelper.getGlobalStorageSize()).isEqualTo(2);
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(0))
                .isEqualTo(initialState);
        assertThat(TestingLongStateHandleHelper.getStateHandleValueByIndex(1))
                .isEqualTo(replaceState);
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0))
                .isZero();
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(1))
                .isOne();

        // Initial value
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        getZooKeeperClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertThat(actual).isEqualTo(initialState);
    }

    /** Tests get operation. */
    @Test
    void testGetAndExists() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testGetAndExists";
        final long state = 311222268470898L;

        // Test
        assertThat(store.exists(pathInZooKeeper).isExisting()).isFalse();

        store.addAndLock(pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(state));
        RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle> actual =
                store.getAndLock(pathInZooKeeper);

        // Verify
        assertThat(actual.retrieveState().getValue()).isEqualTo(state);
        assertThat(store.exists(pathInZooKeeper).getValue()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testExistsOnMarkedForDeletionNode() throws Exception {
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testExistsOnMarkedForDeletionEntry");
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(client, new TestingLongStateHandleHelper());

        final String markedForDeletionPath = "marked-for-deletion";
        zkStore.addAndLock(
                generateZookeeperPath(markedForDeletionPath),
                new TestingLongStateHandleHelper.LongStateHandle(1L));

        markNodeForDeletion(client, markedForDeletionPath);

        assertThat(zkStore.exists(generateZookeeperPath(markedForDeletionPath)).isExisting())
                .isFalse();
    }

    /** Tests that a non existing path throws an Exception. */
    @Test
    void testGetNonExistingPath() {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateHandleProvider);

        assertThatThrownBy(() -> store.getAndLock("/testGetNonExistingPath"))
                .isInstanceOf(Exception.class);
    }

    /** Tests that all added state is returned. */
    @Test
    void testGetAll() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testGetAll";

        final Set<Long> expected = new HashSet<>();
        expected.add(311222268470898L);
        expected.add(132812888L);
        expected.add(27255442L);
        expected.add(11122233124L);

        // Test
        for (long val : expected) {
            store.addAndLock(
                    pathInZooKeeper + val, new TestingLongStateHandleHelper.LongStateHandle(val));
        }

        for (Tuple2<RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>, String>
                val : store.getAllAndLock()) {
            assertThat(expected.remove(val.f0.retrieveState().getValue())).isTrue();
        }
        assertThat(expected).isEmpty();
    }

    @Test
    void testGetAllAndLockOnConcurrentDelete() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testGetAllAndLockOnConcurrentDelete");

        // this store simulates the ZooKeeper connection for maintaining the lifecycle (i.e.
        // creating and deleting the nodes) of the StateHandles
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>
                storeForCreationAndDeletion =
                        new ZooKeeperStateHandleStore<>(client, stateHandleProvider);

        // this store simulates a concurrent access to ZooKeeper
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>
                storeForRetrieval = new ZooKeeperStateHandleStore<>(client, stateHandleProvider);

        final String pathInZooKeeperPrefix = "/node";

        final long stateForDeletion = 42L;
        final String handlePathForDeletion = pathInZooKeeperPrefix + "-for-deletion";
        storeForCreationAndDeletion.addAndLock(
                handlePathForDeletion,
                new TestingLongStateHandleHelper.LongStateHandle(stateForDeletion));

        final long stateToKeep = stateForDeletion + 2;
        storeForCreationAndDeletion.addAndLock(
                pathInZooKeeperPrefix + "-keep",
                new TestingLongStateHandleHelper.LongStateHandle(stateToKeep));

        final List<
                        Tuple2<
                                RetrievableStateHandle<
                                        TestingLongStateHandleHelper.LongStateHandle>,
                                String>>
                actuallyLockedHandles =
                        storeForRetrieval.getAllAndLock(
                                parentPath -> {
                                    final List<String> childNodes =
                                            client.getChildren().forPath(parentPath);
                                    // the following block simulates the concurrent deletion of the
                                    // child node after the node names are delivered to the
                                    // storeForRetrieval causing a retry
                                    if (storeForCreationAndDeletion
                                            .exists(handlePathForDeletion)
                                            .isExisting()) {
                                        storeForCreationAndDeletion.releaseAndTryRemove(
                                                handlePathForDeletion);
                                    }

                                    return childNodes;
                                });

        assertThat(Iterables.getOnlyElement(actuallyLockedHandles).f0.retrieveState().getValue())
                .as("Only the StateHandle that was expected to be kept should be returned.")
                .isEqualTo(stateToKeep);
    }

    @Test
    void testGetAllAndLockWhileEntryIsMarkedForDeletion() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testGetAllAndLockWhileEntryIsMarkedForDeletion");

        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>
                stateHandleStore = new ZooKeeperStateHandleStore<>(client, stateHandleProvider);

        final String pathInZooKeeperPrefix = "/node";

        final long stateForDeletion = 42L;
        final String handlePathForDeletion = pathInZooKeeperPrefix + "-for-deletion";
        stateHandleStore.addAndLock(
                handlePathForDeletion,
                new TestingLongStateHandleHelper.LongStateHandle(stateForDeletion));
        // marks the entry for deletion but doesn't delete it, yet
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath(ZooKeeperStateHandleStore.getRootLockPath(handlePathForDeletion));

        final long stateToKeep = stateForDeletion + 2;
        stateHandleStore.addAndLock(
                pathInZooKeeperPrefix + "-keep",
                new TestingLongStateHandleHelper.LongStateHandle(stateToKeep));

        final List<
                        Tuple2<
                                RetrievableStateHandle<
                                        TestingLongStateHandleHelper.LongStateHandle>,
                                String>>
                actuallyLockedHandles = stateHandleStore.getAllAndLock();

        assertThat(Iterables.getOnlyElement(actuallyLockedHandles).f0.retrieveState().getValue())
                .as("Only the StateHandle that was expected to be kept should be returned.")
                .isEqualTo(stateToKeep);
    }

    /** Tests that the state is returned sorted. */
    @Test
    void testGetAllSortedByName() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateHandleProvider);

        // Config
        final String basePath = "/testGetAllSortedByName";

        final Long[] expected = new Long[] {311222268470898L, 132812888L, 27255442L, 11122233124L};

        // Test
        for (long val : expected) {
            final String pathInZooKeeper = String.format("%s%016d", basePath, val);
            store.addAndLock(
                    pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(val));
        }

        List<Tuple2<RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>, String>>
                actual = store.getAllAndLock();
        assertThat(actual).hasSize(expected.length);

        // bring the elements in sort order
        Arrays.sort(expected);
        actual.sort(Comparator.comparing(o -> o.f1));

        for (int i = 0; i < expected.length; i++) {
            assertThat((Long) actual.get(i).f0.retrieveState().getValue()).isEqualTo(expected[i]);
        }
    }

    /** Tests that state handles are correctly removed. */
    @Test
    void testRemove() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testRemove";

        store.addAndLock(
                pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(27255442L));

        final int numberOfGlobalDiscardCalls = TestingLongStateHandleHelper.getGlobalDiscardCount();

        // Test
        store.releaseAndTryRemove(pathInZooKeeper);

        // Verify discarded
        assertThat(getZooKeeperClient().getChildren().forPath("/")).isEmpty();
        assertThat(TestingLongStateHandleHelper.getGlobalDiscardCount())
                .isEqualTo(numberOfGlobalDiscardCalls + 1);
    }

    /**
     * Tests that the ZooKeeperStateHandleStore can handle corrupted data by releasing and trying to
     * remove the respective ZooKeeper ZNodes.
     */
    @Test
    void testCorruptedData() throws Exception {
        final TestingLongStateHandleHelper stateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), stateStorage);

        final Collection<Long> input = new HashSet<>();
        input.add(1L);
        input.add(2L);
        input.add(3L);

        for (Long aLong : input) {
            store.addAndLock("/" + aLong, new TestingLongStateHandleHelper.LongStateHandle(aLong));
        }

        // corrupt one of the entries
        getZooKeeperClient().setData().forPath("/" + 2, new byte[2]);

        List<Tuple2<RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>, String>>
                allEntries = store.getAllAndLock();

        Collection<Long> expected = new HashSet<>(input);
        expected.remove(2L);

        Collection<Long> actual = new HashSet<>(expected.size());

        for (Tuple2<RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>, String>
                entry : allEntries) {
            actual.add(entry.f0.retrieveState().getValue());
        }

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that a concurrent delete operation cannot succeed if another instance holds a lock
     * on the specified node.
     */
    @Test
    void testConcurrentDeleteOperation() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore1 =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore2 =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        final String statePath = "/state";

        zkStore1.addAndLock(statePath, new TestingLongStateHandleHelper.LongStateHandle(42L));
        RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle> stateHandle =
                zkStore2.getAndLock(statePath);

        // this should not remove the referenced node because we are still holding a state handle
        // reference via zkStore2
        zkStore1.releaseAndTryRemove(statePath);

        // sanity check
        assertThat(stateHandle.retrieveState().getValue()).isEqualTo(42L);

        Stat nodeStat = getZooKeeperClient().checkExists().forPath(statePath);

        assertThat(nodeStat)
                .as("NodeStat should not be null, otherwise the referenced node does not exist.")
                .isNotNull();

        zkStore2.releaseAndTryRemove(statePath);

        nodeStat = getZooKeeperClient().checkExists().forPath(statePath);

        assertThat(nodeStat)
                .as("NodeState should be null, because the referenced node should no longer exist.")
                .isNull();
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that getAndLock removes a created lock if the RetrievableStateHandle cannot be
     * retrieved (e.g. deserialization problem).
     */
    @Test
    void testLockCleanupWhenGetAndLockFails() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore1 =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore2 =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        final String path = "/state";

        zkStore1.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));

        final byte[] corruptedData = {1, 2};

        // corrupt the data
        getZooKeeperClient().setData().forPath(path, corruptedData);

        assertThatExceptionOfType(IOException.class)
                .as("Should fail because we cannot deserialize the node's data")
                .isThrownBy(() -> zkStore2.getAndLock(path));

        // check that there is no lock node left
        String lockNodePath = zkStore2.getInstanceLockPath(path);

        Stat stat = getZooKeeperClient().checkExists().forPath(lockNodePath);

        // zkStore2 should not have created a lock node
        assertThat(stat).as("zkStore2 should not have created a lock node.").isNull();

        Collection<String> children = getZooKeeperClient().getChildren().forPath(path);

        // there should be exactly one lock node from zkStore1
        assertThat(children).hasSize(1);

        zkStore1.releaseAndTryRemove(path);

        stat = getZooKeeperClient().checkExists().forPath(path);

        assertThat(stat).as("The state node should have been removed.").isNull();
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that lock nodes will be released if the client dies.
     */
    @Test
    void testLockCleanupWhenClientTimesOut() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
        configuration.setInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 100);
        configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, "timeout");

        try (CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                        ZooKeeperUtils.startCuratorFramework(
                                configuration, NoOpFatalErrorHandler.INSTANCE);
                CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper2 =
                        ZooKeeperUtils.startCuratorFramework(
                                configuration, NoOpFatalErrorHandler.INSTANCE)) {

            CuratorFramework client = curatorFrameworkWrapper.asCuratorFramework();
            CuratorFramework client2 = curatorFrameworkWrapper2.asCuratorFramework();
            ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                    new ZooKeeperStateHandleStore<>(client, longStateStorage);

            final String path = "/state";

            zkStore.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));

            // this should delete all ephemeral nodes
            client.close();

            Stat stat = client2.checkExists().forPath(path);

            // check that our state node still exists
            assertThat(stat).isNotNull();

            Collection<String> children =
                    client2.getChildren().forPath(ZooKeeperStateHandleStore.getRootLockPath(path));

            // check that the lock node has been released
            assertThat(children).isEmpty();
        }
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that we can release a locked state handles in the ZooKeeperStateHandleStore.
     */
    @Test
    void testRelease() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        final String path = "/state";

        zkStore.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));

        final String lockPath = zkStore.getInstanceLockPath(path);

        Stat stat = getZooKeeperClient().checkExists().forPath(lockPath);

        assertThat(stat).as("Expected an existing lock").isNotNull();

        zkStore.release(path);

        stat =
                getZooKeeperClient()
                        .checkExists()
                        .forPath(ZooKeeperStateHandleStore.getRootLockPath(path));

        // release should have removed the lock child
        assertThat(stat.getNumChildren()).as("Expected no lock nodes as children").isZero();

        zkStore.releaseAndTryRemove(path);

        stat = getZooKeeperClient().checkExists().forPath(path);

        assertThat(stat).as("State node should have been removed.").isNull();
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that we can release all locked state handles in the ZooKeeperStateHandleStore.
     */
    @Test
    void testReleaseAll() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(getZooKeeperClient(), longStateStorage);

        final Collection<String> paths = Arrays.asList("/state1", "/state2", "/state3");

        for (String path : paths) {
            zkStore.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));
        }

        for (String path : paths) {
            Stat stat =
                    getZooKeeperClient().checkExists().forPath(zkStore.getInstanceLockPath(path));

            assertThat(stat).as("Expecte and existing lock.").isNotNull();
        }

        zkStore.releaseAll();

        for (String path : paths) {
            Stat stat =
                    getZooKeeperClient()
                            .checkExists()
                            .forPath(ZooKeeperStateHandleStore.getRootLockPath(path));

            assertThat(stat.getNumChildren()).isZero();
        }
    }

    @Test
    void testRemoveAllHandlesShouldRemoveAllPaths() throws Exception {
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(
                        ZooKeeperUtils.useNamespaceAndEnsurePath(getZooKeeperClient(), "/path"),
                        new TestingLongStateHandleHelper());

        zkStore.addAndLock("/state", new TestingLongStateHandleHelper.LongStateHandle(1L));
        zkStore.clearEntries();

        assertThat(zkStore.getAllHandles()).isEmpty();
    }

    @Test
    void testGetAllHandlesWithMarkedForDeletionEntries() throws Exception {
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        getZooKeeperClient(), "/testGetAllHandlesWithMarkedForDeletionEntries");
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(client, new TestingLongStateHandleHelper());

        final String notMarkedForDeletionNodeName = "not-marked-for-deletion";
        final String markedForDeletionNodeName = "marked-for-deletion";
        zkStore.addAndLock(
                generateZookeeperPath(notMarkedForDeletionNodeName),
                new TestingLongStateHandleHelper.LongStateHandle(1L));
        zkStore.addAndLock(
                generateZookeeperPath(markedForDeletionNodeName),
                new TestingLongStateHandleHelper.LongStateHandle(2L));

        markNodeForDeletion(client, markedForDeletionNodeName);

        assertThat(zkStore.getAllHandles())
                .containsExactlyInAnyOrder(notMarkedForDeletionNodeName, markedForDeletionNodeName);
    }

    private static void markNodeForDeletion(CuratorFramework client, String childNodeName)
            throws Exception {
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath(
                        ZooKeeperStateHandleStore.getRootLockPath(
                                generateZookeeperPath(childNodeName)));
    }

    private static TestingLongStateHandleHelper.PreDiscardCallback throwExceptionOnce(
            RuntimeException e) {
        return discardIdx -> {
            if (discardIdx == 0) {
                throw e;
            }
        };
    }
}
