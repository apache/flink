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
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.hamcrest.core.IsInstanceOf;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
public class ZooKeeperStateHandleStoreTest extends TestLogger {

    private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(1);

    @AfterClass
    public static void tearDown() throws Exception {
        ZOOKEEPER.shutdown();
    }

    @Before
    public void cleanUp() throws Exception {
        ZOOKEEPER.deleteAll();
        TestingLongStateHandleHelper.clearGlobalState();
    }

    /** Tests add operation with lock. */
    @Test
    public void testAddAndLock() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();
        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        // Config
        final String pathInZooKeeper = "/testAdd";
        final long state = 1239712317L;

        // Test
        store.addAndLock(pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(state));

        // Verify
        // State handle created
        assertEquals(1, store.getAllAndLock().size());
        assertEquals(state, store.getAndLock(pathInZooKeeper).retrieveState().getValue());

        // Path created and is persistent
        Stat stat = ZOOKEEPER.getClient().checkExists().forPath(pathInZooKeeper);
        assertNotNull(stat);
        assertEquals(0, stat.getEphemeralOwner());

        List<String> children = ZOOKEEPER.getClient().getChildren().forPath(pathInZooKeeper);

        // there should be one child which is the lock
        assertEquals(1, children.size());

        stat = ZOOKEEPER.getClient().checkExists().forPath(pathInZooKeeper + '/' + children.get(0));
        assertNotNull(stat);

        // check that the child is an ephemeral node
        assertNotEquals(0, stat.getEphemeralOwner());

        // Data is equal
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertEquals(state, actual);
    }

    /**
     * Tests that the created state handle is not discarded if ZooKeeper create fails with an
     * generic exception.
     */
    @Test
    public void testFailingAddWithPossiblyInconsistentState() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        CuratorFramework client = spy(ZOOKEEPER.getClient());
        when(client.inTransaction()).thenThrow(new RuntimeException("Expected test Exception."));

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(client, stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testAddDiscardStateHandleAfterFailure";
        final long state = 81282227L;

        try {
            // Test
            store.addAndLock(
                    pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(state));
            fail("PossibleInconsistentStateException should have been thrown.");
        } catch (PossibleInconsistentStateException ignored) {
            // PossibleInconsistentStateException expected
        }

        // State handle created and not discarded
        assertEquals(1, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(state, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        assertEquals(0, TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0));
    }

    @Test
    public void testAddAndLockExistingNode() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client = ZOOKEEPER.getClient();
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(client, stateHandleProvider);
        final String path = "/test";
        final long firstState = 1337L;
        final long secondState = 7331L;
        store.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(firstState));
        assertThrows(
                StateHandleStore.AlreadyExistException.class,
                () ->
                        store.addAndLock(
                                path,
                                new TestingLongStateHandleHelper.LongStateHandle(secondState)));
        // There should be only single state handle from the first successful attempt.
        assertEquals(1, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(firstState, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        // No state should have been discarded.
        assertEquals(0, TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0));
        // Get state handle from zookeeper.
        assertEquals(firstState, store.getAndLock(path).retrieveState().getValue());
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
    public void testAddAndLockRetrySuccessfulTransaction() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client = ZOOKEEPER.getClient();
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
        assertEquals(1, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(firstState, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        // No state should have been discarded.
        assertEquals(0, TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0));
        // Get state handle from zookeeper.
        assertEquals(firstState, store.getAndLock(path).retrieveState().getValue());
    }

    @Test
    public void testAddFailureHandlingForBadArgumentsException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.BadArgumentsException());
    }

    @Test
    public void testAddFailureHandlingForNoNodeException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.NoNodeException());
    }

    @Test
    public void testAddFailureHandlingForNoAuthException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.NoAuthException());
    }

    @Test
    public void testAddFailureHandlingForBadVersionException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.BadVersionException());
    }

    @Test
    public void testAddFailureHandlingForAuthFailedException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.AuthFailedException());
    }

    @Test
    public void testAddFailureHandlingForInvalidACLException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.InvalidACLException());
    }

    @Test
    public void testAddFailureHandlingForSessionMovedException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.SessionMovedException());
    }

    @Test
    public void testAddFailureHandlingForNotReadOnlyException() {
        testFailingAddWithStateDiscardTriggeredFor(new KeeperException.NotReadOnlyException());
    }

    private static void testFailingAddWithStateDiscardTriggeredFor(Exception actualException) {
        testFailingAddWithStateDiscardTriggeredFor(actualException, actualException.getClass());
    }

    private static void testFailingAddWithStateDiscardTriggeredFor(
            Exception actualException, Class<? extends Throwable> expectedException) {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>(
                        ZOOKEEPER.getClient(), stateHandleProvider) {
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

        try {
            // Test
            store.addAndLock(
                    pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(state));
            fail(expectedException.getSimpleName() + " should have been thrown.");
        } catch (Exception ex) {
            assertThat(ex, IsInstanceOf.instanceOf(expectedException));
        }

        // State handle created and discarded
        assertEquals(1, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(state, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        assertEquals(1, TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0));
    }

    /** Tests that a state handle is replaced. */
    @Test
    public void testReplace() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

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
        assertEquals(2, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(initialState, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        assertEquals(replaceState, TestingLongStateHandleHelper.getStateHandleValueByIndex(1));

        // Path created and is persistent
        Stat stat = ZOOKEEPER.getClient().checkExists().forPath(pathInZooKeeper);
        assertNotNull(stat);
        assertEquals(0, stat.getEphemeralOwner());

        // Data is equal
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertEquals(replaceState, actual);
    }

    /** Tests that a non existing path throws an Exception. */
    @Test(expected = Exception.class)
    public void testReplaceNonExistingPath() throws Exception {
        final RetrievableStateStorageHelper<TestingLongStateHandleHelper.LongStateHandle>
                stateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateStorage);

        store.replace(
                "/testReplaceNonExistingPath",
                IntegerResourceVersion.valueOf(0),
                new TestingLongStateHandleHelper.LongStateHandle(1L));
    }

    /** Tests that the replace state handle is discarded if ZooKeeper setData fails. */
    @Test
    public void testReplaceDiscardStateHandleAfterFailure() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        CuratorFramework client = spy(ZOOKEEPER.getClient());
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

        try {
            store.replace(
                    pathInZooKeeper,
                    IntegerResourceVersion.valueOf(0),
                    new TestingLongStateHandleHelper.LongStateHandle(replaceState));
            fail("Did not throw expected exception");
        } catch (Exception ignored) {
        }

        // Verify
        // State handle created and discarded
        assertEquals(2, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(initialState, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        assertEquals(replaceState, TestingLongStateHandleHelper.getStateHandleValueByIndex(1));
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0), is(0));
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(1), is(0));

        // Initial value
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertEquals(initialState, actual);
    }

    @Test
    public void testDiscardAfterReplaceFailureWithNoNodeException() throws Exception {
        testDiscardAfterReplaceFailureWith(
                new KeeperException.NoNodeException(), StateHandleStore.NotExistException.class);
    }

    @Test
    public void testDiscardAfterReplaceFailureWithNodeExistsException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.NodeExistsException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithBadArgumentsException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.BadArgumentsException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithNoAuthException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.NoAuthException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithBadVersionException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.BadVersionException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithAuthFailedException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.AuthFailedException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithInvalidACLException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.InvalidACLException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithSessionMovedException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.SessionMovedException());
    }

    @Test
    public void testDiscardAfterReplaceFailureWithNotReadOnlyException() throws Exception {
        testDiscardAfterReplaceFailureWith(new KeeperException.NotReadOnlyException());
    }

    private static void testDiscardAfterReplaceFailureWith(Exception actualException)
            throws Exception {
        testDiscardAfterReplaceFailureWith(actualException, actualException.getClass());
    }

    private static void testDiscardAfterReplaceFailureWith(
            Exception actualException, Class<? extends Throwable> expectedException)
            throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle>(
                        ZOOKEEPER.getClient(), stateHandleProvider) {
                    @Override
                    protected void setStateHandle(
                            String path, byte[] serializedStateHandle, int expectedVersion)
                            throws Exception {
                        throw actualException;
                    }
                };

        // Config
        final String pathInZooKeeper =
                "/testReplaceDiscardStateHandleAfterFailure-" + expectedException.getSimpleName();
        final long initialState = 30968470898L;
        final long replaceState = 88383776661L;

        // Test
        store.addAndLock(
                pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(initialState));

        try {
            store.replace(
                    pathInZooKeeper,
                    IntegerResourceVersion.valueOf(0),
                    new TestingLongStateHandleHelper.LongStateHandle(replaceState));
            fail("Did not throw expected exception");
        } catch (Throwable t) {
            assertThat(t, IsInstanceOf.instanceOf(expectedException));
        }

        // State handle created and discarded
        assertEquals(2, TestingLongStateHandleHelper.getGlobalStorageSize());
        assertEquals(initialState, TestingLongStateHandleHelper.getStateHandleValueByIndex(0));
        assertEquals(replaceState, TestingLongStateHandleHelper.getStateHandleValueByIndex(1));
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(0), is(0));
        assertThat(TestingLongStateHandleHelper.getDiscardCallCountForStateHandleByIndex(1), is(1));

        // Initial value
        @SuppressWarnings("unchecked")
        final long actual =
                ((RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>)
                                InstantiationUtil.deserializeObject(
                                        ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
                                        ClassLoader.getSystemClassLoader()))
                        .retrieveState()
                        .getValue();

        assertEquals(initialState, actual);
    }

    /** Tests get operation. */
    @Test
    public void testGetAndExists() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testGetAndExists";
        final long state = 311222268470898L;

        // Test
        assertThat(store.exists(pathInZooKeeper).isExisting(), is(false));

        store.addAndLock(pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(state));
        RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle> actual =
                store.getAndLock(pathInZooKeeper);

        // Verify
        assertEquals(state, actual.retrieveState().getValue());
        assertTrue(store.exists(pathInZooKeeper).getValue() >= 0);
    }

    /** Tests that a non existing path throws an Exception. */
    @Test(expected = Exception.class)
    public void testGetNonExistingPath() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

        store.getAndLock("/testGetNonExistingPath");
    }

    /** Tests that all added state is returned. */
    @Test
    public void testGetAll() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

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
            assertTrue(expected.remove(val.f0.retrieveState().getValue()));
        }
        assertEquals(0, expected.size());
    }

    @Test
    public void testGetAllAndLockOnConcurrentDelete() throws Exception {
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();
        final CuratorFramework client =
                ZooKeeperUtils.useNamespaceAndEnsurePath(
                        ZOOKEEPER.getClient(), "/testGetAllAndLockOnConcurrentDelete");

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

        assertEquals(
                "Only the StateHandle that was expected to be kept should be returned.",
                stateToKeep,
                Iterables.getOnlyElement(actuallyLockedHandles).f0.retrieveState().getValue());
    }

    /** Tests that the state is returned sorted. */
    @Test
    public void testGetAllSortedByName() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

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
        assertEquals(expected.length, actual.size());

        // bring the elements in sort order
        Arrays.sort(expected);
        Collections.sort(actual, Comparator.comparing(o -> o.f1));

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], (Long) actual.get(i).f0.retrieveState().getValue());
        }
    }

    /** Tests that state handles are correctly removed. */
    @Test
    public void testRemove() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testRemove";

        store.addAndLock(
                pathInZooKeeper, new TestingLongStateHandleHelper.LongStateHandle(27255442L));

        final int numberOfGlobalDiscardCalls = TestingLongStateHandleHelper.getGlobalDiscardCount();

        // Test
        store.releaseAndTryRemove(pathInZooKeeper);

        // Verify discarded
        assertEquals(0, ZOOKEEPER.getClient().getChildren().forPath("/").size());
        assertEquals(
                numberOfGlobalDiscardCalls + 1,
                TestingLongStateHandleHelper.getGlobalDiscardCount());
    }

    /** Tests that all state handles are correctly discarded. */
    @Test
    public void testReleaseAndTryRemoveAll() throws Exception {
        // Setup
        final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateHandleProvider);

        // Config
        final String pathInZooKeeper = "/testDiscardAll";

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

        store.releaseAndTryRemoveAll();

        // Verify all discarded
        assertEquals(0, ZOOKEEPER.getClient().getChildren().forPath("/").size());
    }

    /**
     * Tests that the ZooKeeperStateHandleStore can handle corrupted data by releasing and trying to
     * remove the respective ZooKeeper ZNodes.
     */
    @Test
    public void testCorruptedData() throws Exception {
        final TestingLongStateHandleHelper stateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> store =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), stateStorage);

        final Collection<Long> input = new HashSet<>();
        input.add(1L);
        input.add(2L);
        input.add(3L);

        for (Long aLong : input) {
            store.addAndLock("/" + aLong, new TestingLongStateHandleHelper.LongStateHandle(aLong));
        }

        // corrupt one of the entries
        ZOOKEEPER.getClient().setData().forPath("/" + 2, new byte[2]);

        List<Tuple2<RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>, String>>
                allEntries = store.getAllAndLock();

        Collection<Long> expected = new HashSet<>(input);
        expected.remove(2L);

        Collection<Long> actual = new HashSet<>(expected.size());

        for (Tuple2<RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle>, String>
                entry : allEntries) {
            actual.add(entry.f0.retrieveState().getValue());
        }

        assertEquals(expected, actual);
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that a concurrent delete operation cannot succeed if another instance holds a lock
     * on the specified node.
     */
    @Test
    public void testConcurrentDeleteOperation() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore1 =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore2 =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        final String statePath = "/state";

        zkStore1.addAndLock(statePath, new TestingLongStateHandleHelper.LongStateHandle(42L));
        RetrievableStateHandle<TestingLongStateHandleHelper.LongStateHandle> stateHandle =
                zkStore2.getAndLock(statePath);

        // this should not remove the referenced node because we are still holding a state handle
        // reference via zkStore2
        zkStore1.releaseAndTryRemove(statePath);

        // sanity check
        assertEquals(42L, stateHandle.retrieveState().getValue());

        Stat nodeStat = ZOOKEEPER.getClient().checkExists().forPath(statePath);

        assertNotNull(
                "NodeStat should not be null, otherwise the referenced node does not exist.",
                nodeStat);

        zkStore2.releaseAndTryRemove(statePath);

        nodeStat = ZOOKEEPER.getClient().checkExists().forPath(statePath);

        assertNull(
                "NodeState should be null, because the referenced node should no longer exist.",
                nodeStat);
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that getAndLock removes a created lock if the RetrievableStateHandle cannot be
     * retrieved (e.g. deserialization problem).
     */
    @Test
    public void testLockCleanupWhenGetAndLockFails() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore1 =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore2 =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        final String path = "/state";

        zkStore1.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));

        final byte[] corruptedData = {1, 2};

        // corrupt the data
        ZOOKEEPER.getClient().setData().forPath(path, corruptedData);

        try {
            zkStore2.getAndLock(path);
            fail("Should fail because we cannot deserialize the node's data");
        } catch (IOException ignored) {
            // expected to fail
        }

        // check that there is no lock node left
        String lockNodePath = zkStore2.getLockPath(path);

        Stat stat = ZOOKEEPER.getClient().checkExists().forPath(lockNodePath);

        // zkStore2 should not have created a lock node
        assertNull("zkStore2 should not have created a lock node.", stat);

        Collection<String> children = ZOOKEEPER.getClient().getChildren().forPath(path);

        // there should be exactly one lock node from zkStore1
        assertEquals(1, children.size());

        zkStore1.releaseAndTryRemove(path);

        stat = ZOOKEEPER.getClient().checkExists().forPath(path);

        assertNull("The state node should have been removed.", stat);
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that lock nodes will be released if the client dies.
     */
    @Test
    public void testLockCleanupWhenClientTimesOut() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, ZOOKEEPER.getConnectString());
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
            assertNotNull(stat);

            Collection<String> children = client2.getChildren().forPath(path);

            // check that the lock node has been released
            assertEquals(0, children.size());
        }
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that we can release a locked state handles in the ZooKeeperStateHandleStore.
     */
    @Test
    public void testRelease() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        final String path = "/state";

        zkStore.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));

        final String lockPath = zkStore.getLockPath(path);

        Stat stat = ZOOKEEPER.getClient().checkExists().forPath(lockPath);

        assertNotNull("Expected an existing lock", stat);

        zkStore.release(path);

        stat = ZOOKEEPER.getClient().checkExists().forPath(path);

        // release should have removed the lock child
        assertEquals("Expected no lock nodes as children", 0, stat.getNumChildren());

        zkStore.releaseAndTryRemove(path);

        stat = ZOOKEEPER.getClient().checkExists().forPath(path);

        assertNull("State node should have been removed.", stat);
    }

    /**
     * FLINK-6612
     *
     * <p>Tests that we can release all locked state handles in the ZooKeeperStateHandleStore
     */
    @Test
    public void testReleaseAll() throws Exception {
        final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

        ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(ZOOKEEPER.getClient(), longStateStorage);

        final Collection<String> paths = Arrays.asList("/state1", "/state2", "/state3");

        for (String path : paths) {
            zkStore.addAndLock(path, new TestingLongStateHandleHelper.LongStateHandle(42L));
        }

        for (String path : paths) {
            Stat stat = ZOOKEEPER.getClient().checkExists().forPath(zkStore.getLockPath(path));

            assertNotNull("Expecte and existing lock.", stat);
        }

        zkStore.releaseAll();

        for (String path : paths) {
            Stat stat = ZOOKEEPER.getClient().checkExists().forPath(path);

            assertEquals(0, stat.getNumChildren());
        }

        zkStore.releaseAndTryRemoveAll();

        Stat stat = ZOOKEEPER.getClient().checkExists().forPath("/");

        assertEquals(0, stat.getNumChildren());
    }

    @Test
    public void testRemoveAllHandlesShouldRemoveAllPaths() throws Exception {
        final ZooKeeperStateHandleStore<TestingLongStateHandleHelper.LongStateHandle> zkStore =
                new ZooKeeperStateHandleStore<>(
                        ZooKeeperUtils.useNamespaceAndEnsurePath(ZOOKEEPER.getClient(), "/path"),
                        new TestingLongStateHandleHelper());

        zkStore.addAndLock("/state", new TestingLongStateHandleHelper.LongStateHandle(1L));
        zkStore.clearEntries();

        assertThat(zkStore.getAllHandles(), is(empty()));
    }
}
