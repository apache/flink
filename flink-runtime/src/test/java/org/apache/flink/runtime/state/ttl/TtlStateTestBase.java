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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.StateMigrationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.function.Consumer;

import static org.apache.flink.runtime.state.ttl.StateBackendTestContext.NUMBER_OF_KEY_GROUPS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/** State TTL base test suite. */
@RunWith(Parameterized.class)
public abstract class TtlStateTestBase {
	protected static final long TTL = 100;
	private static final int INC_CLEANUP_ALL_KEYS =
		((CopyOnWriteStateMap.DEFAULT_CAPACITY >> 1) + (CopyOnWriteStateMap.DEFAULT_CAPACITY >> 2) + 1) * NUMBER_OF_KEY_GROUPS;

	protected MockTtlTimeProvider timeProvider;
	protected StateBackendTestContext sbetc;

	protected static final String UNEXPIRED_AVAIL = "Unexpired state should be available";
	protected static final String UPDATED_UNEXPIRED_AVAIL = "Unexpired state should be available after update";
	protected static final String EXPIRED_UNAVAIL = "Expired state should be unavailable";
	private static final String EXPIRED_AVAIL = "Expired state should be available";

	private StateTtlConfig ttlConfig;

	@Before
	public void setup() {
		timeProvider = new MockTtlTimeProvider();
		sbetc = createStateBackendTestContext(timeProvider);
	}

	protected abstract StateBackendTestContext createStateBackendTestContext(TtlTimeProvider timeProvider);

	@Parameterized.Parameter
	public TtlStateTestContextBase<?, ?, ?> ctx;

	@Parameterized.Parameters(name = "{0}")
	public static List<TtlStateTestContextBase<?, ?, ?>> testContexts() {
		return Arrays.asList(
			new TtlValueStateTestContext(),
			new TtlFixedLenElemListStateTestContext(),
			new TtlNonFixedLenElemListStateTestContext(),
			new TtlMapStateAllEntriesTestContext(),
			new TtlMapStatePerElementTestContext(),
			new TtlMapStatePerNullElementTestContext(),
			new TtlAggregatingStateTestContext(),
			new TtlReducingStateTestContext(),
			new TtlFoldingStateTestContext());
	}

	public boolean fullSnapshot() {
		return true;
	}

	@SuppressWarnings("unchecked")
	protected <S extends InternalKvState<?, String, ?>, UV> TtlStateTestContextBase<S, UV, ?> ctx() {
		return (TtlStateTestContextBase<S, UV, ?>) ctx;
	}

	@SuppressWarnings("unchecked")
	private <UV> TtlMergingStateTestContext<?, UV, ?> mctx() {
		return (TtlMergingStateTestContext<?, UV, ?>) ctx;
	}

	private void initTest() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnCreateAndWrite, StateTtlConfig.StateVisibility.NeverReturnExpired);
	}

	private void initTest(
				StateTtlConfig.UpdateType updateType,
				StateTtlConfig.StateVisibility visibility) throws Exception {
		initTest(updateType, visibility, TTL);
	}

	private void initTest(
		StateTtlConfig.UpdateType updateType,
		StateTtlConfig.StateVisibility visibility,
		long ttl) throws Exception {
		initTest(getConfBuilder(ttl)
			.setUpdateType(updateType)
			.setStateVisibility(visibility)
			.disableCleanupInBackground()
			.build());
	}

	protected static StateTtlConfig.Builder getConfBuilder(long ttl) {
		return StateTtlConfig.newBuilder(Time.milliseconds(ttl));
	}

	protected <S extends State> StateDescriptor<S, Object> initTest(StateTtlConfig ttlConfig) throws Exception {
		this.ttlConfig = ttlConfig;
		sbetc.createAndRestoreKeyedStateBackend(null);
		sbetc.setCurrentKey("defaultKey");
		StateDescriptor<S, Object> stateDesc = createState();
		ctx().initTestValues();
		return stateDesc;
	}

	@SuppressWarnings("unchecked")
	private <S extends State> StateDescriptor<S, Object> createState() throws Exception {
		StateDescriptor<S, Object> stateDescriptor = ctx().createStateDescriptor();
		stateDescriptor.enableTimeToLive(ttlConfig);
		ctx().ttlState =
			(InternalKvState<?, String, ?>) sbetc.createState(stateDescriptor, "defaultNamespace");
		return stateDescriptor;
	}

	private void takeAndRestoreSnapshot() throws Exception {
		restoreSnapshot(sbetc.takeSnapshot(), NUMBER_OF_KEY_GROUPS);
	}

	protected void takeAndRestoreSnapshot(int numberOfKeyGroupsAfterRestore) throws Exception {
		restoreSnapshot(sbetc.takeSnapshot(), numberOfKeyGroupsAfterRestore);
	}

	private void restoreSnapshot(KeyedStateHandle snapshot, int numberOfKeyGroups) throws Exception {
		sbetc.createAndRestoreKeyedStateBackend(numberOfKeyGroups, snapshot);
		sbetc.setCurrentKey("defaultKey");
		createState();
	}

	protected boolean incrementalCleanupSupported() {
		return false;
	}

	@Test
	public void testNonExistentValue() throws Exception {
		initTest();
		assertEquals("Non-existing state should be empty", ctx().emptyValue, ctx().get());
	}

	@Test
	public void testExactExpirationOnWrite() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnCreateAndWrite, StateTtlConfig.StateVisibility.NeverReturnExpired);

		takeAndRestoreSnapshot();

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 20;
		assertEquals(UNEXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		ctx().update(ctx().updateUnexpired);

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		assertEquals(UPDATED_UNEXPIRED_AVAIL, ctx().getUnexpired, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 170;
		ctx().update(ctx().updateExpired);

		takeAndRestoreSnapshot();

		timeProvider.time = 220;
		assertEquals(UPDATED_UNEXPIRED_AVAIL, ctx().getUpdateExpired, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 300;
		assertEquals(EXPIRED_UNAVAIL, ctx().emptyValue, ctx().get());
		assertEquals("Original state should be cleared on access", ctx().emptyValue, ctx().getOriginal());
	}

	@Test
	public void testRelaxedExpirationOnWrite() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnCreateAndWrite, StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		assertEquals(EXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());
		assertEquals("Original state should be cleared on access", ctx().emptyValue, ctx().getOriginal());
		assertEquals("Expired state should be cleared on access", ctx().emptyValue, ctx().get());
	}

	@Test
	public void testExactExpirationOnRead() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnReadAndWrite, StateTtlConfig.StateVisibility.NeverReturnExpired);

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		assertEquals(UNEXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		assertEquals("Unexpired state should be available after read", ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 250;
		assertEquals(EXPIRED_UNAVAIL, ctx().emptyValue, ctx().get());
		assertEquals("Original state should be cleared on access", ctx().emptyValue, ctx().getOriginal());
	}

	@Test
	public void testRelaxedExpirationOnRead() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnReadAndWrite, StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		assertEquals(UNEXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 170;
		assertEquals(EXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());
		assertEquals("Expired state should be cleared on access", ctx().emptyValue, ctx().get());
	}

	@Test
	public void testExpirationTimestampOverflow() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnCreateAndWrite, StateTtlConfig.StateVisibility.NeverReturnExpired, Long.MAX_VALUE);

		timeProvider.time = 10;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		assertEquals(UNEXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());
	}

	@Test
	public void testMergeNamespaces() throws Exception {
		assumeThat(ctx, instanceOf(TtlMergingStateTestContext.class));

		initTest();

		timeProvider.time = 0;
		List<Tuple2<String, Object>> expiredUpdatesToMerge = mctx().generateExpiredUpdatesToMerge();
		mctx().applyStateUpdates(expiredUpdatesToMerge);

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		List<Tuple2<String, Object>> unexpiredUpdatesToMerge = mctx().generateUnexpiredUpdatesToMerge();
		mctx().applyStateUpdates(unexpiredUpdatesToMerge);

		takeAndRestoreSnapshot();

		timeProvider.time = 150;
		List<Tuple2<String, Object>> finalUpdatesToMerge = mctx().generateFinalUpdatesToMerge();
		mctx().applyStateUpdates(finalUpdatesToMerge);

		takeAndRestoreSnapshot();

		timeProvider.time = 230;
		mctx().ttlState.mergeNamespaces("targetNamespace", TtlMergingStateTestContext.NAMESPACES);
		mctx().ttlState.setCurrentNamespace("targetNamespace");
		assertEquals("Unexpected result of merge operation",
			mctx().getMergeResult(unexpiredUpdatesToMerge, finalUpdatesToMerge), mctx().get());
	}

	@Test
	public void testMultipleKeys() throws Exception {
		initTest();
		testMultipleStateIds(id -> sbetc.setCurrentKey(id), false);
	}

	@Test
	public void testMultipleKeysWithSnapshotCleanup() throws Exception {
		assumeTrue("full snapshot strategy", fullSnapshot());
		initTest(getConfBuilder(TTL).cleanupFullSnapshot().build());
		// set time back after restore to see entry unexpired if it was not cleaned up in snapshot properly
		testMultipleStateIds(id -> sbetc.setCurrentKey(id), true);
	}

	@Test
	public void testMultipleNamespaces() throws Exception {
		initTest();
		testMultipleStateIds(id -> ctx().ttlState.setCurrentNamespace(id), false);
	}

	@Test
	public void testMultipleNamespacesWithSnapshotCleanup() throws Exception {
		assumeTrue("full snapshot strategy", fullSnapshot());
		initTest(getConfBuilder(TTL).cleanupFullSnapshot().build());
		// set time back after restore to see entry unexpired if it was not cleaned up in snapshot properly
		testMultipleStateIds(id -> ctx().ttlState.setCurrentNamespace(id), true);
	}

	private void testMultipleStateIds(Consumer<String> idChanger, boolean timeBackAfterRestore) throws Exception {
		// test empty storage snapshot/restore
		takeAndRestoreSnapshot();

		timeProvider.time = 0;
		idChanger.accept("id2");
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		idChanger.accept("id1");
		ctx().update(ctx().updateEmpty);
		idChanger.accept("id2");
		ctx().update(ctx().updateUnexpired);

		timeProvider.time = 120;
		takeAndRestoreSnapshot();

		idChanger.accept("id1");
		assertEquals(UNEXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());
		idChanger.accept("id2");
		assertEquals(UPDATED_UNEXPIRED_AVAIL, ctx().getUnexpired, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 170;
		idChanger.accept("id2");
		ctx().update(ctx().updateExpired);

		timeProvider.time = 230;
		takeAndRestoreSnapshot();

		timeProvider.time = timeBackAfterRestore ? 170 : timeProvider.time;
		idChanger.accept("id1");
		assertEquals(EXPIRED_UNAVAIL, ctx().emptyValue, ctx().get());
		idChanger.accept("id2");
		assertEquals(UPDATED_UNEXPIRED_AVAIL, ctx().getUpdateExpired, ctx().get());

		timeProvider.time = 300;
		takeAndRestoreSnapshot();

		timeProvider.time = timeBackAfterRestore ? 230 : timeProvider.time;
		idChanger.accept("id1");
		assertEquals(EXPIRED_UNAVAIL, ctx().emptyValue, ctx().get());
		idChanger.accept("id2");
		assertEquals(EXPIRED_UNAVAIL, ctx().emptyValue, ctx().get());
	}

	@Test
	public void testSnapshotChangeRestore() throws Exception {
		initTest();

		timeProvider.time = 0;
		sbetc.setCurrentKey("k1");
		ctx().update(ctx().updateEmpty);

		timeProvider.time = 50;
		sbetc.setCurrentKey("k1");
		ctx().update(ctx().updateUnexpired);

		timeProvider.time = 100;
		sbetc.setCurrentKey("k2");
		ctx().update(ctx().updateEmpty);

		KeyedStateHandle snapshot = sbetc.takeSnapshot();

		timeProvider.time = 170;
		sbetc.setCurrentKey("k1");
		ctx().update(ctx().updateExpired);
		sbetc.setCurrentKey("k2");
		ctx().update(ctx().updateUnexpired);

		restoreSnapshot(snapshot, NUMBER_OF_KEY_GROUPS);

		timeProvider.time = 180;
		sbetc.setCurrentKey("k1");
		assertEquals(EXPIRED_UNAVAIL, ctx().emptyValue, ctx().get());
		sbetc.setCurrentKey("k2");
		assertEquals(UNEXPIRED_AVAIL, ctx().getUpdateEmpty, ctx().get());
	}

	@Test(expected = StateMigrationException.class)
	public void testRestoreTtlAndRegisterNonTtlStateCompatFailure() throws Exception {
		assumeThat(this, not(instanceOf(MockTtlStateTest.class)));

		initTest();

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		KeyedStateHandle snapshot = sbetc.takeSnapshot();
		sbetc.createAndRestoreKeyedStateBackend(snapshot);

		sbetc.setCurrentKey("defaultKey");
		sbetc.createState(ctx().createStateDescriptor(), "");
	}

	@Test
	public void testIncrementalCleanup() throws Exception {
		assumeTrue(incrementalCleanupSupported());

		initTest(getConfBuilder(TTL).cleanupIncrementally(5, true).build());

		final int keysToUpdate = (CopyOnWriteStateMap.DEFAULT_CAPACITY >> 3) * NUMBER_OF_KEY_GROUPS;

		timeProvider.time = 0;
		// create enough keys to trigger incremental rehash
		updateKeys(0, INC_CLEANUP_ALL_KEYS, ctx().updateEmpty);

		timeProvider.time = 50;
		// update some
		updateKeys(0, keysToUpdate, ctx().updateUnexpired);

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture = sbetc.triggerSnapshot();

		// update more concurrently with snapshotting
		updateKeys(keysToUpdate, keysToUpdate * 2, ctx().updateUnexpired);
		timeProvider.time = 120; // expire rest
		triggerMoreIncrementalCleanupByOtherOps();
		// check rest expired and cleanup updated
		checkExpiredKeys(keysToUpdate * 2, INC_CLEANUP_ALL_KEYS);

		KeyedStateHandle snapshot = snapshotRunnableFuture.get().getJobManagerOwnedSnapshot();
		// restore snapshot which should discard concurrent updates
		timeProvider.time = 50;
		restoreSnapshot(snapshot, NUMBER_OF_KEY_GROUPS);

		// check rest unexpired, also after restore which should discard concurrent updates
		checkUnexpiredKeys(keysToUpdate, INC_CLEANUP_ALL_KEYS, ctx().getUpdateEmpty);

		timeProvider.time = 120;

		// remove some
		for (int i = keysToUpdate >> 1; i < keysToUpdate + (keysToUpdate >> 2); i++) {
			sbetc.setCurrentKey(Integer.toString(i));
			ctx().ttlState.clear();
		}
		// check updated not expired
		checkUnexpiredKeys(0, keysToUpdate >> 1, ctx().getUnexpired);
		triggerMoreIncrementalCleanupByOtherOps();
		// check that concurrently updated and then restored with original values are expired
		checkExpiredKeys(keysToUpdate, keysToUpdate * 2);

		timeProvider.time = 170;
		// check rest expired and cleanup updated
		checkExpiredKeys(keysToUpdate >> 1, INC_CLEANUP_ALL_KEYS);
		// check updated expired
		checkExpiredKeys(0, keysToUpdate >> 1);
	}

	private <T> void updateKeys(int startKey, int endKey, T value) throws Exception {
		for (int i = startKey; i < endKey; i++) {
			sbetc.setCurrentKey(Integer.toString(i));
			ctx().update(value);
		}
	}

	private <T> void checkUnexpiredKeys(int startKey, int endKey, T value) throws Exception {
		for (int i = startKey; i < endKey; i++) {
			sbetc.setCurrentKey(Integer.toString(i));
			assertEquals(UNEXPIRED_AVAIL, value, ctx().get());
		}
	}

	private void checkExpiredKeys(int startKey, int endKey) throws Exception {
		for (int i = startKey; i < endKey; i++) {
			sbetc.setCurrentKey(Integer.toString(i));
			assertEquals("Original state should be cleared", ctx().emptyValue, ctx().getOriginal());
		}
	}

	private void triggerMoreIncrementalCleanupByOtherOps() throws Exception {
		// trigger more cleanup by doing something out side of INC_CLEANUP_ALL_KEYS
		for (int i = INC_CLEANUP_ALL_KEYS; i < INC_CLEANUP_ALL_KEYS * 2; i++) {
			sbetc.setCurrentKey(Integer.toString(i));
			if (i % 2 == 0) {
				ctx().get();
			} else {
				ctx().update(ctx().updateEmpty);
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		sbetc.dispose();
	}
}
