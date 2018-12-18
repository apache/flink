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
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.StateMigrationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/** State TTL base test suite. */
@RunWith(Parameterized.class)
public abstract class TtlStateTestBase {
	private static final long TTL = 100;

	private MockTtlTimeProvider timeProvider;
	private StateBackendTestContext sbetc;
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
			new TtlListStateTestContext(),
			new TtlMapStateAllEntriesTestContext(),
			new TtlMapStatePerElementTestContext(),
			new TtlMapStatePerNullElementTestContext(),
			new TtlAggregatingStateTestContext(),
			new TtlReducingStateTestContext(),
			new TtlFoldingStateTestContext());
	}

	@SuppressWarnings("unchecked")
	private <S extends InternalKvState<?, String, ?>, UV> TtlStateTestContextBase<S, UV, ?> ctx() {
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
			.build());
	}

	private static StateTtlConfig.Builder getConfBuilder(long ttl) {
		return StateTtlConfig.newBuilder(Time.milliseconds(ttl));
	}

	private void initTest(StateTtlConfig ttlConfig) throws Exception {
		this.ttlConfig = ttlConfig;
		sbetc.createAndRestoreKeyedStateBackend();
		sbetc.restoreSnapshot(null);
		createState();
		ctx().initTestValues();
	}

	@SuppressWarnings("unchecked")
	private <S extends State> void createState() throws Exception {
		StateDescriptor<S, Object> stateDescriptor = ctx().createStateDescriptor();
		stateDescriptor.enableTimeToLive(ttlConfig);
		ctx().ttlState =
			(InternalKvState<?, String, ?>) sbetc.createState(stateDescriptor, "defaultNamespace");
	}

	private void takeAndRestoreSnapshot() throws Exception {
		KeyedStateHandle snapshot = sbetc.takeSnapshot();
		sbetc.createAndRestoreKeyedStateBackend();
		sbetc.restoreSnapshot(snapshot);
		createState();
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
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		ctx().update(ctx().updateUnexpired);

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		assertEquals("Unexpired state should be available after update", ctx().getUnexpired, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 170;
		ctx().update(ctx().updateExpired);

		takeAndRestoreSnapshot();

		timeProvider.time = 220;
		assertEquals("Unexpired state should be available after update", ctx().getUpdateExpired, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 300;
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
		assertEquals("Original state should be cleared on access", ctx().emptyValue, ctx().getOriginal());
	}

	@Test
	public void testRelaxedExpirationOnWrite() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnCreateAndWrite, StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		assertEquals("Expired state should be available", ctx().getUpdateEmpty, ctx().get());
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
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 120;
		assertEquals("Unexpired state should be available after read", ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 250;
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
		assertEquals("Original state should be cleared on access", ctx().emptyValue, ctx().getOriginal());
	}

	@Test
	public void testRelaxedExpirationOnRead() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnReadAndWrite, StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 170;
		assertEquals("Expired state should be available", ctx().getUpdateEmpty, ctx().get());
		assertEquals("Expired state should be cleared on access", ctx().emptyValue, ctx().get());
	}

	@Test
	public void testExpirationTimestampOverflow() throws Exception {
		initTest(StateTtlConfig.UpdateType.OnCreateAndWrite, StateTtlConfig.StateVisibility.NeverReturnExpired, Long.MAX_VALUE);

		timeProvider.time = 10;
		ctx().update(ctx().updateEmpty);

		takeAndRestoreSnapshot();

		timeProvider.time = 50;
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());
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
		testMultipleStateIdsWithSnapshotCleanup(id -> sbetc.setCurrentKey(id));
	}

	@Test
	public void testMultipleNamespaces() throws Exception {
		testMultipleStateIdsWithSnapshotCleanup(id -> ctx().ttlState.setCurrentNamespace(id));
	}

	private void testMultipleStateIdsWithSnapshotCleanup(Consumer<String> idChanger) throws Exception {
		initTest();
		testMultipleStateIds(idChanger, false);

		initTest(getConfBuilder(TTL).cleanupFullSnapshot().build());
		// set time back after restore to see entry unexpired if it was not cleaned up in snapshot properly
		testMultipleStateIds(idChanger, true);
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
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());
		idChanger.accept("id2");
		assertEquals("Unexpired state should be available after update", ctx().getUnexpired, ctx().get());

		takeAndRestoreSnapshot();

		timeProvider.time = 170;
		idChanger.accept("id2");
		ctx().update(ctx().updateExpired);

		timeProvider.time = 230;
		takeAndRestoreSnapshot();

		timeProvider.time = timeBackAfterRestore ? 170 : timeProvider.time;
		idChanger.accept("id1");
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
		idChanger.accept("id2");
		assertEquals("Unexpired state should be available after update", ctx().getUpdateExpired, ctx().get());

		timeProvider.time = 300;
		takeAndRestoreSnapshot();

		timeProvider.time = timeBackAfterRestore ? 230 : timeProvider.time;
		idChanger.accept("id1");
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
		idChanger.accept("id2");
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
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

		sbetc.createAndRestoreKeyedStateBackend();
		sbetc.restoreSnapshot(snapshot);
		createState();

		timeProvider.time = 180;
		sbetc.setCurrentKey("k1");
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
		sbetc.setCurrentKey("k2");
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());
	}

	@Test(expected = StateMigrationException.class)
	public void testRestoreTtlAndRegisterNonTtlStateCompatFailure() throws Exception {
		assumeThat(this, not(instanceOf(MockTtlStateTest.class)));

		initTest();

		timeProvider.time = 0;
		ctx().update(ctx().updateEmpty);

		KeyedStateHandle snapshot = sbetc.takeSnapshot();
		sbetc.createAndRestoreKeyedStateBackend();

		sbetc.restoreSnapshot(snapshot);
		sbetc.createState(ctx().createStateDescriptor(), "");
	}

	@After
	public void tearDown() {
		sbetc.disposeKeyedStateBackend();
	}
}
