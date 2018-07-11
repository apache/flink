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
import org.apache.flink.api.common.state.StateTtlConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyedStateFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateFactory;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

abstract class TtlStateTestBase<S extends InternalKvState<?, String, ?>, UV, GV> {
	private static final long TTL = 100;
	private static final KeyedStateFactory MOCK_ORIGINAL_STATE_FACTORY = new MockKeyedStateFactory();

	S ttlState;
	MockTimeProvider timeProvider;
	StateTtlConfiguration ttlConfig;

	ThrowingConsumer<UV, Exception> updater;
	SupplierWithException<GV, Exception> getter;
	SupplierWithException<?, Exception> originalGetter;

	UV updateEmpty;
	UV updateUnexpired;
	UV updateExpired;

	GV getUpdateEmpty;
	GV getUnexpired;
	GV getUpdateExpired;

	GV emptyValue = null;

	void initTest() {
		initTest(StateTtlConfiguration.TtlUpdateType.OnCreateAndWrite, StateTtlConfiguration.TtlStateVisibility.NeverReturnExpired);
	}

	private void initTest(StateTtlConfiguration.TtlUpdateType updateType, StateTtlConfiguration.TtlStateVisibility visibility) {
		initTest(updateType, visibility, TTL);
	}

	private void initTest(StateTtlConfiguration.TtlUpdateType updateType, StateTtlConfiguration.TtlStateVisibility visibility, long ttl) {
		timeProvider = new MockTimeProvider();
		StateTtlConfiguration.Builder ttlConfigBuilder = StateTtlConfiguration.newBuilder(Time.seconds(5));
		ttlConfigBuilder.setTtlUpdateType(updateType)
						.setStateVisibility(visibility)
						.setTimeCharacteristic(StateTtlConfiguration.TtlTimeCharacteristic.ProcessingTime)
						.setTtl(Time.milliseconds(ttl));
		ttlConfig = ttlConfigBuilder.build();
		ttlState = createState();
		initTestValues();
	}

	abstract S createState();

	<SV, US extends State, IS extends US> IS wrapMockState(StateDescriptor<IS, SV> stateDesc) {
		try {
			return TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
				StringSerializer.INSTANCE, stateDesc,
				MOCK_ORIGINAL_STATE_FACTORY, ttlConfig, timeProvider);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Unexpected exception wrapping mock state", e);
		}
	}

	abstract void initTestValues();

	@Test
	public void testNonExistentValue() throws Exception {
		initTest();
		assertEquals("Non-existing state should be empty", emptyValue, getter.get());
	}

	@Test
	public void testExactExpirationOnWrite() throws Exception {
		initTest(StateTtlConfiguration.TtlUpdateType.OnCreateAndWrite, StateTtlConfiguration.TtlStateVisibility.NeverReturnExpired);

		timeProvider.time = 0;
		updater.accept(updateEmpty);

		timeProvider.time = 20;
		assertEquals("Unexpired state should be available", getUpdateEmpty, getter.get());

		timeProvider.time = 50;
		updater.accept(updateUnexpired);

		timeProvider.time = 120;
		assertEquals("Unexpired state should be available after update", getUnexpired, getter.get());

		timeProvider.time = 170;
		updater.accept(updateExpired);

		timeProvider.time = 220;
		assertEquals("Unexpired state should be available after update", getUpdateExpired, getter.get());

		timeProvider.time = 300;
		assertEquals("Expired state should be unavailable", emptyValue, getter.get());
		assertEquals("Original state should be cleared on access", emptyValue, originalGetter.get());
	}

	@Test
	public void testRelaxedExpirationOnWrite() throws Exception {
		initTest(StateTtlConfiguration.TtlUpdateType.OnCreateAndWrite, StateTtlConfiguration.TtlStateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		updater.accept(updateEmpty);

		timeProvider.time = 120;
		assertEquals("Expired state should be available", getUpdateEmpty, getter.get());
		assertEquals("Expired state should be cleared on access", emptyValue, getter.get());
	}

	@Test
	public void testExactExpirationOnRead() throws Exception {
		initTest(StateTtlConfiguration.TtlUpdateType.OnReadAndWrite, StateTtlConfiguration.TtlStateVisibility.NeverReturnExpired);

		timeProvider.time = 0;
		updater.accept(updateEmpty);

		timeProvider.time = 50;
		assertEquals("Unexpired state should be available", getUpdateEmpty, getter.get());

		timeProvider.time = 120;
		assertEquals("Unexpired state should be available after read", getUpdateEmpty, getter.get());

		timeProvider.time = 250;
		assertEquals("Expired state should be unavailable", emptyValue, getter.get());
		assertEquals("Original state should be cleared on access", emptyValue, originalGetter.get());
	}

	@Test
	public void testRelaxedExpirationOnRead() throws Exception {
		initTest(StateTtlConfiguration.TtlUpdateType.OnReadAndWrite, StateTtlConfiguration.TtlStateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		updater.accept(updateEmpty);

		timeProvider.time = 50;
		assertEquals("Unexpired state should be available", getUpdateEmpty, getter.get());

		timeProvider.time = 170;
		assertEquals("Expired state should be available", getUpdateEmpty, getter.get());
		assertEquals("Expired state should be cleared on access", emptyValue, getter.get());
	}

	@Test
	public void testExpirationTimestampOverflow() throws Exception {
		initTest(StateTtlConfiguration.TtlUpdateType.OnCreateAndWrite, StateTtlConfiguration.TtlStateVisibility.NeverReturnExpired, Long.MAX_VALUE);

		timeProvider.time = 10;
		updater.accept(updateEmpty);

		timeProvider.time = 50;
		assertEquals("Unexpired state should be available", getUpdateEmpty, getter.get());
	}
}
