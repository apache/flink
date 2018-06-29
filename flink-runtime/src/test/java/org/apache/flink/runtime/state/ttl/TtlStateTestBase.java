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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

abstract class TtlStateTestBase<S extends InternalKvState<?, String, ?>, UV, GV> {
	private static final long TTL = 100;

	S ttlState;
	MockTimeProvider timeProvider;
	TtlConfig ttlConfig;

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
		initTest(TtlConfig.TtlUpdateType.OnCreateAndWrite, TtlConfig.TtlStateVisibility.NeverReturnExpired);
	}

	private void initTest(TtlConfig.TtlUpdateType updateType, TtlConfig.TtlStateVisibility visibility) {
		initTest(updateType, visibility, TTL);
	}

	private void initTest(TtlConfig.TtlUpdateType updateType, TtlConfig.TtlStateVisibility visibility, long ttl) {
		timeProvider = new MockTimeProvider();
		ttlConfig = new TtlConfig(
			updateType,
			visibility,
			TtlConfig.TtlTimeCharacteristic.ProcessingTime,
			Time.milliseconds(ttl));
		ttlState = createState();
		initTestValues();
	}

	abstract S createState();

	abstract void initTestValues();

	@Test
	public void testNonExistentValue() throws Exception {
		initTest();
		assertEquals("Non-existing state should be empty", emptyValue, getter.get());
	}

	@Test
	public void testExactExpirationOnWrite() throws Exception {
		initTest(TtlConfig.TtlUpdateType.OnCreateAndWrite, TtlConfig.TtlStateVisibility.NeverReturnExpired);

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
		initTest(TtlConfig.TtlUpdateType.OnCreateAndWrite, TtlConfig.TtlStateVisibility.ReturnExpiredIfNotCleanedUp);

		timeProvider.time = 0;
		updater.accept(updateEmpty);

		timeProvider.time = 120;
		assertEquals("Expired state should be available", getUpdateEmpty, getter.get());
		assertEquals("Expired state should be cleared on access", emptyValue, getter.get());
	}

	@Test
	public void testExactExpirationOnRead() throws Exception {
		initTest(TtlConfig.TtlUpdateType.OnReadAndWrite, TtlConfig.TtlStateVisibility.NeverReturnExpired);

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
		initTest(TtlConfig.TtlUpdateType.OnReadAndWrite, TtlConfig.TtlStateVisibility.ReturnExpiredIfNotCleanedUp);

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
		initTest(TtlConfig.TtlUpdateType.OnCreateAndWrite, TtlConfig.TtlStateVisibility.NeverReturnExpired, Long.MAX_VALUE);

		timeProvider.time = 10;
		updater.accept(updateEmpty);

		timeProvider.time = 50;
		assertEquals("Unexpired state should be available", getUpdateEmpty, getter.get());
	}
}
