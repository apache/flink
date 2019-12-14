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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.util.KeyedBudgetManager.AcquisitionResult;
import org.apache.flink.util.Preconditions;

import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test suite for {@link KeyedBudgetManager}.
 */
@SuppressWarnings("MagicNumber")
public class KeyedBudgetManagerTest extends TestLogger {
	private static final String[] TEST_KEYS = {"k1", "k2", "k3", "k4"};
	private static final long[] TEST_BUDGETS = {15, 17, 22, 11};
	private static final Executor NEW_THREAD_EXECUTOR = r -> new Thread(r).start();

	private KeyedBudgetManager<String> keyedBudgetManager;

	@Before
	public void setup() {
		keyedBudgetManager = createSimpleKeyedBudget();
	}

	@After
	public void teardown() {
		keyedBudgetManager.releaseAll();
		checkNoKeyBudgetChange();
	}

	@Test
	public void testSuccessfulAcquisitionForKey() {
		long acquired = keyedBudgetManager.acquireBudgetForKey("k1", 10L);

		assertThat(acquired, is(10L));
		checkOneKeyBudgetChange("k1", 5L);
	}

	@Test
	public void testFailedAcquisitionForKey() {
		long maxPossibleBudgetToAcquire = keyedBudgetManager.acquireBudgetForKey("k1", 20L);

		assertThat(maxPossibleBudgetToAcquire, is(15L));
		checkNoKeyBudgetChange();
	}

	@Test
	public void testSuccessfulReleaseForKey() {
		keyedBudgetManager.acquireBudgetForKey("k1", 10L);
		keyedBudgetManager.releaseBudgetForKey("k1", 5L);

		checkOneKeyBudgetChange("k1", 10L);
	}

	@Test
	public void testFailedReleaseForKey() {
		keyedBudgetManager.acquireBudgetForKey("k1", 10L);
		try {
			keyedBudgetManager.releaseBudgetForKey("k1", 15L);
			fail("IllegalStateException is expected to fail over-sized release");
		} catch (IllegalStateException e) {
			// expected
		}

		checkOneKeyBudgetChange("k1", 5L);
	}

	@Test
	public void testSuccessfulAcquisitionForKeys() {
		AcquisitionResult<String> acquired = acquireForMultipleKeys(5L);

		assertThat(checkAcquisitionSuccess(acquired, 4L), is(true));

		assertThat(keyedBudgetManager.availableBudgetForKey("k1"), is(15L));
		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(19L));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(45L));
	}

	@Test
	public void testConcurrentAcquisitionForKeys() throws ExecutionException, InterruptedException {
		long pageSize = 5L;
		CompletableFuture<AcquisitionResult<String>> allocation1 = acquireForMultipleKeysAsync(pageSize);
		CompletableFuture<Long> availableBudgetForKeysFuture = getAvailableBudgetForKeysAsync();
		CompletableFuture<AcquisitionResult<String>> allocation2 = acquireForMultipleKeysAsync(pageSize);
		Arrays
			.asList(allocation1, allocation2, availableBudgetForKeysFuture)
			.forEach(KeyedBudgetManagerTest::waitForFutureSilently);

		boolean firstSucceeded = checkFirstAcquisitionSucceeded(allocation1, allocation2);
		boolean secondSucceeded = checkFirstAcquisitionSucceeded(allocation2, allocation1);
		assertThat(firstSucceeded || secondSucceeded, is(true));

		long availableBudgetForKeys = availableBudgetForKeysFuture.get();
		assertThat(availableBudgetForKeys == 39L || availableBudgetForKeys == 19L, is(true));
	}

	@Test
	public void testConcurrentReleaseForKeys() throws ExecutionException, InterruptedException {
		long pageSize = 5L;
		Map<String, Long> sizeByKey = acquireForMultipleKeys(pageSize)
			.getAcquiredPerKey()
			.entrySet()
			.stream()
			.collect(Collectors.toMap(Entry::getKey, e -> e.getValue() * pageSize));

		CompletableFuture<Void> release1 = releaseKeysAsync(sizeByKey);
		CompletableFuture<Long> availableBudgetForKeysFuture = getAvailableBudgetForKeysAsync();
		CompletableFuture<Void> release2 = releaseKeysAsync(sizeByKey);
		Arrays
			.asList(release1, availableBudgetForKeysFuture, release2)
			.forEach(KeyedBudgetManagerTest::waitForFutureSilently);

		boolean firstSucceeded = !release1.isCompletedExceptionally() && release2.isCompletedExceptionally();
		boolean secondSucceeded = !release2.isCompletedExceptionally() && release1.isCompletedExceptionally();
		assertThat(firstSucceeded || secondSucceeded, is(true));

		long availableBudgetForKeys = availableBudgetForKeysFuture.get();
		assertThat(availableBudgetForKeys == 39L || availableBudgetForKeys == 19L, is(true));

		checkNoKeyBudgetChange();
	}

	@Test
	public void testFailedAcquisitionForKeys() {
		AcquisitionResult<String> acquired =
			keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 6, 6);

		assertThat(acquired.isFailure(), is(true));
		assertThat(acquired.getTotalAvailableForAllQueriedKeys(), is(5L));
		checkNoKeyBudgetChange();
	}

	@Test
	public void testSuccessfulReleaseForKeys() {
		keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 8);
		keyedBudgetManager.releaseBudgetForKeys(createdBudgetMap(new String[] {"k2", "k3"}, new long[] {7, 10}));

		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(24L));
		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k1", "k4")), is(26L));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(50L));
	}

	@Test
	public void testSuccessfulReleaseForKeysWithMixedRequests() {
		keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 8);
		keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k1", "k4"), 6, 3);
		keyedBudgetManager.releaseBudgetForKeys(createdBudgetMap(new String[] {"k2", "k3"}, new long[] {7, 10}));

		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(24L));
		assertThat(keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k1", "k4")), is(8L));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(32L));
	}

	private void checkNoKeyBudgetChange() {
		checkKeysBudgetChange(Collections.emptyMap());
	}

	private void checkOneKeyBudgetChange(
			@SuppressWarnings("SameParameterValue") String key,
			long budget) {
		checkKeysBudgetChange(Collections.singletonMap(key, budget));
	}

	private void checkKeysBudgetChange(
			Map<String, Long> changedBudgetPerKey) {
		long totalExpectedBudget = 0L;
		for (int i = 0; i < TEST_KEYS.length; i++) {
			long expectedBudget = changedBudgetPerKey.containsKey(TEST_KEYS[i]) ?
				changedBudgetPerKey.get(TEST_KEYS[i]) : TEST_BUDGETS[i];
			assertThat(keyedBudgetManager.availableBudgetForKey(TEST_KEYS[i]), is(expectedBudget));
			totalExpectedBudget += expectedBudget;
		}
		assertThat(keyedBudgetManager.maxTotalBudget(), is(LongStream.of(TEST_BUDGETS).sum()));
		assertThat(keyedBudgetManager.totalAvailableBudget(), is(totalExpectedBudget));
	}

	private CompletableFuture<AcquisitionResult<String>> acquireForMultipleKeysAsync(long pageSize) {
		return CompletableFuture.supplyAsync(() -> acquireForMultipleKeys(pageSize), NEW_THREAD_EXECUTOR);
	}

	private CompletableFuture<Long> getAvailableBudgetForKeysAsync() {
		return CompletableFuture.supplyAsync(() -> keyedBudgetManager.availableBudgetForKeys(Arrays.asList("k2", "k3")), NEW_THREAD_EXECUTOR);
	}

	private AcquisitionResult<String> acquireForMultipleKeys(long pageSize) {
		return keyedBudgetManager.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, pageSize);
	}

	private CompletableFuture<Void> releaseKeysAsync(Map<String, Long> sizeByKey) {
		return CompletableFuture.runAsync(() -> keyedBudgetManager.releaseBudgetForKeys(sizeByKey), NEW_THREAD_EXECUTOR);
	}

	private static boolean checkFirstAcquisitionSucceeded(
		Future<AcquisitionResult<String>> allocation1,
		Future<AcquisitionResult<String>> allocation2) throws ExecutionException, InterruptedException {
		return checkAcquisitionSuccess(allocation1.get(), 4L) && allocation2.get().isFailure();
	}

	private static boolean checkAcquisitionSuccess(
		AcquisitionResult<String> acquired,
		@SuppressWarnings("SameParameterValue") long numberOfPageToAcquire) {
		return acquired.isSuccess() &&
			acquired.getAcquiredPerKey().values().stream().mapToLong(b -> b).sum() == numberOfPageToAcquire;
	}

	private static KeyedBudgetManager<String> createSimpleKeyedBudget() {
		return new KeyedBudgetManager<>(createdBudgetMap(TEST_KEYS, TEST_BUDGETS), 1L);
	}

	private static Map<String, Long> createdBudgetMap(String[] keys, long[] budgets) {
		Preconditions.checkArgument(keys.length == budgets.length);
		Map<String, Long> keydBudgets = new HashMap<>();
		for (int i = 0; i < keys.length; i++) {
			keydBudgets.put(keys[i], budgets[i]);
		}
		return keydBudgets;
	}

	private static void waitForFutureSilently(Future<?> future) {
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			// silent
		}
	}
}
