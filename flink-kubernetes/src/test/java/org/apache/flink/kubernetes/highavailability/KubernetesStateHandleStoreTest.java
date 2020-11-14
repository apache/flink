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
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.StringResourceVersion;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper.LongRetrievableStateHandle;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link KubernetesStateHandleStore} operations.
 */
public class KubernetesStateHandleStoreTest extends KubernetesHighAvailabilityTestBase {

	private static final String PREFIX = "test-prefix-";
	private final String key = PREFIX + JobID.generate();
	private final Predicate<String> filter = k -> k.startsWith(PREFIX);
	private final Long state = 12345L;

	private TestingLongStateHandleHelper longStateStorage;

	@Before
	public void setup() {
		super.setup();
		longStateStorage = new TestingLongStateHandleHelper();
	}

	@Test
	public void testAdd() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					store.addAndLock(key, state);
					assertThat(store.getAllAndLock().size(), is(1));
					assertThat(store.getAndLock(key).retrieveState(), is(state));
				});
		}};
	}

	@Test
	public void testAddAlreadyExistingKey() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					getLeaderConfigMap().getData().put(key, "existing data");

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);

					try {
						store.addAndLock(key, state);
						fail("Exception should be thrown.");
					} catch (StateHandleStore.AlreadyExistException ex) {
						final String msg = String.format(
							"%s already exists in ConfigMap %s", key, LEADER_CONFIGMAP_NAME);
						assertThat(ex, FlinkMatchers.containsMessage(msg));
					}
					assertThat(longStateStorage.getStateHandles().size(), is(1));
					assertThat(longStateStorage.getStateHandles().get(0).getNumberOfDiscardCalls(), is(1));
				});
		}};
	}

	@Test
	public void testAddFailedWhenConfigMapNotExistAndDiscardState() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);

					try {
						store.addAndLock(key, state);
						fail("Exception should be thrown.");
					} catch (Exception ex) {
						final String msg = String.format("ConfigMap %s does not exist.", LEADER_CONFIGMAP_NAME);
						assertThat(ex, FlinkMatchers.containsMessage(msg));
					}
					assertThat(longStateStorage.getStateHandles().size(), is(1));
					assertThat(longStateStorage.getStateHandles().get(0).getNumberOfDiscardCalls(), is(1));
				});
		}};
	}

	@Test
	public void testReplace() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);

					store.addAndLock(key, state);

					final Long newState = 23456L;
					final StringResourceVersion resourceVersion = store.exists(key);
					store.replace(key, resourceVersion, newState);

					assertThat(store.getAllAndLock().size(), is(1));
					assertThat(store.getAndLock(key).retrieveState(), is(newState));
				});
		}};
	}

	@Test
	public void testReplaceWithKeyNotExist() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					final Long newState = 23456L;

					try {
						assertThat(store.exists(key), is(StringResourceVersion.notExisting()));
						store.replace(key, StringResourceVersion.notExisting(), newState);
						fail("Exception should be thrown.");
					} catch (StateHandleStore.NotExistException e) {
						final String msg = String.format(
							"Could not find %s in ConfigMap %s", key, LEADER_CONFIGMAP_NAME);
						assertThat(e, FlinkMatchers.containsMessage(msg));
					}
					assertThat(store.getAllAndLock().size(), is(0));
				});
		}};
	}

	@Test
	public void testReplaceWithNoLeadershipAndDiscardState() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					final Long newState = 23456L;

					store.addAndLock(key, state);
					// Lost leadership
					getLeaderCallback().notLeader();
					electionEventHandler.waitForRevokeLeader(TIMEOUT);
					getLeaderConfigMap().getAnnotations().remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

					final StringResourceVersion resourceVersion = store.exists(key);
					store.replace(key, resourceVersion, newState);
					assertThat(store.getAllAndLock().size(), is(1));
					// The state do not change
					assertThat(store.getAndLock(key).retrieveState(), is(state));

					assertThat(longStateStorage.getStateHandles().size(), is(2));
					assertThat(longStateStorage.getStateHandles().get(0).getNumberOfDiscardCalls(), is(0));
					assertThat(longStateStorage.getStateHandles().get(1).getNumberOfDiscardCalls(), is(1));
				});
		}};
	}

	@Test
	public void testReplaceFailedAndDiscardState() throws Exception {
		final FlinkRuntimeException updateException = new FlinkRuntimeException("Failed to update");
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					store.addAndLock(key, state);

					final FlinkKubeClient anotherFlinkKubeClient = createFlinkKubeClientBuilder()
						.setCheckAndUpdateConfigMapFunction((configMapName, function) -> {
							throw updateException;
						}).build();
					final KubernetesStateHandleStore<Long> anotherStore = new KubernetesStateHandleStore<>(
						anotherFlinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);

					final Long newState = 23456L;
					final StringResourceVersion resourceVersion = anotherStore.exists(key);
					assertThat(resourceVersion.isExisting(), is(true));
					try {
						anotherStore.replace(key, resourceVersion, newState);
						fail("We should get an exception when kube client failed to update.");
					} catch (Exception ex) {
						assertThat(ex, FlinkMatchers.containsCause(updateException));
					}
					assertThat(anotherStore.getAllAndLock().size(), is(1));
					// The state do not change
					assertThat(anotherStore.getAndLock(key).retrieveState(), is(state));

					assertThat(longStateStorage.getStateHandles().size(), is(2));
					assertThat(longStateStorage.getStateHandles().get(0).getNumberOfDiscardCalls(), is(0));
					assertThat(longStateStorage.getStateHandles().get(1).getNumberOfDiscardCalls(), is(1));
				});
		}};
	}

	@Test
	public void testGetAndExist() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					store.addAndLock(key, state);
					assertThat(store.exists(key), is(not(StringResourceVersion.notExisting())));
					assertThat(store.getAndLock(key).retrieveState(), is(state));
				});
		}};
	}

	@Test
	public void testGetNonExistingKey() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					final String nonExistingKey = "non-existing-key";
					store.addAndLock(key, state);
					assertThat(store.exists(nonExistingKey), is(StringResourceVersion.notExisting()));
					try {
						store.getAndLock(nonExistingKey);
						fail("Exception should be thrown.");
					} catch (StateHandleStore.NotExistException e) {
						final String msg = String.format(
							"Could not find %s in ConfigMap %s", nonExistingKey, LEADER_CONFIGMAP_NAME);
						assertThat(e, FlinkMatchers.containsMessage(msg));
					}
				});
		}};
	}

	@Test
	public void testGetAll() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					final List<Long> expected = Arrays.asList(3L, 2L, 1L);

					for (Long each : expected) {
						store.addAndLock(key + each, each);
					}
					final Long[] actual = store.getAllAndLock().stream()
						.map(FunctionUtils.uncheckedFunction(e -> e.f0.retrieveState()))
						.toArray(Long[]::new);
					assertThat(Arrays.asList(actual), containsInAnyOrder(expected.toArray()));
				});
		}};
	}

	@Test
	public void testGetAllHandles() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					final List<String> expected = Arrays.asList(key + 3, key + 2, key + 1);

					for (String each : expected) {
						store.addAndLock(each, state);
					}
					final String[] actual = store.getAllHandles().toArray(new String[0]);
					expected.sort(Comparator.comparing(e -> e));
					assertThat(Arrays.asList(actual), containsInAnyOrder(expected.toArray()));
				});
		}};
	}

	@Test
	public void testRemove() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();
					LongRetrievableStateHandle.clearNumberOfGlobalDiscardCalls();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					store.addAndLock(key, state);
					assertThat(store.getAllAndLock().size(), is(1));
					assertThat(store.releaseAndTryRemove(key), is(true));
					assertThat(store.getAllAndLock().size(), is(0));

					// State should also be discarded.
					assertThat(LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls(), is(1));
				});
		}};
	}

	@Test
	public void testRemoveFailedShouldNotDiscardState() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();
					LongRetrievableStateHandle.clearNumberOfGlobalDiscardCalls();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);

					store.addAndLock(key, state);
					// Lost leadership
					getLeaderCallback().notLeader();
					electionEventHandler.waitForRevokeLeader(TIMEOUT);
					getLeaderConfigMap().getAnnotations().remove(KubernetesLeaderElector.LEADER_ANNOTATION_KEY);

					assertThat(store.releaseAndTryRemove(key), is(false));
					assertThat(store.getAllAndLock().size(), is(1));

					assertThat(LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls(), is(0));
				});
		}};
	}

	@Test
	public void testRemoveAllHandlesAndDiscardState() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();
					LongRetrievableStateHandle.clearNumberOfGlobalDiscardCalls();

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					store.addAndLock(key, state);
					store.addAndLock(key + "1", 2L);
					assertThat(store.getAllAndLock().size(), is(2));
					store.releaseAndTryRemoveAll();
					assertThat(store.getAllAndLock().size(), is(0));
					assertThat(LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls(), is(2));
				});
		}};
	}

	@Test
	public void testRemoveAllHandles() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();
					LongRetrievableStateHandle.clearNumberOfGlobalDiscardCalls();

					final String anotherKey = "key-not-with-prefix";
					getLeaderConfigMap().getData().put(anotherKey, "value");

					final KubernetesStateHandleStore<Long> store = new KubernetesStateHandleStore<>(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, longStateStorage, filter, LOCK_IDENTITY);
					store.addAndLock(key, state);
					store.addAndLock(key + "1", 2L);
					assertThat(store.getAllAndLock().size(), is(2));
					store.clearEntries();
					assertThat(store.getAllAndLock().size(), is(0));
					assertThat(LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls(), is(0));

					// Should only remove the key with specified prefix.
					assertThat(getLeaderConfigMap().getData().containsKey(anotherKey), is(true));
				});
		}};
	}
}
