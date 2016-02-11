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

package org.apache.flink.contrib.streaming.state;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;

import static org.junit.Assert.*;

public class DbStateBackendTest {

	private static NetworkServerControl server;
	private static File tempDir;
	private static DbBackendConfig conf;
	private static String url1;
	private static String url2;

	@BeforeClass
	public static void startDerbyServer() throws UnknownHostException, Exception {
		server = new NetworkServerControl(InetAddress.getByName("localhost"), 1527, "flink", "flink");

		// Async call, we need to ensure that the server starts before leaving
		// the method
		server.start(null);

		tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		conf = new DbBackendConfig("flink", "flink",
				"jdbc:derby://localhost:1527/" + tempDir.getAbsolutePath() + "/flinkDB1;create=true");
		conf.setDbAdapter(new DerbyAdapter());
		conf.setKvStateCompactionFrequency(1);

		url1 = "jdbc:derby://localhost:1527/" + tempDir.getAbsolutePath() + "/flinkDB1;create=true";
		url2 = "jdbc:derby://localhost:1527/" + tempDir.getAbsolutePath() + "/flinkDB2;create=true";

		// We need to ensure that the Derby server starts properly before
		// beginning the tests
		ensureServerStarted(server);
	}

	public static void ensureServerStarted(NetworkServerControl server) throws InterruptedException {
		// We try to ping the server 10 times with 1s sleep in between
		// If the ping succeeds without exception the server started
		int retry = 0;
		while (true) {
			if (retry < 10) {
				try {
					server.ping();
					break;
				} catch (Exception e) {
					retry++;
					Thread.sleep(1000);
				}
			} else {
				throw new RuntimeException("Could not start the Derby server in 10 seconds.");
			}
		}
	}

	@AfterClass
	public static void stopDerbyServer() throws Exception {
		try {
			server.shutdown();
			FileUtils.deleteDirectory(new File(tempDir.getAbsolutePath() + "/flinkDB1"));
			FileUtils.deleteDirectory(new File(tempDir.getAbsolutePath() + "/flinkDB2"));
			FileUtils.forceDelete(new File("derby.log"));
		} catch (Exception ignore) {
		}
	}

	@Test
	public void testSetupAndSerialization() throws Exception {
		DbStateBackend dbBackend = new DbStateBackend(conf);

		assertFalse(dbBackend.isInitialized());

		// serialize / copy the backend
		DbStateBackend backend = CommonTestUtils.createCopySerializable(dbBackend);
		assertFalse(backend.isInitialized());

		Environment env = new DummyEnvironment("test", 1, 0);
		backend.initializeForJob(env, "dummy-setup-ser", StringSerializer.INSTANCE);
		String jobId = env.getJobID().toString().substring(0, 16);

		assertNotNull(backend.getConnections());
		assertTrue(
				isTableCreated(backend.getConnections().getFirst(), "checkpoints_" + jobId));

		backend.disposeAllStateForCurrentJob();
		assertFalse(
				isTableCreated(backend.getConnections().getFirst(), "checkpoints_" + jobId));
		backend.close();

		assertTrue(backend.getConnections().getFirst().isClosed());
	}

	@Test
	public void testSerializableState() throws Exception {
		Environment env = new DummyEnvironment("test", 1, 0);
		DbStateBackend backend = new DbStateBackend(conf);
		String jobId = env.getJobID().toString().substring(0, 16);

		backend.initializeForJob(env, "dummy-ser-state", StringSerializer.INSTANCE);

		String state1 = "dummy state";
		String state2 = "row row row your boat";
		Integer state3 = 42;

		StateHandle<String> handle1 = backend.checkpointStateSerializable(state1, 439568923746L,
				System.currentTimeMillis());
		StateHandle<String> handle2 = backend.checkpointStateSerializable(state2, 439568923746L,
				System.currentTimeMillis());
		StateHandle<Integer> handle3 = backend.checkpointStateSerializable(state3, 439568923746L,
				System.currentTimeMillis());

		assertEquals(state1, handle1.getState(getClass().getClassLoader()));
		handle1.discardState();

		assertEquals(state2, handle2.getState(getClass().getClassLoader()));
		handle2.discardState();

		assertFalse(isTableEmpty(backend.getConnections().getFirst(), "checkpoints_" + jobId));

		assertEquals(state3, handle3.getState(getClass().getClassLoader()));
		handle3.discardState();

		assertTrue(isTableEmpty(backend.getConnections().getFirst(), "checkpoints_" + jobId));

		backend.close();

	}

	@Test
	public void testKeyValueState() throws Exception {

		// We will create the DbStateBackend backed with a FsStateBackend for
		// nonPartitioned states
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend fileBackend = new FsStateBackend(localFileUri(tempDir));

			DbStateBackend backend = new DbStateBackend(conf, fileBackend);

			Environment env = new DummyEnvironment("test", 2, 0);

			backend.initializeForJob(env, "dummy_test_kv", IntSerializer.INSTANCE);

			ValueState<String> state = backend.createValueState(IntSerializer.INSTANCE,
				new ValueStateDescriptor<>("state1", StringSerializer.INSTANCE, null));

			LazyDbValueState<Integer, Integer, String> kv = (LazyDbValueState<Integer, Integer, String>) state;

			String tableName = "dummy_test_kv_state1";
			assertTrue(isTableCreated(backend.getConnections().getFirst(), tableName));

			assertEquals(0, kv.size());

			kv.setCurrentNamespace(1);

			// some modifications to the state
			kv.setCurrentKey(1);
			assertNull(kv.value());
			kv.update("1");
			assertEquals(1, kv.size());
			kv.setCurrentKey(2);
			assertNull(kv.value());
			kv.update("2");
			assertEquals(2, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1", kv.value());
			assertEquals(2, kv.size());

			kv.snapshot(682375462378L, 100);

			// make some more modifications
			kv.setCurrentKey(1);
			kv.update("u1");
			kv.setCurrentKey(2);
			kv.update("u2");
			kv.setCurrentKey(3);
			kv.update("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, Integer, ValueState<String>, ValueStateDescriptor<String>, DbStateBackend> snapshot2 = kv.snapshot(682375462379L,
					200);

			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("u1", kv.value());
			kv.setCurrentKey(2);
			assertEquals("u2", kv.value());
			kv.setCurrentKey(3);
			assertEquals("u3", kv.value());

			// restore the first snapshot and validate it
			KvState<Integer, Integer, ValueState<String>, ValueStateDescriptor<String>, DbStateBackend> restored2 = snapshot2.restoreState(
				backend,
				IntSerializer.INSTANCE,
				getClass().getClassLoader(),
				6823754623710L);

			restored2.setCurrentNamespace(1);

			@SuppressWarnings("unchecked")
			ValueState<String> restoredState2 = (ValueState<String>) restored2;

			restored2.setCurrentKey(1);
			assertEquals("u1", restoredState2.value());
			restored2.setCurrentKey(2);
			assertEquals("u2", restoredState2.value());
			restored2.setCurrentKey(3);
			assertEquals("u3", restoredState2.value());

			backend.close();
		} finally {
			deleteDirectorySilently(tempDir);
		}
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testListState() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend fileBackend = new FsStateBackend(localFileUri(tempDir));

			DbStateBackend backend = new DbStateBackend(conf, fileBackend);

			Environment env = new DummyEnvironment("test", 2, 0);

			backend.initializeForJob(env, "dummy_test_kv_list", IntSerializer.INSTANCE);

			ListStateDescriptor<String> kvId = new ListStateDescriptor<>("id", StringSerializer.INSTANCE);
			ListState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			@SuppressWarnings("unchecked")
			KvState<Integer, Void, ListState<String>, ListStateDescriptor<String>, DbStateBackend> kv =
				(KvState<Integer, Void, ListState<String>, ListStateDescriptor<String>, DbStateBackend>) state;

			Joiner joiner = Joiner.on(",");
			// some modifications to the state
			kv.setCurrentKey(1);
			assertEquals("", joiner.join(state.get()));
			state.add("1");
			kv.setCurrentKey(2);
			assertEquals("", joiner.join(state.get()));
			state.add("2");
			kv.setCurrentKey(1);
			assertEquals("1", joiner.join(state.get()));

			// draw a snapshot
			KvStateSnapshot<Integer, Void, ListState<String>, ListStateDescriptor<String>, DbStateBackend> snapshot1 =
				kv.snapshot(682375462378L, 2);

			// make some more modifications
			kv.setCurrentKey(1);
			state.add("u1");
			kv.setCurrentKey(2);
			state.add("u2");
			kv.setCurrentKey(3);
			state.add("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, Void, ListState<String>, ListStateDescriptor<String>, DbStateBackend> snapshot2 =
				kv.snapshot(682375462379L, 4);

			// validate the original state
			kv.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(state.get()));
			kv.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(state.get()));
			kv.setCurrentKey(3);
			assertEquals("u3", joiner.join(state.get()));

			kv.dispose();

			// restore the second snapshot and validate it
			KvState<Integer, Void, ListState<String>, ListStateDescriptor<String>, DbStateBackend> restored2 = snapshot2.restoreState(
				backend,
				IntSerializer.INSTANCE,
				this.getClass().getClassLoader(), 20);

			@SuppressWarnings("unchecked")
			ListState<String> restored2State = (ListState<String>) restored2;

			restored2.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(restored2State.get()));
			restored2.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(restored2State.get()));
			restored2.setCurrentKey(3);
			assertEquals("u3", joiner.join(restored2State.get()));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	public void testReducingState() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend fileBackend = new FsStateBackend(localFileUri(tempDir));

			DbStateBackend backend = new DbStateBackend(conf, fileBackend);

			Environment env = new DummyEnvironment("test", 2, 0);

			backend.initializeForJob(env, "dummy_test_kv_reduce", IntSerializer.INSTANCE);

			ReducingStateDescriptor<String> kvId = new ReducingStateDescriptor<>("id",
				new ReduceFunction<String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String reduce(String value1, String value2) throws Exception {
						return value1 + "," + value2;
					}
				},
				StringSerializer.INSTANCE);
			ReducingState<String> state = backend.getPartitionedState(null, VoidSerializer.INSTANCE, kvId);

			@SuppressWarnings("unchecked")
			KvState<Integer, Void, ReducingState<String>, ReducingStateDescriptor<String>, DbStateBackend> kv =
				(KvState<Integer, Void, ReducingState<String>, ReducingStateDescriptor<String>, DbStateBackend>) state;

			Joiner joiner = Joiner.on(",");
			// some modifications to the state
			kv.setCurrentKey(1);
			assertEquals(null, state.get());
			state.add("1");
			kv.setCurrentKey(2);
			assertEquals(null, state.get());
			state.add("2");
			kv.setCurrentKey(1);
			assertEquals("1", state.get());

			// draw a snapshot
			KvStateSnapshot<Integer, Void, ReducingState<String>, ReducingStateDescriptor<String>, DbStateBackend> snapshot1 =
				kv.snapshot(682375462378L, 2);

			// make some more modifications
			kv.setCurrentKey(1);
			state.add("u1");
			kv.setCurrentKey(2);
			state.add("u2");
			kv.setCurrentKey(3);
			state.add("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, Void, ReducingState<String>, ReducingStateDescriptor<String>, DbStateBackend> snapshot2 =
				kv.snapshot(682375462379L, 4);

			// validate the original state
			kv.setCurrentKey(1);
			assertEquals("1,u1", state.get());
			kv.setCurrentKey(2);
			assertEquals("2,u2", state.get());
			kv.setCurrentKey(3);
			assertEquals("u3", state.get());

			kv.dispose();

			// restore the second snapshot and validate it
			KvState<Integer, Void, ReducingState<String>, ReducingStateDescriptor<String>, DbStateBackend> restored2 = snapshot2.restoreState(
				backend,
				IntSerializer.INSTANCE,
				this.getClass().getClassLoader(), 20);

			@SuppressWarnings("unchecked")
			ReducingState<String> restored2State = (ReducingState<String>) restored2;

			restored2.setCurrentKey(1);
			assertEquals("1,u1", restored2State.get());
			restored2.setCurrentKey(2);
			assertEquals("2,u2", restored2State.get());
			restored2.setCurrentKey(3);
			assertEquals("u3", restored2State.get());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCompaction() throws Exception {
		DbBackendConfig conf = new DbBackendConfig("flink", "flink", url1);
		MockAdapter adapter = new MockAdapter();
		conf.setKvStateCompactionFrequency(2);
		conf.setDbAdapter(adapter);

		DbStateBackend backend1 = new DbStateBackend(conf);
		DbStateBackend backend2 = new DbStateBackend(conf);
		DbStateBackend backend3 = new DbStateBackend(conf);

		backend1.initializeForJob(new DummyEnvironment("test", 3, 0), "dummy_1", StringSerializer.INSTANCE);
		backend2.initializeForJob(new DummyEnvironment("test", 3, 1), "dummy_2", StringSerializer.INSTANCE);
		backend3.initializeForJob(new DummyEnvironment("test", 3, 2), "dummy_3", StringSerializer.INSTANCE);

		ValueState<String> s1State = backend1.createValueState(StringSerializer.INSTANCE, 
				new ValueStateDescriptor<>("a1", StringSerializer.INSTANCE, null));
		
		ValueState<String> s2State = backend2.createValueState(StringSerializer.INSTANCE, 
				new ValueStateDescriptor<>("a2", StringSerializer.INSTANCE, null));
		
		ValueState<String> s3State = backend3.createValueState(StringSerializer.INSTANCE, 
				new ValueStateDescriptor<>("a3", StringSerializer.INSTANCE, null));

		LazyDbValueState<?, ?, ?> s1 = (LazyDbValueState<?, ?, ?>) s1State;
		LazyDbValueState<?, ?, ?> s2 = (LazyDbValueState<?, ?, ?>) s2State;
		LazyDbValueState<?, ?, ?> s3 = (LazyDbValueState<?, ?, ?>) s3State;

		assertTrue(s1.isCompactor());
		assertFalse(s2.isCompactor());
		assertFalse(s3.isCompactor());
		assertNotNull(s1.getExecutor());
		assertNull(s2.getExecutor());
		assertNull(s3.getExecutor());

		s1.snapshot(1, 100);
		s1.notifyCheckpointComplete(1);
		s1.snapshot(2, 200);
		s1.snapshot(3, 300);
		s1.notifyCheckpointComplete(2);
		s1.notifyCheckpointComplete(3);
		s1.snapshot(4, 400);
		s1.snapshot(5, 500);
		s1.notifyCheckpointComplete(4);
		s1.notifyCheckpointComplete(5);

		s1.dispose();
		s2.dispose();
		s3.dispose();

		// Wait until the compaction completes
		s1.getExecutor().awaitTermination(5, TimeUnit.SECONDS);
		assertEquals(2, adapter.numCompcations.get());
		assertEquals(5, adapter.keptAlive);

		backend1.close();
		backend2.close();
		backend3.close();
	}

	@Test
	public void testCaching() throws Exception {

		List<String> urls = Lists.newArrayList(url1, url2);
		DbBackendConfig conf = new DbBackendConfig("flink", "flink",
				urls);

		conf.setDbAdapter(new DerbyAdapter());
		conf.setKvCacheSize(3);
		conf.setMaxKvInsertBatchSize(2);

		// We evict 2 elements when the cache is full
		conf.setMaxKvCacheEvictFraction(0.6f);

		DbStateBackend backend = new DbStateBackend(conf);

		Environment env = new DummyEnvironment("test", 2, 0);

		String tableName = "dummy_test_caching_state1";
		assertFalse(isTableCreated(DriverManager.getConnection(url1, "flink", "flink"), tableName));
		assertFalse(isTableCreated(DriverManager.getConnection(url2, "flink", "flink"), tableName));

		backend.initializeForJob(env, "dummy_test_caching", IntSerializer.INSTANCE);

		ValueState<String> state = backend.createValueState(IntSerializer.INSTANCE,
			new ValueStateDescriptor<>("state1", StringSerializer.INSTANCE, "a"));

		LazyDbValueState<Integer, Integer, String> kv = (LazyDbValueState<Integer, Integer, String>) state;

		assertTrue(isTableCreated(DriverManager.getConnection(url1, "flink", "flink"), tableName));
		assertTrue(isTableCreated(DriverManager.getConnection(url2, "flink", "flink"), tableName));

		Map<Tuple2<Integer, Integer>, Optional<String>> cache = kv.getStateCache();
		Map<Tuple2<Integer, Integer>, Optional<String>> modified = kv.getModified();

		assertEquals(0, kv.size());

		kv.setCurrentNamespace(1);

		// some modifications to the state
		kv.setCurrentKey(1);
		assertEquals("a", kv.value());

		kv.update(null);
		assertEquals(1, kv.size());
		kv.setCurrentKey(2);
		assertEquals("a", kv.value());
		kv.update("2");
		assertEquals(2, kv.size());
		assertEquals("2", kv.value());

		kv.setCurrentKey(1);
		assertEquals("a", kv.value());

		kv.setCurrentKey(3);
		kv.update("3");
		assertEquals("3", kv.value());

		assertTrue(modified.containsKey(Tuple2.of(1, 1)));
		assertTrue(modified.containsKey(Tuple2.of(2, 1)));
		assertTrue(modified.containsKey(Tuple2.of(3, 1)));

		// 1,2 should be evicted as the cache filled
		kv.setCurrentKey(4);
		kv.update("4");
		assertEquals("4", kv.value());

		assertFalse(modified.containsKey(Tuple2.of(1, 1)));
		assertFalse(modified.containsKey(Tuple2.of(2, 1)));
		assertTrue(modified.containsKey(Tuple2.of(3, 1)));
		assertTrue(modified.containsKey(Tuple2.of(4, 1)));

		assertEquals(Optional.of("3"), cache.get(Tuple2.of(3, 1)));
		assertEquals(Optional.of("4"), cache.get(Tuple2.of(4, 1)));
		assertFalse(cache.containsKey(Tuple2.of(1, 1)));
		assertFalse(cache.containsKey(Tuple2.of(2, 1)));

		// draw a snapshot
		kv.snapshot(682375462378L, 100);

		assertTrue(modified.isEmpty());

		// make some more modifications
		kv.setCurrentKey(2);
		assertEquals("2", kv.value());
		kv.update(null);
		assertEquals("a", kv.value());

		assertTrue(modified.containsKey(Tuple2.of(2, 1)));
		assertEquals(1, modified.size());

		assertEquals(Optional.of("3"), cache.get(Tuple2.of(3, 1)));
		assertEquals(Optional.of("4"), cache.get(Tuple2.of(4, 1)));
		assertEquals(Optional.absent(), cache.get(Tuple2.of(2, 1)));
		assertFalse(cache.containsKey(Tuple2.of(1, 1)));

		assertTrue(modified.containsKey(Tuple2.of(2, 1)));
		assertFalse(modified.containsKey(Tuple2.of(3, 1)));
		assertFalse(modified.containsKey(Tuple2.of(4, 1)));
		assertTrue(cache.containsKey(Tuple2.of(3, 1)));
		assertTrue(cache.containsKey(Tuple2.of(4, 1)));

		// clear cache from initial keys

		kv.setCurrentKey(5);
		kv.value();
		kv.setCurrentKey(6);
		kv.value();
		kv.setCurrentKey(7);
		kv.value();

		assertFalse(modified.containsKey(Tuple2.of(5, 1)));
		assertTrue(modified.containsKey(Tuple2.of(6, 1)));
		assertTrue(modified.containsKey(Tuple2.of(7, 1)));

		assertFalse(cache.containsKey(Tuple2.of(1, 1)));
		assertFalse(cache.containsKey(Tuple2.of(2, 1)));
		assertFalse(cache.containsKey(Tuple2.of(3, 1)));
		assertFalse(cache.containsKey(Tuple2.of(4, 1)));

		kv.setCurrentKey(2);
		assertEquals("a", kv.value());

		long checkpointTs = System.currentTimeMillis();

		// Draw a snapshot that we will restore later
		KvStateSnapshot<Integer, Integer, ValueState<String>, ValueStateDescriptor<String>, DbStateBackend> snapshot1 = kv.snapshot(682375462379L, checkpointTs);

		assertTrue(modified.isEmpty());

		// Do some updates then draw another snapshot (imitate a partial
		// failure), these updates should not be visible if we restore snapshot1
		kv.setCurrentKey(1);
		kv.update("123");
		kv.setCurrentKey(3);
		kv.update("456");
		kv.setCurrentKey(2);
		kv.notifyCheckpointComplete(682375462379L);
		kv.update("2");
		kv.setCurrentKey(4);
		kv.update("4");
		kv.update("5");

		kv.snapshot(6823754623710L, checkpointTs + 10);

		// restore the second snapshot and validate it (we set a new default
		// value here to make sure that the default wasn't written)
		KvState<Integer, Integer, ValueState<String>, ValueStateDescriptor<String>, DbStateBackend> restored = snapshot1.restoreState(
			backend,
			IntSerializer.INSTANCE,
			getClass().getClassLoader(),
			6823754623711L);

		LazyDbValueState<Integer, Integer, String> lazyRestored = (LazyDbValueState<Integer, Integer, String>) restored;

		cache = lazyRestored.getStateCache();
		modified = lazyRestored.getModified();

		restored.setCurrentNamespace(1);

		@SuppressWarnings("unchecked")
		ValueState<String> restoredState = (ValueState<String>) restored;

		restored.setCurrentKey(1);

		assertEquals("a", restoredState.value());
		// make sure that we got the default and not some value from the db
		assertEquals(cache.get(Tuple2.of(1, 1)), Optional.<String>absent());
		restored.setCurrentKey(2);
		assertEquals("a", restoredState.value());
		// make sure that we got the default and not some value from the db
		assertEquals(cache.get(Tuple2.of(2, 1)), Optional.<String>absent());
		restored.setCurrentKey(3);
		assertEquals("3", restoredState.value());
		restored.setCurrentKey(4);
		assertEquals("4", restoredState.value());

		backend.close();
	}

	private static boolean isTableCreated(Connection con, String tableName) throws SQLException {
		return con.getMetaData().getTables(null, null, tableName.toUpperCase(), null).next();
	}

	private static boolean isTableEmpty(Connection con, String tableName) throws SQLException {
		try (Statement smt = con.createStatement()) {
			ResultSet res = smt.executeQuery("select * from " + tableName);
			return !res.next();
		}
	}

	private static String localFileUri(File path) {
		return path.toURI().toString();
	}

	private static void deleteDirectorySilently(File dir) {
		try {
			FileUtils.deleteDirectory(dir);
		} catch (IOException ignored) {
		}
	}

	private static class MockAdapter extends DerbyAdapter {

		private static final long serialVersionUID = 1L;
		public AtomicInteger numCompcations = new AtomicInteger(0);
		public int keptAlive = 0;

		@Override
		public void compactKvStates(String kvStateId, Connection con, long lowerTs, long upperTs) throws SQLException {
			numCompcations.incrementAndGet();
		}

		@Override
		public void keepAlive(Connection con) throws SQLException {
			keptAlive++;
		}
	}

}
