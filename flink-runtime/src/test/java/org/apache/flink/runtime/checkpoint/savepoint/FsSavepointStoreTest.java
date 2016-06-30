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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Matchers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class FsSavepointStoreTest {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	/**
	 * Tests a store-load-dispose sequence.
	 */
	@Test
	public void testStoreLoadDispose() throws Exception {
		FsSavepointStore store = new FsSavepointStore(tmp.getRoot().getPath(), "fs-savepoint-store-test-");
		assertEquals(0, tmp.getRoot().listFiles().length);

		// Store
		SavepointV0 stored = new SavepointV0(1929292, SavepointV01Test.createTaskStates(4, 24));
		String path = store.storeSavepoint(stored);
		assertEquals(1, tmp.getRoot().listFiles().length);

		// Load
		Savepoint loaded = store.loadSavepoint(path);
		assertEquals(stored, loaded);

		// Dispose
		store.disposeSavepoint(path, ClassLoader.getSystemClassLoader());

		assertEquals(0, tmp.getRoot().listFiles().length);
	}

	/**
	 * Tests loading with unexpected magic number.
	 */
	@Test
	public void testUnexpectedSavepoint() throws Exception {
		FsSavepointStore store = new FsSavepointStore(tmp.getRoot().getPath(), "fs-savepoint-store-test-");

		// Random file
		Path filePath = new Path(tmp.getRoot().getPath(), UUID.randomUUID().toString());
		FSDataOutputStream fdos = FileSystem.get(filePath.toUri()).create(filePath, false);
		DataOutputStream dos = new DataOutputStream(fdos);
		for (int i = 0; i < 10; i++) {
			dos.writeLong(ThreadLocalRandom.current().nextLong());
		}

		try {
			store.loadSavepoint(filePath.toString());
			fail("Did not throw expected Exception");
		} catch (RuntimeException e) {
			assertTrue(e.getMessage().contains("Flink 1.0") && e.getMessage().contains("Unexpected magic number"));
		}
	}

	/**
	 * Tests addition of a new savepoint version.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleSavepointVersions() throws Exception {
		Field field = SavepointSerializers.class.getDeclaredField("SERIALIZERS");
		field.setAccessible(true);
		Map<Integer, SavepointSerializer<?>> serializers = (Map<Integer, SavepointSerializer<?>>) field.get(null);

		assertTrue(serializers.size() >= 1);

		FsSavepointStore store = new FsSavepointStore(tmp.getRoot().getPath(), "fs-savepoint-store-test-");
		assertEquals(0, tmp.getRoot().listFiles().length);

		// New savepoint type for test
		int version = ThreadLocalRandom.current().nextInt();
		long checkpointId = ThreadLocalRandom.current().nextLong();

		// Add serializer
		serializers.put(version, NewSavepointSerializer.INSTANCE);

		TestSavepoint newSavepoint = new TestSavepoint(version, checkpointId);
		String pathNewSavepoint = store.storeSavepoint(newSavepoint);
		assertEquals(1, tmp.getRoot().listFiles().length);

		// Savepoint v0
		Savepoint savepoint = new SavepointV0(checkpointId, SavepointV01Test.createTaskStates(4, 32));
		String pathSavepoint = store.storeSavepoint(savepoint);
		assertEquals(2, tmp.getRoot().listFiles().length);

		// Load
		Savepoint loaded = store.loadSavepoint(pathNewSavepoint);
		assertEquals(newSavepoint, loaded);

		loaded = store.loadSavepoint(pathSavepoint);
		assertEquals(savepoint, loaded);
	}

	/**
	 * Tests that an exception during store cleans up the created savepoint file.
	 */
	@Test
	public void testCleanupOnStoreFailure() throws Exception {
		Field field = SavepointSerializers.class.getDeclaredField("SERIALIZERS");
		field.setAccessible(true);
		Map<Integer, SavepointSerializer<?>> serializers = (Map<Integer, SavepointSerializer<?>>) field.get(null);

		final int version = 123123;
		SavepointSerializer<TestSavepoint> serializer = mock(SavepointSerializer.class);
		doThrow(new RuntimeException("Test Exception")).when(serializer)
				.serialize(Matchers.any(TestSavepoint.class), any(DataOutputStream.class));

		FsSavepointStore store = new FsSavepointStore(tmp.getRoot().getPath(), "fs-savepoint-store-test-");
		serializers.put(version, serializer);

		Savepoint savepoint = new TestSavepoint(version, 12123123);

		assertEquals(0, tmp.getRoot().listFiles().length);

		try {
			store.storeSavepoint(savepoint);
		} catch (Throwable ignored) {
		}

		assertEquals("Savepoint file not cleaned up on failure", 0, tmp.getRoot().listFiles().length);
	}

	private static class NewSavepointSerializer implements SavepointSerializer<TestSavepoint> {

		private static final NewSavepointSerializer INSTANCE = new NewSavepointSerializer();

		@Override
		public void serialize(TestSavepoint savepoint, DataOutputStream dos) throws IOException {
			dos.writeInt(savepoint.version);
			dos.writeLong(savepoint.checkpointId);
		}

		@Override
		public TestSavepoint deserialize(DataInputStream dis) throws IOException {
			int version = dis.readInt();
			long checkpointId = dis.readLong();
			return new TestSavepoint(version, checkpointId);
		}

	}

	private static class TestSavepoint implements Savepoint {

		private final int version;
		private final long checkpointId;

		public TestSavepoint(int version, long checkpointId) {
			this.version = version;
			this.checkpointId = checkpointId;
		}

		@Override
		public int getVersion() {
			return version;
		}

		@Override
		public long getCheckpointId() {
			return checkpointId;
		}

		@Override
		public Collection<TaskState> getTaskStates() {
			return Collections.EMPTY_LIST;
		}

		@Override
		public void dispose(ClassLoader classLoader) {
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TestSavepoint that = (TestSavepoint) o;
			return version == that.version && checkpointId == that.checkpointId;

		}

		@Override
		public int hashCode() {
			int result = version;
			result = 31 * result + (int) (checkpointId ^ (checkpointId >>> 32));
			return result;
		}
	}

}
