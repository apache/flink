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

package org.apache.flink.runtime.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.IntValueSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.filesystem.FileStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;

import org.junit.Test;

public class FileStateBackendTest {

	@Test
	public void testSetupAndSerialization() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			final String backendDir = localFileUri(tempDir);
			FsStateBackend originalBackend = new FsStateBackend(backendDir);

			assertFalse(originalBackend.isInitialized());
			assertEquals(new URI(backendDir), originalBackend.getBasePath().toUri());
			assertNull(originalBackend.getCheckpointDirectory());

			// serialize / copy the backend
			FsStateBackend backend = CommonTestUtils.createCopySerializable(originalBackend);
			assertFalse(backend.isInitialized());
			assertEquals(new URI(backendDir), backend.getBasePath().toUri());
			assertNull(backend.getCheckpointDirectory());

			// no file operations should be possible right now
			try {
				backend.checkpointStateSerializable("exception train rolling in", 2L, System.currentTimeMillis());
				fail("should fail with an exception");
			} catch (IllegalStateException e) {
				// supreme!
			}

			backend.initializeForJob(new DummyEnvironment("test", 1, 0));
			assertNotNull(backend.getCheckpointDirectory());

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());
			assertTrue(checkpointDir.exists());
			assertTrue(isDirectoryEmpty(checkpointDir));

			backend.disposeAllStateForCurrentJob();
			assertNull(backend.getCheckpointDirectory());

			assertTrue(isDirectoryEmpty(tempDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			deleteDirectorySilently(tempDir);
		}
	}

	@Test
	public void testSerializableState() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(
				new FsStateBackend(tempDir.toURI(), 40));
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			String state1 = "dummy state";
			String state2 = "row row row your boat";
			Integer state3 = 42;

			StateHandle<String> handle1 = backend.checkpointStateSerializable(state1, 439568923746L, System.currentTimeMillis());
			StateHandle<String> handle2 = backend.checkpointStateSerializable(state2, 439568923746L, System.currentTimeMillis());
			StateHandle<Integer> handle3 = backend.checkpointStateSerializable(state3, 439568923746L, System.currentTimeMillis());
			
			assertEquals(state1, handle1.getState(getClass().getClassLoader()));
			handle1.discardState();
			
			assertEquals(state2, handle2.getState(getClass().getClassLoader()));
			handle2.discardState();
			
			assertEquals(state3, handle3.getState(getClass().getClassLoader()));
			handle3.discardState();

			assertTrue(isDirectoryEmpty(checkpointDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			deleteDirectorySilently(tempDir);
		}
	}

	@Test
	public void testStateOutputStream() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			// the state backend has a very low in-mem state threshold (15 bytes)
			FsStateBackend backend = CommonTestUtils.createCopySerializable(
				new FsStateBackend(tempDir.toURI(), 15));
			
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			byte[] state1 = new byte[1274673];
			byte[] state2 = new byte[1];
			byte[] state3 = new byte[0];
			byte[] state4 = new byte[177];

			Random rnd = new Random();
			rnd.nextBytes(state1);
			rnd.nextBytes(state2);
			rnd.nextBytes(state3);
			rnd.nextBytes(state4);

			long checkpointId = 97231523452L;

			FsStateBackend.FsCheckpointStateOutputStream stream1 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			FsStateBackend.FsCheckpointStateOutputStream stream2 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			FsStateBackend.FsCheckpointStateOutputStream stream3 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			stream1.write(state1);
			stream2.write(state2);
			stream3.write(state3);

			FileStreamStateHandle handle1 = (FileStreamStateHandle) stream1.closeAndGetHandle();
			ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
			ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

			// use with try-with-resources
			FileStreamStateHandle handle4;
			try (StateBackend.CheckpointStateOutputStream stream4 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = (FileStreamStateHandle) stream4.closeAndGetHandle();
			}

			// close before accessing handle
			StateBackend.CheckpointStateOutputStream stream5 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			stream5.write(state4);
			stream5.close();
			try {
				stream5.closeAndGetHandle();
				fail();
			} catch (IOException e) {
				// uh-huh
			}

			validateBytesInStream(handle1.getState(getClass().getClassLoader()), state1);
			handle1.discardState();
			assertFalse(isDirectoryEmpty(checkpointDir));
			ensureLocalFileDeleted(handle1.getFilePath());

			validateBytesInStream(handle2.getState(getClass().getClassLoader()), state2);
			handle2.discardState();

			validateBytesInStream(handle3.getState(getClass().getClassLoader()), state3);
			handle3.discardState();

			validateBytesInStream(handle4.getState(getClass().getClassLoader()), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			deleteDirectorySilently(tempDir);
		}
	}

	@Test
	public void testKeyValueState() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			KvState<Integer, String, FsStateBackend> kv =
					backend.createKvState("0", "a", IntSerializer.INSTANCE, StringSerializer.INSTANCE, null);

			assertEquals(0, kv.size());

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

			// draw a snapshot
			KvStateSnapshot<Integer, String, FsStateBackend> snapshot1 =
					kv.snapshot(682375462378L, System.currentTimeMillis());

			// make some more modifications
			kv.setCurrentKey(1);
			kv.update("u1");
			kv.setCurrentKey(2);
			kv.update("u2");
			kv.setCurrentKey(3);
			kv.update("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, String, FsStateBackend> snapshot2 =
					kv.snapshot(682375462379L, System.currentTimeMillis());

			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("u1", kv.value());
			kv.setCurrentKey(2);
			assertEquals("u2", kv.value());
			kv.setCurrentKey(3);
			assertEquals("u3", kv.value());

			// restore the first snapshot and validate it
			KvState<Integer, String, FsStateBackend> restored1 = snapshot1.restoreState(backend,
					IntSerializer.INSTANCE, StringSerializer.INSTANCE, null, getClass().getClassLoader(), 1);

			assertEquals(2, restored1.size());
			restored1.setCurrentKey(1);
			assertEquals("1", restored1.value());
			restored1.setCurrentKey(2);
			assertEquals("2", restored1.value());

			// restore the first snapshot and validate it
			KvState<Integer, String, FsStateBackend> restored2 = snapshot2.restoreState(backend,
					IntSerializer.INSTANCE, StringSerializer.INSTANCE, null, getClass().getClassLoader(), 1);

			assertEquals(3, restored2.size());
			restored2.setCurrentKey(1);
			assertEquals("u1", restored2.value());
			restored2.setCurrentKey(2);
			assertEquals("u2", restored2.value());
			restored2.setCurrentKey(3);
			assertEquals("u3", restored2.value());

			snapshot1.discardState();
			assertFalse(isDirectoryEmpty(checkpointDir));

			snapshot2.discardState();
			assertTrue(isDirectoryEmpty(checkpointDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			deleteDirectorySilently(tempDir);
		}
	}

	@Test
	public void testRestoreWithWrongSerializers() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			KvState<Integer, String, FsStateBackend> kv =
					backend.createKvState("a_0", "a", IntSerializer.INSTANCE, StringSerializer.INSTANCE, null);

			kv.setCurrentKey(1);
			kv.update("1");
			kv.setCurrentKey(2);
			kv.update("2");

			KvStateSnapshot<Integer, String, FsStateBackend> snapshot =
					kv.snapshot(682375462378L, System.currentTimeMillis());


			@SuppressWarnings("unchecked")
			TypeSerializer<Integer> fakeIntSerializer =
					(TypeSerializer<Integer>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			@SuppressWarnings("unchecked")
			TypeSerializer<String> fakeStringSerializer =
					(TypeSerializer<String>) (TypeSerializer<?>) new ValueSerializer<StringValue>(StringValue.class);

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						StringSerializer.INSTANCE, null, getClass().getClassLoader(), 1);
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			try {
				snapshot.restoreState(backend, IntSerializer.INSTANCE,
						fakeStringSerializer, null, getClass().getClassLoader(), 1);
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						fakeStringSerializer, null, getClass().getClassLoader(), 1);
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			snapshot.discardState();

			assertTrue(isDirectoryEmpty(checkpointDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			deleteDirectorySilently(tempDir);
		}
	}

	@Test
	public void testCopyDefaultValue() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			KvState<Integer, IntValue, FsStateBackend> kv =
					backend.createKvState("a_0", "a", IntSerializer.INSTANCE, IntValueSerializer.INSTANCE, new IntValue(-1));

			kv.setCurrentKey(1);
			IntValue default1 = kv.value();

			kv.setCurrentKey(2);
			IntValue default2 = kv.value();

			assertNotNull(default1);
			assertNotNull(default2);
			assertEquals(default1, default2);
			assertFalse(default1 == default2);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			deleteDirectorySilently(tempDir);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static void ensureLocalFileDeleted(Path path) {
		URI uri = path.toUri();
		if ("file".equals(uri.getScheme())) {
			File file = new File(uri.getPath());
			assertFalse("file not properly deleted", file.exists());
		}
		else {
			throw new IllegalArgumentException("not a local path");
		}
	}

	private static void deleteDirectorySilently(File dir) {
		try {
			FileUtils.deleteDirectory(dir);
		}
		catch (IOException ignored) {}
	}

	private static boolean isDirectoryEmpty(File directory) {
		String[] nested = directory.list();
		return nested == null || nested.length == 0;
	}

	private static String localFileUri(File path) {
		return path.toURI().toString();
	}

	private static void validateBytesInStream(InputStream is, byte[] data) throws IOException {
		byte[] holder = new byte[data.length];
		int numBytesRead = is.read(holder);
		
		if (holder.length == 0) {
			assertTrue("stream not empty", numBytesRead == 0 || numBytesRead == -1);
		} else {
			assertEquals("not enough data", holder.length, numBytesRead);
		}
		
		assertEquals("too much data", -1, is.read());
		assertArrayEquals("wrong data", data, holder);
	}
}
