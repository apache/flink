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

import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateIdentifier;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.IntValueSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.state.filesystem.FileStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.types.IntValue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

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

			backend.initializeForJob(new JobID(), IntSerializer.INSTANCE, this.getClass().getClassLoader());
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
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new JobID(), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			String state1 = "dummy state";
			String state2 = "row row row your boat";
			Integer state3 = 42;

			StateHandle<String> handle1 = backend.checkpointStateSerializable(state1, 439568923746L, System.currentTimeMillis());
			StateHandle<String> handle2 = backend.checkpointStateSerializable(state2, 439568923746L, System.currentTimeMillis());
			StateHandle<Integer> handle3 = backend.checkpointStateSerializable(state3, 439568923746L, System.currentTimeMillis());

			assertFalse(isDirectoryEmpty(checkpointDir));
			assertEquals(state1, handle1.getState(getClass().getClassLoader()));
			handle1.discardState();

			assertFalse(isDirectoryEmpty(checkpointDir));
			assertEquals(state2, handle2.getState(getClass().getClassLoader()));
			handle2.discardState();

			assertFalse(isDirectoryEmpty(checkpointDir));
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
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new JobID(), IntSerializer.INSTANCE, this.getClass().getClassLoader());

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

			FileStreamStateHandle handle1 = stream1.closeAndGetHandle();
			FileStreamStateHandle handle2 = stream2.closeAndGetHandle();
			FileStreamStateHandle handle3 = stream3.closeAndGetHandle();

			// use with try-with-resources
			StreamStateHandle handle4;
			try (AbstractStateBackend.CheckpointStateOutputStream stream4 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = stream4.closeAndGetHandle();
			}

			// close before accessing handle
			AbstractStateBackend.CheckpointStateOutputStream stream5 =
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
			assertFalse(isDirectoryEmpty(checkpointDir));
			ensureLocalFileDeleted(handle2.getFilePath());

			validateBytesInStream(handle3.getState(getClass().getClassLoader()), state3);
			handle3.discardState();
			assertFalse(isDirectoryEmpty(checkpointDir));
			ensureLocalFileDeleted(handle3.getFilePath());

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
	public void testValueState() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new JobID(), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			ValueStateIdentifier<String> kvId = new ValueStateIdentifier<>("id", null, StringSerializer.INSTANCE);
			ValueState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> kv =
					(KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend>) state;

			assertEquals(0, kv.size());

			// some modifications to the state
			kv.setCurrentKey(1);
			assertNull(state.value());
			state.update("1");
			assertEquals(1, kv.size());
			kv.setCurrentKey(2);
			assertNull(state.value());
			state.update("2");
			assertEquals(2, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1", state.value());
			assertEquals(2, kv.size());

			// draw a snapshot
			KvStateSnapshot<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> snapshot1 =
					kv.snapshot(682375462378L, System.currentTimeMillis());

			// make some more modifications
			kv.setCurrentKey(1);
			state.update("u1");
			kv.setCurrentKey(2);
			state.update("u2");
			kv.setCurrentKey(3);
			state.update("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> snapshot2 =
					kv.snapshot(682375462379L, System.currentTimeMillis());

			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("u1", state.value());
			kv.setCurrentKey(2);
			assertEquals("u2", state.value());
			kv.setCurrentKey(3);
			assertEquals("u3", state.value());

			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> restored1 = snapshot1.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ValueState<String> restored1State = (ValueState<String>) restored1;

			assertEquals(2, restored1.size());
			restored1.setCurrentKey(1);
			assertEquals("1", restored1State.value());
			restored1.setCurrentKey(2);
			assertEquals("2", restored1State.value());

			// restore the first snapshot and validate it
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> restored2 = snapshot2.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ValueState<String> restored2State = (ValueState<String>) restored2;

			assertEquals(3, restored2.size());
			restored2.setCurrentKey(1);
			assertEquals("u1", restored2State.value());
			restored2.setCurrentKey(2);
			assertEquals("u2", restored2State.value());
			restored2.setCurrentKey(3);
			assertEquals("u3", restored2State.value());

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
	public void testListState() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new JobID(), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			ListStateIdentifier<String> kvId = new ListStateIdentifier<>("id", StringSerializer.INSTANCE);
			ListState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> kv =
					(KvState<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend>) state;

			assertEquals(0, kv.size());

			Joiner joiner = Joiner.on(",");
			// some modifications to the state
			kv.setCurrentKey(1);
			assertEquals("", joiner.join(state.get()));
			state.add("1");
			assertEquals(1, kv.size());
			kv.setCurrentKey(2);
			assertEquals("", joiner.join(state.get()));
			state.add("2");
			assertEquals(2, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1", joiner.join(state.get()));
			assertEquals(2, kv.size());

			// draw a snapshot
			KvStateSnapshot<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> snapshot1 =
					kv.snapshot(682375462378L, System.currentTimeMillis());

			// make some more modifications
			kv.setCurrentKey(1);
			state.add("u1");
			kv.setCurrentKey(2);
			state.add("u2");
			kv.setCurrentKey(3);
			state.add("u3");

			// draw another snapshot
			KvStateSnapshot<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> snapshot2 =
					kv.snapshot(682375462379L, System.currentTimeMillis());

			// validate the original state
			assertEquals(3, kv.size());
			kv.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(state.get()));
			kv.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(state.get()));
			kv.setCurrentKey(3);
			assertEquals("u3", joiner.join(state.get()));

			// restore the first snapshot and validate it
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> restored1 = snapshot1.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ListState<String> restored1State = (ListState<String>) restored1;

			assertEquals(2, restored1.size());
			restored1.setCurrentKey(1);
			assertEquals("1", joiner.join(restored1State.get()));
			restored1.setCurrentKey(2);
			assertEquals("2", joiner.join(restored1State.get()));

			// restore the second snapshot and validate it
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> restored2 = snapshot2.restoreState(
					backend,
					IntSerializer.INSTANCE,
					kvId,
					this.getClass().getClassLoader());

			@SuppressWarnings("unchecked")
			ListState<String> restored2State = (ListState<String>) restored2;

			assertEquals(3, restored2.size());
			restored2.setCurrentKey(1);
			assertEquals("1,u1", joiner.join(restored2State.get()));
			restored2.setCurrentKey(2);
			assertEquals("2,u2", joiner.join(restored2State.get()));
			restored2.setCurrentKey(3);
			assertEquals("u3", joiner.join(restored2State.get()));

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
	public void testValueStateRestoreWithWrongSerializers() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			ValueStateIdentifier<String> kvId = new ValueStateIdentifier<>("id", null, StringSerializer.INSTANCE);
			ValueState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> kv =
					(KvState<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend>) state;

			kv.setCurrentKey(1);
			state.update("1");
			kv.setCurrentKey(2);
			state.update("2");

			KvStateSnapshot<Integer, ValueState<String>, ValueStateIdentifier<String>, FsStateBackend> snapshot =
					kv.snapshot(682375462378L, System.currentTimeMillis());



			@SuppressWarnings("unchecked")
			TypeSerializer<Integer> fakeIntSerializer =
					(TypeSerializer<Integer>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						kvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			@SuppressWarnings("unchecked")
			ValueStateIdentifier<String> fakeKvId =
					(ValueStateIdentifier<String>)(ValueStateIdentifier<?>) new ValueStateIdentifier<>("id", null, FloatSerializer.INSTANCE);

			try {
				snapshot.restoreState(backend, IntSerializer.INSTANCE,
						fakeKvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						fakeKvId, getClass().getClassLoader());
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
	public void testListStateRestoreWithWrongSerializers() {
		File tempDir = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(localFileUri(tempDir)));
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			File checkpointDir = new File(backend.getCheckpointDirectory().toUri().getPath());

			ListStateIdentifier<String> kvId = new ListStateIdentifier<>("id", StringSerializer.INSTANCE);
			ListState<String> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> kv =
					(KvState<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend>) state;

			kv.setCurrentKey(1);
			state.add("1");
			kv.setCurrentKey(2);
			state.add("2");

			KvStateSnapshot<Integer, ListState<String>, ListStateIdentifier<String>, FsStateBackend> snapshot =
					kv.snapshot(682375462378L, System.currentTimeMillis());



			@SuppressWarnings("unchecked")
			TypeSerializer<Integer> fakeIntSerializer =
					(TypeSerializer<Integer>) (TypeSerializer<?>) FloatSerializer.INSTANCE;

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						kvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception");
			}

			@SuppressWarnings("unchecked")
			ListStateIdentifier<String> fakeKvId =
					(ListStateIdentifier<String>)(ListStateIdentifier<?>) new ListStateIdentifier<>("id", FloatSerializer.INSTANCE);

			try {
				snapshot.restoreState(backend, IntSerializer.INSTANCE,
						fakeKvId, getClass().getClassLoader());
				fail("should recognize wrong serializers");
			} catch (IllegalArgumentException e) {
				// expected
			} catch (Exception e) {
				fail("wrong exception " + e);
			}

			try {
				snapshot.restoreState(backend, fakeIntSerializer,
						fakeKvId, getClass().getClassLoader());
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
			backend.initializeForJob(new JobID(0L, 0L), IntSerializer.INSTANCE, this.getClass().getClassLoader());

			ValueStateIdentifier<IntValue> kvId = new ValueStateIdentifier<>("id", new IntValue(-1), IntValueSerializer.INSTANCE);
			ValueState<IntValue> state = kvId.bind(backend);

			@SuppressWarnings("unchecked")
			KvState<Integer, ValueState<IntValue>, ValueStateIdentifier<IntValue>, FsStateBackend> kv =
					(KvState<Integer, ValueState<IntValue>, ValueStateIdentifier<IntValue>, FsStateBackend>) state;

			kv.setCurrentKey(1);
			IntValue default1 = state.value();

			kv.setCurrentKey(2);
			IntValue default2 = state.value();

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
		return  nested == null || nested.length == 0;
	}

	private static String localFileUri(File path) {
		return path.toURI().toString();
	}

	private static void validateBytesInStream(InputStream is, byte[] data) throws IOException {
		byte[] holder = new byte[data.length];
		assertEquals("not enough data", holder.length, is.read(holder));
		assertEquals("too much data", -1, is.read());
		assertArrayEquals("wrong data", data, holder);
	}
}
