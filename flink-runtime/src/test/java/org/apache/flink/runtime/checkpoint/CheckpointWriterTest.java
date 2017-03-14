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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializer;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializers;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1Test;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Tests for the logic to read/write checkpoint metadata via the {@link Checkpoints} class.
 */
public class CheckpointWriterTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	// ------------------------------------------------------------------------

	/**
	 * Simple write and read round trip.
	 */
	@Test
	public void testWriteAndReadMetadata() throws Exception {
		final SavepointV1 savepoint = new SavepointV1(1929292, SavepointV1Test.createTaskStates(4, 24));

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStream dos = new DataOutputStream(baos)) {
			Checkpoints.storeCheckpointMetadata(savepoint, dos);
		}

		final Savepoint loaded;
		try(ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			DataInputStream dis = new DataInputStream(bais))
		{
			loaded = Checkpoints.loadSavepoint(dis, getClass().getClassLoader());
		}

		assertEquals(savepoint, loaded);
	}

	/**
	 * Tests loading with unexpected magic number.
	 */
	@Test
	public void testUnexpectedMagicNumber() throws Exception {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		// Random data
		try (DataOutputStream dos = new DataOutputStream(baos)) {
			Random rnd = new Random();
			for (int i = 0; i < 10; i++) {
				dos.writeLong(rnd.nextLong());
			}
		}

		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
			Checkpoints.loadSavepoint(dis, getClass().getClassLoader());
			fail("Did not throw expected Exception");
		}
		catch (IOException e) {
			assertTrue(e.getMessage().contains("Flink 1.0") && e.getMessage().contains("Unexpected magic number"));
		}
	}

	/**
	 * Tests loading with unexpected savepoint version.
	 */
	@Test
	public void testUnexpectedVersion() throws Exception {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try (DataOutputStream dos = new DataOutputStream(baos)) {
			// header with non-existing version
			dos.writeLong(Checkpoints.HEADER_MAGIC_NUMBER);
			dos.writeInt(155555);

			// some more random data
			Random rnd = new Random();
			for (int i = 0; i < 10; i++) {
				dos.writeLong(rnd.nextLong());
			}
		}

		try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
			Checkpoints.loadSavepoint(dis, getClass().getClassLoader());
			fail("Did not throw expected Exception");
		}
		catch (IOException ignored) {
			// expected
		}
	}

	/**
	 * Tests addition of a new savepoint version.
	 */
	@Test
	public void testMultipleSavepointVersions() throws Exception {
		
		// New savepoint type for test
		final long checkpointId = ThreadLocalRandom.current().nextLong();
		final int version = ThreadLocalRandom.current().nextInt();

		final Savepoint currentCheckpoint = new SavepointV1(checkpointId, SavepointV1Test.createTaskStates(4, 32));
		final TestCheckpoint newCheckpoint = new TestCheckpoint(version, checkpointId);

		// Add serializer
		Field field = SavepointSerializers.class.getDeclaredField("SERIALIZERS");
		field.setAccessible(true);
		@SuppressWarnings("unchecked")
		final Map<Integer, SavepointSerializer<?>> serializers = (Map<Integer, SavepointSerializer<?>>) field.get(null);
		assertTrue(serializers.size() >= 1);
		serializers.put(version, NewSavepointSerializer.INSTANCE);

		// write the checkpoint metadata
		final ByteArrayOutputStream data1 = new ByteArrayOutputStream();
		final ByteArrayOutputStream data2 = new ByteArrayOutputStream();

		try (DataOutputStream dos = new DataOutputStream(data1)) {
			Checkpoints.storeCheckpointMetadata(currentCheckpoint, dos);
		}
		try (DataOutputStream dos = new DataOutputStream(data2)) {
			Checkpoints.storeCheckpointMetadata(newCheckpoint, dos);
		}

		final Savepoint loaded1;
		try(ByteArrayInputStream bais = new ByteArrayInputStream(data1.toByteArray()); 
			DataInputStream dis = new DataInputStream(bais))
		{
			loaded1 = Checkpoints.loadSavepoint(dis, getClass().getClassLoader());
		}

		final Savepoint loaded2;
		try(ByteArrayInputStream bais = new ByteArrayInputStream(data2.toByteArray());
			DataInputStream dis = new DataInputStream(bais))
		{
			loaded2 = Checkpoints.loadSavepoint(dis, getClass().getClassLoader());
		}

		assertEquals(currentCheckpoint, loaded1);
		assertEquals(newCheckpoint, loaded2);
	}

	// ------------------------------------------------------------------------
	//  Test Checkpoint and Serializer 
	// ------------------------------------------------------------------------

	private static class NewSavepointSerializer implements SavepointSerializer<TestCheckpoint> {

		static final NewSavepointSerializer INSTANCE = new NewSavepointSerializer();

		@Override
		public void serialize(TestCheckpoint savepoint, DataOutputStream dos) throws IOException {
			dos.writeInt(savepoint.version);
			dos.writeLong(savepoint.checkpointId);
		}

		@Override
		public TestCheckpoint deserialize(DataInputStream dis, ClassLoader userCL) throws IOException {
			int version = dis.readInt();
			long checkpointId = dis.readLong();
			return new TestCheckpoint(version, checkpointId);
		}

	}

	private static class TestCheckpoint implements Savepoint {

		private final int version;
		private final long checkpointId;

		public TestCheckpoint(int version, long checkpointId) {
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
			return Collections.emptyList();
		}

		@Override
		public void dispose() {}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TestCheckpoint that = (TestCheckpoint) o;
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
