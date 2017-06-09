/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class KeyedStateCheckpointOutputStreamTest {

	private static final int STREAM_CAPACITY = 128;

	private static KeyedStateCheckpointOutputStream createStream(KeyGroupRange keyGroupRange) {
		CheckpointStreamFactory.CheckpointStateOutputStream checkStream =
				new TestMemoryCheckpointOutputStream(STREAM_CAPACITY);
		return new KeyedStateCheckpointOutputStream(checkStream, keyGroupRange);
	}

	private KeyGroupsStateHandle writeAllTestKeyGroups(
			KeyedStateCheckpointOutputStream stream, KeyGroupRange keyRange) throws Exception {

		DataOutputView dov = new DataOutputViewStreamWrapper(stream);
		for (int kg : keyRange) {
			stream.startNewKeyGroup(kg);
			dov.writeInt(kg);
		}

		return stream.closeAndGetHandle();
	}

	@Test
	public void testCloseNotPropagated() throws Exception {
		KeyedStateCheckpointOutputStream stream = createStream(new KeyGroupRange(0, 0));
		TestMemoryCheckpointOutputStream innerStream = (TestMemoryCheckpointOutputStream) stream.getDelegate();
		stream.close();
		Assert.assertFalse(innerStream.isClosed());
	}

	@Test
	public void testEmptyKeyedStream() throws Exception {
		final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
		KeyedStateCheckpointOutputStream stream = createStream(keyRange);
		TestMemoryCheckpointOutputStream innerStream = (TestMemoryCheckpointOutputStream) stream.getDelegate();
		KeyGroupsStateHandle emptyHandle = stream.closeAndGetHandle();
		Assert.assertTrue(innerStream.isClosed());
		Assert.assertEquals(null, emptyHandle);
	}

	@Test
	public void testWriteReadRoundtrip() throws Exception {
		final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
		KeyedStateCheckpointOutputStream stream = createStream(keyRange);
		KeyGroupsStateHandle fullHandle = writeAllTestKeyGroups(stream, keyRange);
		Assert.assertNotNull(fullHandle);

		verifyRead(fullHandle, keyRange);
	}

	@Test
	public void testWriteKeyGroupTracking() throws Exception {
		final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
		KeyedStateCheckpointOutputStream stream = createStream(keyRange);

		try {
			stream.startNewKeyGroup(4711);
			Assert.fail();
		} catch (IllegalArgumentException expected) {
			// good
		}

		Assert.assertEquals(-1, stream.getCurrentKeyGroup());

		DataOutputView dov = new DataOutputViewStreamWrapper(stream);
		int previous = -1;
		for (int kg : keyRange) {
			Assert.assertFalse(stream.isKeyGroupAlreadyStarted(kg));
			Assert.assertFalse(stream.isKeyGroupAlreadyFinished(kg));
			stream.startNewKeyGroup(kg);
			if(-1 != previous) {
				Assert.assertTrue(stream.isKeyGroupAlreadyStarted(previous));
				Assert.assertTrue(stream.isKeyGroupAlreadyFinished(previous));
			}
			Assert.assertTrue(stream.isKeyGroupAlreadyStarted(kg));
			Assert.assertFalse(stream.isKeyGroupAlreadyFinished(kg));
			dov.writeInt(kg);
			previous = kg;
		}

		KeyGroupsStateHandle fullHandle = stream.closeAndGetHandle();

		verifyRead(fullHandle, keyRange);

		for (int kg : keyRange) {
			try {
				stream.startNewKeyGroup(kg);
				Assert.fail();
			} catch (IOException ex) {
				// required
			}
		}
	}

	@Test
	public void testReadWriteMissingKeyGroups() throws Exception {
		final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
		KeyedStateCheckpointOutputStream stream = createStream(keyRange);

		DataOutputView dov = new DataOutputViewStreamWrapper(stream);
		stream.startNewKeyGroup(1);
		dov.writeInt(1);

		KeyGroupsStateHandle fullHandle = stream.closeAndGetHandle();

		int count = 0;
		try (FSDataInputStream in = fullHandle.openInputStream()) {
			DataInputView div = new DataInputViewStreamWrapper(in);
			for (int kg : fullHandle.getKeyGroupRange()) {
				long off = fullHandle.getOffsetForKeyGroup(kg);
				if (off >= 0) {
					in.seek(off);
					Assert.assertEquals(1, div.readInt());
					++count;
				}
			}
		}

		Assert.assertEquals(1, count);
	}

	private static void verifyRead(KeyGroupsStateHandle fullHandle, KeyGroupRange keyRange) throws IOException {
		int count = 0;
		try (FSDataInputStream in = fullHandle.openInputStream()) {
			DataInputView div = new DataInputViewStreamWrapper(in);
			for (int kg : fullHandle.getKeyGroupRange()) {
				long off = fullHandle.getOffsetForKeyGroup(kg);
				in.seek(off);
				Assert.assertEquals(kg, div.readInt());
				++count;
			}
		}

		Assert.assertEquals(keyRange.getNumberOfKeyGroups(), count);
	}
}