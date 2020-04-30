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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.CheckedThread;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.fail;

/**
 * This tests that the {@link KryoSerializer} properly fails when accessed by two threads
 * concurrently and that Kryo serializers are properly duplicated to use them in different threads.
 *
 * <p><b>Important:</b> This test only works if assertions are activated (-ea) on the JVM
 * when running tests.
 */
public class KryoSerializerConcurrencyTest {

	@Test
	public void testDuplicateSerializerWithDefaultSerializerClass() {
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.addDefaultKryoSerializer(WrappedString.class, TestSerializer.class);
		runDuplicateSerializerTest(executionConfig);
	}

	@Test
	public void testDuplicateSerializerWithDefaultSerializerInstance() {
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.addDefaultKryoSerializer(WrappedString.class, new TestSerializer());
		runDuplicateSerializerTest(executionConfig);
	}

	@Test
	public void testDuplicateSerializerWithRegisteredSerializerClass() {
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.registerTypeWithKryoSerializer(WrappedString.class, TestSerializer.class);
		runDuplicateSerializerTest(executionConfig);
	}

	@Test
	public void testDuplicateSerializerWithRegisteredSerializerInstance() {
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.registerTypeWithKryoSerializer(WrappedString.class, new TestSerializer());
		runDuplicateSerializerTest(executionConfig);
	}

	private void runDuplicateSerializerTest(ExecutionConfig executionConfig) {
		final KryoSerializer<WrappedString> original = new KryoSerializer<>(WrappedString.class, executionConfig);
		final KryoSerializer<WrappedString> duplicate = original.duplicate();

		WrappedString testString = new WrappedString("test");

		String copyWithOriginal = original.copy(testString).content;
		String copyWithDuplicate = duplicate.copy(testString).content;

		Assert.assertTrue(copyWithOriginal.startsWith(testString.content));
		Assert.assertTrue(copyWithDuplicate.startsWith(testString.content));

		// check that both serializer instances have appended a different identity hash
		Assert.assertNotEquals(copyWithOriginal, copyWithDuplicate);
	}

	@Test
	public void testConcurrentUseOfSerializer() throws Exception {
		final KryoSerializer<String> serializer = new KryoSerializer<>(String.class, new ExecutionConfig());

		final BlockerSync sync = new BlockerSync();

		final DataOutputView regularOut = new DataOutputSerializer(32);
		final DataOutputView lockingOut = new LockingView(sync);

		// this thread serializes and gets stuck there
		final CheckedThread thread = new CheckedThread("serializer") {
			@Override
			public void go() throws Exception {
				serializer.serialize("a value", lockingOut);
			}
		};

		thread.start();
		sync.awaitBlocker();

		// this should fail with an exception
		try {
			serializer.serialize("value", regularOut);
			fail("should have failed with an exception");
		}
		catch (IllegalStateException e) {
			// expected
		}
		finally {
			// release the thread that serializes
			sync.releaseBlocker();
		}

		// this propagates exceptions from the spawned thread
		thread.sync();
	}

	// ------------------------------------------------------------------------

	private static class LockingView extends DataOutputSerializer {

		private final BlockerSync blocker;

		LockingView(BlockerSync blocker) {
			super(32);
			this.blocker = blocker;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			blocker.blockNonInterruptible();
		}
	}

	/**
	 * A test class that wraps a string.
	 */
	public static class WrappedString {

		private final String content;

		WrappedString(String content) {
			this.content = content;
		}

		@Override
		public String toString() {
			return "WrappedString{" +
				"content='" + content + '\'' +
				'}';
		}
	}

	/**
	 * A test serializer for {@link WrappedString} that appends its identity hash.
	 */
	public static class TestSerializer extends Serializer<WrappedString> implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public void write(Kryo kryo, Output output, WrappedString object) {
			output.writeString(object.content);
		}

		@Override
		public WrappedString read(Kryo kryo, Input input, Class<WrappedString> type) {
			return new WrappedString(input.readString() + " " + System.identityHashCode(this));
		}
	}
}
