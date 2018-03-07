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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

/**
 * This tests that the {@link KryoSerializer} properly fails when accessed by two threads
 * concurrently.
 *
 * <p><b>Important:</b> This test only works if assertions are activated (-ea) on the JVM
 * when running tests.
 */
public class KryoSerializerConcurrencyTest {

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
}
