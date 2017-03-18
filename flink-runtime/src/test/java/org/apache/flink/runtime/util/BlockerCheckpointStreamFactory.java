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

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import java.io.IOException;

/**
 * {@link CheckpointStreamFactory} for tests that allows for testing cancellation in async IO
 */
@VisibleForTesting
@Internal
public class BlockerCheckpointStreamFactory implements CheckpointStreamFactory {

	private final int maxSize;
	private volatile int afterNumberInvocations;
	private volatile OneShotLatch blocker;
	private volatile OneShotLatch waiter;

	MemCheckpointStreamFactory.MemoryCheckpointOutputStream lastCreatedStream;

	public MemCheckpointStreamFactory.MemoryCheckpointOutputStream getLastCreatedStream() {
		return lastCreatedStream;
	}

	public BlockerCheckpointStreamFactory(int maxSize) {
		this.maxSize = maxSize;
	}

	public void setAfterNumberInvocations(int afterNumberInvocations) {
		this.afterNumberInvocations = afterNumberInvocations;
	}

	public void setBlockerLatch(OneShotLatch latch) {
		this.blocker = latch;
	}

	public void setWaiterLatch(OneShotLatch latch) {
		this.waiter = latch;
	}

	@Override
	public MemCheckpointStreamFactory.MemoryCheckpointOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		this.lastCreatedStream = new MemCheckpointStreamFactory.MemoryCheckpointOutputStream(maxSize) {

			private int afterNInvocations = afterNumberInvocations;
			private final OneShotLatch streamBlocker = blocker;
			private final OneShotLatch streamWaiter = waiter;

			@Override
			public void write(int b) throws IOException {

				if (null != waiter) {
					waiter.trigger();
				}

				if (afterNInvocations > 0) {
					--afterNInvocations;
				}

				if (0 == afterNInvocations && null != streamBlocker) {
					try {
						streamBlocker.await();
					} catch (InterruptedException ignored) {
					}
				}
				try {
					super.write(b);
				} catch (IOException ex) {
					if (null != streamWaiter) {
						streamWaiter.trigger();
					}
					throw ex;
				}

				if (0 == afterNInvocations && null != streamWaiter) {
					streamWaiter.trigger();
				}
			}

			@Override
			public void close() {
				super.close();
				if (null != streamWaiter) {
					streamWaiter.trigger();
				}
			}
		};

		return lastCreatedStream;
	}

	@Override
	public void close() throws Exception {

	}
}