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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link AbstractInvokable} that validates expected order of completed and aborted checkpoints.
 */
class CheckpointSequenceValidator extends AbstractInvokable {

	private final long[] checkpointIDs;

	private int i = 0;

	CheckpointSequenceValidator(long... checkpointIDs) {
		super(new DummyEnvironment("test", 1, 0));
		this.checkpointIDs = checkpointIDs;
	}

	@Override
	public void invoke() {
		throw new UnsupportedOperationException("should never be called");
	}

	@Override
	public Future<Boolean> triggerCheckpointAsync(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
		throw new UnsupportedOperationException("should never be called");
	}

	@Override
	public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
		assertTrue("Unexpected triggerCheckpointOnBarrier(" + checkpointMetaData.getCheckpointId() + ")", i < checkpointIDs.length);

		final long expectedId = checkpointIDs[i++];
		if (expectedId >= 0) {
			assertEquals("wrong checkpoint id", expectedId, checkpointMetaData.getCheckpointId());
			assertTrue(checkpointMetaData.getTimestamp() > 0);
		} else {
			fail(String.format(
				"got 'triggerCheckpointOnBarrier(%d)' when expecting an 'abortCheckpointOnBarrier(%d)'",
				checkpointMetaData.getCheckpointId(),
				expectedId));
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
		assertTrue("Unexpected abortCheckpointOnBarrier(" + checkpointId + ")", i < checkpointIDs.length);

		final long expectedId = checkpointIDs[i++];
		if (expectedId < 0) {
			assertEquals("wrong checkpoint id for checkpoint abort", -expectedId, checkpointId);
		} else {
			fail(String.format(
				"got 'abortCheckpointOnBarrier(%d)' when expecting an 'triggerCheckpointOnBarrier(%d)'",
				checkpointId,
				expectedId));
		}
	}

	@Override
	public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
		throw new UnsupportedOperationException("should never be called");
	}
}
