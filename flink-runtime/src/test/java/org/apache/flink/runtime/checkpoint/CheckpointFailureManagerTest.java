/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_EXPIRED;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the checkpoint failure manager.
 */
public class CheckpointFailureManagerTest extends TestLogger {

	@Test
	public void testOnlySucceededCheckpointIdRemoved(){
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);
		failureManager.handleJobLevelCheckpointException(new CheckpointException(CHECKPOINT_EXPIRED), of(1L)); // remember
		failureManager.handleJobLevelCheckpointException(new CheckpointException(CHECKPOINT_EXPIRED), of(2L)); // remember
		failureManager.handleCheckpointSuccess(2L); // reset counter and forget 2L only
		failureManager.handleJobLevelCheckpointException(new CheckpointException(CHECKPOINT_EXPIRED), of(1L)); // ignore (seen)
		failureManager.handleJobLevelCheckpointException(new CheckpointException(CHECKPOINT_EXPIRED), of(3L)); // remember but don't fail (counter <= 2)
		failureManager.handleJobLevelCheckpointException(new CheckpointException(CHECKPOINT_EXPIRED), of(4L)); // remember but don't fail (counter <= 2)
		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testContinuousFailure() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleJobLevelCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(1L));
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(2L));

		//ignore this
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION), of(3L));

		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(4L));
		assertEquals(1, callback.getInvokeCounter());
	}

	@Test
	public void testBreakContinuousFailure() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleJobLevelCheckpointException(new CheckpointException(CheckpointFailureReason.EXCEPTION), of(1L));
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(2L));

		//ignore this
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION), of(3L));

		//reset
		failureManager.handleCheckpointSuccess(4);

		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CHECKPOINT_EXPIRED), of(5L));
		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testTotalCountValue() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(0, callback);
		for (CheckpointFailureReason reason : CheckpointFailureReason.values()) {
			failureManager.handleJobLevelCheckpointException(new CheckpointException(reason), empty());
		}

		assertEquals(1, callback.getInvokeCounter());
	}

	@Test
	public void testIgnoreOneCheckpointRepeatedlyCountMultiTimes() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleJobLevelCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(1L));
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(2L));

		//ignore this
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION), of(3L));

		//ignore repeatedly report from one checkpoint
		failureManager.handleJobLevelCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), of(2L));
		assertEquals(0, callback.getInvokeCounter());
	}

	/**
	 * A failure handler callback for testing.
	 */
	private static class TestFailJobCallback implements CheckpointFailureManager.FailJobCallback {

		private int invokeCounter = 0;

		@Override
		public void failJob(Throwable cause) {
			invokeCounter++;
		}

		@Override
		public void failJobDueToTaskFailure(final Throwable cause, final ExecutionAttemptID executionAttemptID) {
			invokeCounter++;
		}

		public int getInvokeCounter() {
			return invokeCounter;
		}
	}

}
