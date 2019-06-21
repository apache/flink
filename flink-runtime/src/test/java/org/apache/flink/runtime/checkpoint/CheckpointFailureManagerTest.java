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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the checkpoint failure manager.
 */
public class CheckpointFailureManagerTest extends TestLogger {

	@Test
	public void testBreakContinuousFailureWithDifferentReason() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.EXCEPTION), 1);
		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 2);

		//ignore this
		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION), 3);

		//reset
		failureManager.handleCheckpointSuccess(4);

		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED), 5);
		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testIgnoreOneCheckpointRepeatedlyCountMultiTimes() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 1);
		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 2);

		//ignore repeatedly report from one checkpoint
		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 2);
		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testIgnoreOtherFailureReason() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 1);
		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 2);

		//ignore this
		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION), 3);

		failureManager.handleCheckpointException(
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 4);

		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testOldSuccessDoesNotAffectNewFailedCheckpoint() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 1);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 2);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 6);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 7);
		failureManager.handleCheckpointSuccess(5);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 8);
		assertEquals(1, callback.getInvokeCounter());

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 3);
		assertEquals(1, callback.getInvokeCounter());
	}

	@Test
	public void testCheckContinuityForwardSuccess() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 6);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 7);
		assertEquals(0, callback.getInvokeCounter());

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 5);
		assertEquals(1, callback.getInvokeCounter());
	}

	@Test
	public void testCheckContinuityForwardFailed() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 6);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 7);
		assertEquals(0, callback.getInvokeCounter());

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 4);
		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testCheckContinuityBackSuccess() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 3);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 4);
		assertEquals(0, callback.getInvokeCounter());

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 5);
		assertEquals(1, callback.getInvokeCounter());
	}

	@Test
	public void testCheckContinuityBackFailed() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 3);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 4);
		assertEquals(0, callback.getInvokeCounter());

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 6);
		assertEquals(0, callback.getInvokeCounter());
	}

	@Test
	public void testIgnoreEarlierFailureCheckpointAfterNewSuccess() {
		TestFailJobCallback callback = new TestFailJobCallback();
		CheckpointFailureManager failureManager = new CheckpointFailureManager(2, callback);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 1);

		failureManager.handleCheckpointSuccess(5);

		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 2);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 3);
		failureManager.handleCheckpointException(new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED), 4);
		assertEquals(0, callback.getInvokeCounter());
	}

	/**
	 * A failure handler callback for testing.
	 */
	private static class TestFailJobCallback implements CheckpointFailureManager.FailJobCallback {

		private int invokeCounter = 0;

		@Override
		public void failJob() {
			invokeCounter++;
		}

		public int getInvokeCounter() {
			return invokeCounter;
		}
	}

}
