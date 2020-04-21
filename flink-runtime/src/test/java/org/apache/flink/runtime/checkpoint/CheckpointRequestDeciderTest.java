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

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.runtime.util.clock.SystemClock;

import org.junit.Test;

import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * {@link CheckpointRequestDecider} test.
 */
public class CheckpointRequestDeciderTest {
	private static final Consumer<Long> NO_OP = unused -> {
	};

	@Test
	public void testForce() {
		CheckpointRequestDecider decider = new CheckpointRequestDecider(1, NO_OP, SystemClock.getInstance(), Integer.MAX_VALUE, () -> Integer.MAX_VALUE, new Object());
		CheckpointTriggerRequest request = new CheckpointTriggerRequest(0, CheckpointProperties.forSavepoint(true), null, true, false);
		assertEquals(Optional.of(request), decider.chooseRequestToExecute(request, false, 123));
	}

	@Test
	public void testEnqueueOnTooManyPending() {
		int maxConcurrentCheckpointAttempts = 1;
		int[] activeCheckpointsHolder = new int[1];
		CheckpointRequestDecider decider = new CheckpointRequestDecider(maxConcurrentCheckpointAttempts, NO_OP, SystemClock.getInstance(), 1, () -> activeCheckpointsHolder[0], new Object());

		CheckpointTriggerRequest request = new CheckpointTriggerRequest(0, CheckpointProperties.forSavepoint(false), null, true, false);

		activeCheckpointsHolder[0] = maxConcurrentCheckpointAttempts;
		assertFalse(decider.chooseRequestToExecute(request, false, 123).isPresent());

		activeCheckpointsHolder[0] = 0;
		assertEquals(Optional.of(request), decider.chooseQueuedRequestToExecute(false, 123));
	}

	@Test
	public void testSavepointsPrioritized() {
		CheckpointRequestDecider decider = new CheckpointRequestDecider(1, NO_OP, SystemClock.getInstance(), 1, () -> 0, new Object());

		CheckpointTriggerRequest r1 = new CheckpointTriggerRequest(0, CheckpointProperties.forCheckpoint(RETAIN_ON_CANCELLATION), null, true, false);
		CheckpointTriggerRequest r2 = new CheckpointTriggerRequest(0, CheckpointProperties.forSavepoint(false), null, true, false);
		assertFalse(decider.chooseRequestToExecute(r1, true, 123).isPresent());
		assertFalse(decider.chooseRequestToExecute(r2, true, 123).isPresent());
		assertEquals(Optional.of(r2), decider.chooseQueuedRequestToExecute(false, 123));
		assertEquals(Optional.of(r1), decider.chooseQueuedRequestToExecute(false, 123));
	}

}
