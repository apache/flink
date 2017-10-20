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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for the current implementations of {@link CheckpointExceptionHandler} and their factory.
 */
public class CheckpointExceptionHandlerTest extends TestLogger {

	@Test
	public void testRethrowingHandler() {
		DeclineDummyEnvironment environment = new DeclineDummyEnvironment();
		CheckpointExceptionHandlerFactory checkpointExceptionHandlerFactory = new CheckpointExceptionHandlerFactory();
		CheckpointExceptionHandler exceptionHandler =
			checkpointExceptionHandlerFactory.createCheckpointExceptionHandler(true, environment);

		CheckpointMetaData failedCheckpointMetaData = new CheckpointMetaData(42L, 4711L);
		Exception testException = new Exception("test");
		try {
			exceptionHandler.tryHandleCheckpointException(failedCheckpointMetaData, testException);
			Assert.fail("Exception not rethrown.");
		} catch (Exception e) {
			Assert.assertEquals(testException, e);
		}

		Assert.assertNull(environment.getLastDeclinedCheckpointCause());
	}

	@Test
	public void testDecliningHandler() {
		DeclineDummyEnvironment environment = new DeclineDummyEnvironment();
		CheckpointExceptionHandlerFactory checkpointExceptionHandlerFactory = new CheckpointExceptionHandlerFactory();
		CheckpointExceptionHandler exceptionHandler =
			checkpointExceptionHandlerFactory.createCheckpointExceptionHandler(false, environment);

		CheckpointMetaData failedCheckpointMetaData = new CheckpointMetaData(42L, 4711L);
		Exception testException = new Exception("test");
		try {
			exceptionHandler.tryHandleCheckpointException(failedCheckpointMetaData, testException);
		} catch (Exception e) {
			Assert.fail("Exception not handled, but rethrown.");
		}

		Assert.assertEquals(failedCheckpointMetaData.getCheckpointId(), environment.getLastDeclinedCheckpointId());
		Assert.assertEquals(testException, environment.getLastDeclinedCheckpointCause());
	}

	static final class DeclineDummyEnvironment extends DummyEnvironment {

		private long lastDeclinedCheckpointId;
		private Throwable lastDeclinedCheckpointCause;

		DeclineDummyEnvironment() {
			super("test", 1, 0);
			this.lastDeclinedCheckpointId = Long.MIN_VALUE;
			this.lastDeclinedCheckpointCause = null;
		}

		@Override
		public void declineCheckpoint(long checkpointId, Throwable cause) {
			this.lastDeclinedCheckpointId = checkpointId;
			this.lastDeclinedCheckpointCause = cause;
		}

		long getLastDeclinedCheckpointId() {
			return lastDeclinedCheckpointId;
		}

		Throwable getLastDeclinedCheckpointCause() {
			return lastDeclinedCheckpointCause;
		}
	}
}
