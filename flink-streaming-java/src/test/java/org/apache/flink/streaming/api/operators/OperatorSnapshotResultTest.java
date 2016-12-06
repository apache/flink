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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OperatorSnapshotResultTest {

	@Test
	public void testCancel() {
		OperatorSnapshotResult operatorSnapshotResult = new OperatorSnapshotResult();

		operatorSnapshotResult.cancel();

		RunnableFuture<KeyGroupsStateHandle> keyedStateManagedFuture = new TestRunnableFuture<>();
		RunnableFuture<KeyGroupsStateHandle> keyedStateRawFuture = new TestRunnableFuture<>();
		RunnableFuture<OperatorStateHandle> operatorStateManagedFuture = new TestRunnableFuture<>();
		RunnableFuture<OperatorStateHandle> operatorStateRawFuture = new TestRunnableFuture<>();

		operatorSnapshotResult = new OperatorSnapshotResult(
				keyedStateManagedFuture,
				keyedStateRawFuture,
				operatorStateManagedFuture,
				operatorStateRawFuture);

		operatorSnapshotResult.cancel();

		Assert.assertTrue(keyedStateManagedFuture.isCancelled());
		Assert.assertTrue(keyedStateRawFuture.isCancelled());
		Assert.assertTrue(operatorStateManagedFuture.isCancelled());
		Assert.assertTrue(operatorStateRawFuture.isCancelled());

	}

	static final class TestRunnableFuture<T> implements RunnableFuture<T> {

		private boolean canceled;

		public TestRunnableFuture() {
			this.canceled = false;
		}

		@Override
		public void run() {

		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return canceled = true;
		}

		@Override
		public boolean isCancelled() {
			return canceled;
		}

		@Override
		public boolean isDone() {
			return false;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			return null;
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return null;
		}
	}


}