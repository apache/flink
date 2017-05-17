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

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.RunnableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link OperatorSnapshotResult}.
 */
public class OperatorSnapshotResultTest extends TestLogger {

	/**
	 * Tests that all runnable futures in an OperatorSnapshotResult are properly cancelled and if
	 * the StreamStateHandle result is retrievable that the state handle are discarded.
	 */
	@Test
	public void testCancelAndCleanup() throws Exception {
		OperatorSnapshotResult operatorSnapshotResult = new OperatorSnapshotResult();

		operatorSnapshotResult.cancel();

		KeyedStateHandle keyedManagedStateHandle = mock(KeyedStateHandle.class);
		RunnableFuture<KeyedStateHandle> keyedStateManagedFuture = mock(RunnableFuture.class);
		when(keyedStateManagedFuture.get()).thenReturn(keyedManagedStateHandle);

		KeyedStateHandle keyedRawStateHandle = mock(KeyedStateHandle.class);
		RunnableFuture<KeyedStateHandle> keyedStateRawFuture = mock(RunnableFuture.class);
		when(keyedStateRawFuture.get()).thenReturn(keyedRawStateHandle);

		OperatorStateHandle operatorManagedStateHandle = mock(OperatorStateHandle.class);
		RunnableFuture<OperatorStateHandle> operatorStateManagedFuture = mock(RunnableFuture.class);
		when(operatorStateManagedFuture.get()).thenReturn(operatorManagedStateHandle);

		OperatorStateHandle operatorRawStateHandle = mock(OperatorStateHandle.class);
		RunnableFuture<OperatorStateHandle> operatorStateRawFuture = mock(RunnableFuture.class);
		when(operatorStateRawFuture.get()).thenReturn(operatorRawStateHandle);

		operatorSnapshotResult = new OperatorSnapshotResult(
			keyedStateManagedFuture,
			keyedStateRawFuture,
			operatorStateManagedFuture,
			operatorStateRawFuture);

		operatorSnapshotResult.cancel();

		verify(keyedStateManagedFuture).cancel(true);
		verify(keyedStateRawFuture).cancel(true);
		verify(operatorStateManagedFuture).cancel(true);
		verify(operatorStateRawFuture).cancel(true);

		verify(keyedManagedStateHandle).discardState();
		verify(keyedRawStateHandle).discardState();
		verify(operatorManagedStateHandle).discardState();
		verify(operatorRawStateHandle).discardState();
	}
}
