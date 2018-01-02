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
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.RunnableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link OperatorSnapshotFutures}.
 */
public class OperatorSnapshotFuturesTest extends TestLogger {

	/**
	 * Tests that all runnable futures in an OperatorSnapshotResult are properly cancelled and if
	 * the StreamStateHandle result is retrievable that the state handle are discarded.
	 */
	@Test
	public void testCancelAndCleanup() throws Exception {
		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures();

		operatorSnapshotResult.cancel();

		KeyedStateHandle keyedManagedStateHandle = mock(KeyedStateHandle.class);
		RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture = mock(RunnableFuture.class);
		SnapshotResult<KeyedStateHandle> keyedStateManagedResult =
			new SnapshotResult<>(keyedManagedStateHandle, null);
		when(keyedStateManagedFuture.get()).thenReturn(keyedStateManagedResult);

		KeyedStateHandle keyedRawStateHandle = mock(KeyedStateHandle.class);
		RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture = mock(RunnableFuture.class);
		SnapshotResult<KeyedStateHandle> keyedStateRawResult =
			new SnapshotResult<>(keyedRawStateHandle, null);
		when(keyedStateRawFuture.get()).thenReturn(keyedStateRawResult);

		OperatorStateHandle operatorManagedStateHandle = mock(OperatorStreamStateHandle.class);
		RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture = mock(RunnableFuture.class);
		SnapshotResult<OperatorStateHandle> operatorStateManagedResult =
			new SnapshotResult<>(operatorManagedStateHandle, null);
		when(operatorStateManagedFuture.get()).thenReturn(operatorStateManagedResult);

		OperatorStateHandle operatorRawStateHandle = mock(OperatorStreamStateHandle.class);
		RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture = mock(RunnableFuture.class);
		SnapshotResult<OperatorStateHandle> operatorStateRawResult =
			new SnapshotResult<>(operatorRawStateHandle, null);
		when(operatorStateRawFuture.get()).thenReturn(operatorStateRawResult);

		operatorSnapshotResult = new OperatorSnapshotFutures(
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
