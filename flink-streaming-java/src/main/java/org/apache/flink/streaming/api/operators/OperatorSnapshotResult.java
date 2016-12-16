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

import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

/**
 * Result of {@link AbstractStreamOperator#snapshotState}.
 */
public class OperatorSnapshotResult {

	private RunnableFuture<KeyGroupsStateHandle> keyedStateManagedFuture;
	private RunnableFuture<KeyGroupsStateHandle> keyedStateRawFuture;
	private RunnableFuture<OperatorStateHandle> operatorStateManagedFuture;
	private RunnableFuture<OperatorStateHandle> operatorStateRawFuture;

	public OperatorSnapshotResult() {
		this(null, null, null, null);
	}

	public OperatorSnapshotResult(
			RunnableFuture<KeyGroupsStateHandle> keyedStateManagedFuture,
			RunnableFuture<KeyGroupsStateHandle> keyedStateRawFuture,
			RunnableFuture<OperatorStateHandle> operatorStateManagedFuture,
			RunnableFuture<OperatorStateHandle> operatorStateRawFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
		this.keyedStateRawFuture = keyedStateRawFuture;
		this.operatorStateManagedFuture = operatorStateManagedFuture;
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public RunnableFuture<KeyGroupsStateHandle> getKeyedStateManagedFuture() {
		return keyedStateManagedFuture;
	}

	public void setKeyedStateManagedFuture(RunnableFuture<KeyGroupsStateHandle> keyedStateManagedFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
	}

	public RunnableFuture<KeyGroupsStateHandle> getKeyedStateRawFuture() {
		return keyedStateRawFuture;
	}

	public void setKeyedStateRawFuture(RunnableFuture<KeyGroupsStateHandle> keyedStateRawFuture) {
		this.keyedStateRawFuture = keyedStateRawFuture;
	}

	public RunnableFuture<OperatorStateHandle> getOperatorStateManagedFuture() {
		return operatorStateManagedFuture;
	}

	public void setOperatorStateManagedFuture(RunnableFuture<OperatorStateHandle> operatorStateManagedFuture) {
		this.operatorStateManagedFuture = operatorStateManagedFuture;
	}

	public RunnableFuture<OperatorStateHandle> getOperatorStateRawFuture() {
		return operatorStateRawFuture;
	}

	public void setOperatorStateRawFuture(RunnableFuture<OperatorStateHandle> operatorStateRawFuture) {
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public void cancel() {
		cancelIfNotNull(getKeyedStateManagedFuture());
		cancelIfNotNull(getOperatorStateManagedFuture());
		cancelIfNotNull(getKeyedStateRawFuture());
		cancelIfNotNull(getOperatorStateRawFuture());
	}

	private static void cancelIfNotNull(Future<?> future) {
		if (null != future) {
			future.cancel(true);
		}
	}
}
