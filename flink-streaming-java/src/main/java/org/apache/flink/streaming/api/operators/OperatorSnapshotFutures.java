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
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.ExceptionUtils;

import java.util.concurrent.RunnableFuture;

/**
 * Result of {@link StreamOperator#snapshotState}.
 */
public class OperatorSnapshotFutures {

	private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture;
	private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture;
	private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture;
	private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture;

	public OperatorSnapshotFutures() {
		this(null, null, null, null);
	}

	public OperatorSnapshotFutures(
			RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture,
			RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture,
			RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture,
			RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
		this.keyedStateRawFuture = keyedStateRawFuture;
		this.operatorStateManagedFuture = operatorStateManagedFuture;
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateManagedFuture() {
		return keyedStateManagedFuture;
	}

	public void setKeyedStateManagedFuture(RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
	}

	public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateRawFuture() {
		return keyedStateRawFuture;
	}

	public void setKeyedStateRawFuture(RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture) {
		this.keyedStateRawFuture = keyedStateRawFuture;
	}

	public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateManagedFuture() {
		return operatorStateManagedFuture;
	}

	public void setOperatorStateManagedFuture(RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture) {
		this.operatorStateManagedFuture = operatorStateManagedFuture;
	}

	public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateRawFuture() {
		return operatorStateRawFuture;
	}

	public void setOperatorStateRawFuture(RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture) {
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public void cancel() throws Exception {
		Exception exception = null;

		try {
			StateUtil.discardStateFuture(getKeyedStateManagedFuture());
		} catch (Exception e) {
			exception = new Exception("Could not properly cancel managed keyed state future.", e);
		}

		try {
			StateUtil.discardStateFuture(getOperatorStateManagedFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel managed operator state future.", e),
				exception);
		}

		try {
			StateUtil.discardStateFuture(getKeyedStateRawFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel raw keyed state future.", e),
				exception);
		}

		try {
			StateUtil.discardStateFuture(getOperatorStateRawFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel raw operator state future.", e),
				exception);
		}

		if (exception != null) {
			throw exception;
		}
	}
}
