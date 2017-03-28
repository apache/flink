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
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.ExceptionUtils;

import java.util.concurrent.RunnableFuture;

/**
 * Result of {@link StreamOperator#snapshotState}.
 */
public class OperatorSnapshotResult {

	private RunnableFuture<KeyedStateHandle> keyedStateManagedFuture;
	private RunnableFuture<KeyedStateHandle> keyedStateRawFuture;
	private RunnableFuture<OperatorStateHandle> operatorStateManagedFuture;
	private RunnableFuture<OperatorStateHandle> operatorStateRawFuture;

	public OperatorSnapshotResult() {
		this(null, null, null, null);
	}

	public OperatorSnapshotResult(
			RunnableFuture<KeyedStateHandle> keyedStateManagedFuture,
			RunnableFuture<KeyedStateHandle> keyedStateRawFuture,
			RunnableFuture<OperatorStateHandle> operatorStateManagedFuture,
			RunnableFuture<OperatorStateHandle> operatorStateRawFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
		this.keyedStateRawFuture = keyedStateRawFuture;
		this.operatorStateManagedFuture = operatorStateManagedFuture;
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	public RunnableFuture<KeyedStateHandle> getKeyedStateManagedFuture() {
		return keyedStateManagedFuture;
	}

	public void setKeyedStateManagedFuture(RunnableFuture<KeyedStateHandle> keyedStateManagedFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
	}

	public RunnableFuture<KeyedStateHandle> getKeyedStateRawFuture() {
		return keyedStateRawFuture;
	}

	public void setKeyedStateRawFuture(RunnableFuture<KeyedStateHandle> keyedStateRawFuture) {
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

	public void cancel() throws Exception {
		Exception exception = null;

		try {
			StateUtil.discardStateFuture(getKeyedStateManagedFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel managed keyed state future.", e),
				exception);
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

	public boolean hasKeyedState() {
		return keyedStateManagedFuture != null || keyedStateRawFuture != null;
	}

	public boolean hasOperatorState() {
		return operatorStateManagedFuture != null || operatorStateRawFuture != null;
	}

	public boolean hasState() {
		return hasKeyedState() || hasOperatorState();
	}
}
