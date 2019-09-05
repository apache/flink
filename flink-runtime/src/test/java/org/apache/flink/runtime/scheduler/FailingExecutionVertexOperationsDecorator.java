/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Allows to fail ExecutionVertex operations for testing.
 */
public class FailingExecutionVertexOperationsDecorator implements ExecutionVertexOperations {

	private final ExecutionVertexOperations delegate;

	private boolean failDeploy;

	private boolean failCancel;

	public FailingExecutionVertexOperationsDecorator(final ExecutionVertexOperations delegate) {
		this.delegate = checkNotNull(delegate);
	}

	@Override
	public void deploy(final ExecutionVertex executionVertex) throws JobException {
		if (failDeploy) {
			throw new RuntimeException("Expected");
		} else {
			delegate.deploy(executionVertex);
		}
	}

	@Override
	public CompletableFuture<?> cancel(final ExecutionVertex executionVertex) {
		if (failCancel) {
			return FutureUtils.completedExceptionally(new RuntimeException("Expected"));
		} else {
			return delegate.cancel(executionVertex);
		}
	}

	public void enableFailDeploy() {
		failDeploy = true;
	}

	public void disableFailDeploy() {
		failDeploy = false;
	}

	public void enableFailCancel() {
		failCancel = true;
	}

	public void disableFailCancel() {
		failCancel = false;
	}
}
