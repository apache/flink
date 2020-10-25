/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import java.util.concurrent.CompletableFuture;

/**
 * Simple adapter for {@link TaskExecutor} to adapt to {@link TaskManagerRunner.TaskExecutorService}.
 */
public class TaskExecutorToServiceAdapter implements TaskManagerRunner.TaskExecutorService {

	private final TaskExecutor taskExecutor;

	private TaskExecutorToServiceAdapter(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	@Override
	public void start() {
		taskExecutor.start();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return taskExecutor.getTerminationFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return taskExecutor.closeAsync();
	}

	public static TaskExecutorToServiceAdapter createFor(TaskExecutor taskExecutor) {
		return new TaskExecutorToServiceAdapter(taskExecutor);
	}
}
