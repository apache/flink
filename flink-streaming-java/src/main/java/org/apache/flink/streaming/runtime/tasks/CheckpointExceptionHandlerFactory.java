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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.util.Preconditions;

/**
 * This factory produces {@link CheckpointExceptionHandler} instances that handle exceptions during checkpointing in a
 * {@link StreamTask}.
 */
public class CheckpointExceptionHandlerFactory {

	/**
	 * Returns a {@link CheckpointExceptionHandler} that either causes a task to fail completely or to just declines
	 * checkpoint on exception, depending on the parameter flag.
	 */
	public CheckpointExceptionHandler createCheckpointExceptionHandler(
		boolean failTaskOnCheckpointException,
		Environment environment) {

		if (failTaskOnCheckpointException) {
			return new FailingCheckpointExceptionHandler();
		} else {
			return new DecliningCheckpointExceptionHandler(environment);
		}
	}

	/**
	 * This handler makes the task fail by rethrowing a reported exception.
	 */
	static final class FailingCheckpointExceptionHandler implements CheckpointExceptionHandler {

		@Override
		public void tryHandleCheckpointException(
			CheckpointMetaData checkpointMetaData,
			Exception exception) throws Exception {

			throw exception;
		}
	}

	/**
	 * This handler makes the task decline the checkpoint as reaction to the reported exception. The task is not failed.
	 */
	static final class DecliningCheckpointExceptionHandler implements CheckpointExceptionHandler {

		final Environment environment;

		DecliningCheckpointExceptionHandler(Environment environment) {
			this.environment = Preconditions.checkNotNull(environment);
		}

		@Override
		public void tryHandleCheckpointException(
			CheckpointMetaData checkpointMetaData,
			Exception exception) throws Exception {

			environment.declineCheckpoint(checkpointMetaData.getCheckpointId(), exception);
		}
	}
}
