/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.core.io.Versioned;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TaskState;

import java.util.Collection;

/**
 * Savepoints are manually-triggered snapshots from which a program can be
 * resumed on submission.
 *
 * <p>In order to allow changes to the savepoint format between Flink versions,
 * we allow different savepoint implementations (see subclasses of this
 * interface).
 *
 * <p>Savepoints are serialized via a {@link SavepointSerializer} and stored
 * via a {@link SavepointStore}.
 */
public interface Savepoint extends Versioned {

	/**
	 * Returns the checkpoint ID of the savepoint.
	 *
	 * <p>Savepoints use the same mechanisms as regular checkpoints and are
	 * identified by a unique ID. This ID is used to reset the {@link CheckpointIDCounter}
	 * when restoring from a savepoint.
	 *
	 * @return Checkpoint ID of the savepoint
	 */
	long getCheckpointId();

	/**
	 * Returns the snapshotted task states.
	 *
	 * <p>These are used to restore the snapshot state.
	 *
	 * @return Snapshotted task states
	 */
	Collection<TaskState> getTaskStates();

	/**
	 * Disposes the savepoint.
	 */
	void dispose() throws Exception;

}
