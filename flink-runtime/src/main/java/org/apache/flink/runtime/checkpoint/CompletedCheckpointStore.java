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

package org.apache.flink.runtime.checkpoint;

import java.util.List;

/**
 * A bounded LIFO-queue of {@link CompletedCheckpoint} instances.
 */
public interface CompletedCheckpointStore {

	/**
	 * Recover available {@link CompletedCheckpoint} instances.
	 *
	 * <p>After a call to this method, {@link #getLatestCheckpoint()} returns the latest
	 * available checkpoint.
	 */
	void recover() throws Exception;

	/**
	 * Adds a {@link CompletedCheckpoint} instance to the list of completed checkpoints.
	 *
	 * <p>Only a bounded number of checkpoints is kept. When exceeding the maximum number of
	 * retained checkpoints, the oldest one will be discarded via {@link
	 * CompletedCheckpoint#discard(ClassLoader)}.
	 */
	void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception;

	/**
	 * Returns the latest {@link CompletedCheckpoint} instance or <code>null</code> if none was
	 * added.
	 */
	CompletedCheckpoint getLatestCheckpoint() throws Exception;

	/**
	 * Discards all added {@link CompletedCheckpoint} instances via {@link
	 * CompletedCheckpoint#discard(ClassLoader)}.
	 */
	void discardAllCheckpoints() throws Exception;

	/**
	 * Returns all {@link CompletedCheckpoint} instances.
	 *
	 * <p>Returns an empty list if no checkpoint has been added yet.
	 */
	List<CompletedCheckpoint> getAllCheckpoints() throws Exception;

	/**
	 * Returns the current number of retained checkpoints.
	 */
	int getNumberOfRetainedCheckpoints();

}
