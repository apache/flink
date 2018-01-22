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

package org.apache.flink.runtime.state;

import java.io.File;
import java.io.Serializable;

/**
 * Provides root directories and the subtask-specific path to build directories for file-based local recovery. Calls
 * to {@link #rootDirectory(long)} rotate over all available root directories.
 */
public interface LocalRecoveryDirectoryProvider extends Serializable {
	/**
	 * Returns the local state root directory local state for the given checkpoint id w.r.t. our rotation over all
	 * available root dirs.
	 */
	File rootDirectory(long checkpointId);

	/**
	 * Returns the local state base directory for the owning job and given checkpoint id w.r.t. our rotation over all
	 * available root dirs. This directory is contained in the directory returned by {@link #rootDirectory(long)} for
	 * the same checkpoint id.
	 */
	File allocationBaseDirectory(long checkpointId);

	/**
	 * Returns the local state checkpoint base directory for the given checkpoint id w.r.t. our rotation over all
	 * available root dirs. This directory is contained in the directory returned by {@link #rootDirectory(long)} for
	 * the same checkpoint id.
	 */
	File jobAndCheckpointBaseDirectory(long checkpointId);

	/**
	 * Returns the local state directory for the specific operator subtask and the given checkpoint id w.r.t. our
	 * rotation over all available root dirs. This directory is contained in the directory returned by
	 * {@link #jobAndCheckpointBaseDirectory(long)} for the same checkpoint id.
	 */
	File subtaskSpecificCheckpointDirectory(long checkpointId);

	/**
	 * Returns a specific root dir for the given index < {@link #rootDirectoryCount()}. The index must be between
	 * 0 (incl.) and {@link #rootDirectoryCount()} (excl.).
	 */
	File selectRootDirectory(int idx);

	/**
	 * Returns a specific job-and-allocation directory, which is a root dir plus the sub-dir for job-and-allocation.
	 * The index must be between 0 (incl.) and {@link #rootDirectoryCount()} (excl.).
	 */
	File selectAllocationBaseDirectory(int idx);

	/**
	 * Returns the total number of root directories.
	 */
	int rootDirectoryCount();
}
