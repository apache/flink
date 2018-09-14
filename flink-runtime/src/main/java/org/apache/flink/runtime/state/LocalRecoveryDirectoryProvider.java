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
 * Provides directories for local recovery. It offers access to the allocation base directories (i.e. the root
 * directories for all local state that is created under the same allocation id) and the subtask-specific paths, which
 * contain the local state for one subtask. Access by checkpoint id rotates over all root directory indexes, in case
 * that there is more than one. Selection methods are provided to pick the directory under a certain index. Directory
 * structures are of the following shape:
 *
 * <p><blockquote><pre>
 * |-----allocationBaseDirectory------|
 * |-----subtaskBaseDirectory--------------------------------------|
 * |-----subtaskSpecificCheckpointDirectory------------------------------|
 *
 * ../local_state_root_1/allocation_id/job_id/vertex_id_subtask_idx/chk_1/(state)
 * ../local_state_root_2/allocation_id/job_id/vertex_id_subtask_idx/chk_2/(state)
 *
 * (...)
 * </pre></blockquote><p>
 */
public interface LocalRecoveryDirectoryProvider extends Serializable {

	/**
	 * Returns the local state allocation base directory for given checkpoint id w.r.t. our rotation
	 * over all available allocation base directories.
	 */
	File allocationBaseDirectory(long checkpointId);

	/**
	 * Returns the local state directory for the owning subtask the given checkpoint id w.r.t. our rotation over all
	 * available available allocation base directories. This directory is contained in the directory returned by
	 * {@link #allocationBaseDirectory(long)} for the same checkpoint id.
	 */
	File subtaskBaseDirectory(long checkpointId);

	/**
	 * Returns the local state directory for the specific operator subtask and the given checkpoint id w.r.t. our
	 * rotation over all available root dirs. This directory is contained in the directory returned by
	 * {@link #subtaskBaseDirectory(long)} for the same checkpoint id.
	 */
	File subtaskSpecificCheckpointDirectory(long checkpointId);

	/**
	 * Returns a specific allocation base directory. The index must be between 0 (incl.) and
	 * {@link #allocationBaseDirsCount()} (excl.).
	 */
	File selectAllocationBaseDirectory(int idx);

	/**
	 * Returns a specific subtask base directory. The index must be between 0 (incl.) and
	 * {@link #allocationBaseDirsCount()} (excl.). This directory is direct a child of
	 * {@link #selectSubtaskBaseDirectory(int)} given the same index.
	 */
	File selectSubtaskBaseDirectory(int idx);

	/**
	 * Returns the total number of allocation base directories.
	 */
	int allocationBaseDirsCount();
}
