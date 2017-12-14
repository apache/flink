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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Provides root directories and the subtask-specific path to build directories for file-based local recovery. Calls
 * to {@link #rootDirectory(long)} rotate over all available root directories.
 */
public class LocalRecoveryDirectoryProvider implements Serializable {

	private static final long serialVersionUID = 1L;

	/** All available root directories that this can potentially deliver. */
	@Nonnull
	private final File[] rootDirectories;

	/** JobID of the owning job. */
	@Nonnull
	private final JobID jobID;

	/** AllocationID of the owning slot. */
	@Nonnull
	private final AllocationID allocationID;

	/** JobVertexID of the owning task. */
	@Nonnull
	private final JobVertexID jobVertexID;

	/** Index of the owning subtask. */
	@Nonnegative
	private final int subtaskIndex;

	public LocalRecoveryDirectoryProvider(
		File rootDir,
		@Nonnull JobID jobID,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex) {
		this(new File[]{rootDir}, jobID, allocationID, jobVertexID, subtaskIndex);
	}

	public LocalRecoveryDirectoryProvider(
		@Nonnull File[] rootDirectories,
		@Nonnull JobID jobID,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex) {

		Preconditions.checkArgument(rootDirectories.length > 0);
		this.rootDirectories = rootDirectories;
		this.jobID = jobID;
		this.allocationID = allocationID;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;

		for (File directory : rootDirectories) {
			Preconditions.checkNotNull(directory);
			if (!directory.isDirectory()) {
				throw new IllegalStateException("Local recovery root directory " + directory + " does not exist!");
			}
		}
	}

	/**
	 * Returns the local state root directory local state for the given checkpoint id w.r.t. our rotation over all
	 * available root dirs.
	 */
	public File rootDirectory(long checkpointId) {
		return selectRootDirectory((((int) checkpointId) & Integer.MAX_VALUE) % rootDirectories.length);
	}

	/**
	 * Returns the local state base directory for the owning job and given checkpoint id w.r.t. our rotation over all
	 * available root dirs. This directory is contained in the directory returned by {@link #rootDirectory(long)} for
	 * the same checkpoint id.
	 */
	public File jobAndAllocationBaseDirectory(long checkpointId) {
		return new File(rootDirectory(checkpointId), createJobAndAllocationSubDirString());
	}

	/**
	 * Returns the local state checkpoint base directory for the given checkpoint id w.r.t. our rotation over all
	 * available root dirs. This directory is contained in the directory returned by {@link #rootDirectory(long)} for
	 * the same checkpoint id.
	 */
	public File checkpointBaseDirectory(long checkpointId) {
		return new File(jobAndAllocationBaseDirectory(checkpointId), createCheckpointSubDirString(checkpointId));
	}

	/**
	 * Returns the local state directory for the specific operator subtask and the given checkpoint id w.r.t. our
	 * rotation over all available root dirs. This directory is contained in the directory returned by
	 * {@link #checkpointBaseDirectory(long)} for the same checkpoint id.
	 */
	public File subtaskSpecificCheckpointDirectory(long checkpointId) {
		return new File(checkpointBaseDirectory(checkpointId), createSpecificCheckpointSubDirString());
	}

	/**
	 * Returns a specific root dir for the given index < {@link #rootDirectoryCount()}. The index must be between
	 * 0 (incl.) and {@link #rootDirectoryCount()} (excl.).
	 */
	public File selectRootDirectory(int idx) {
		return rootDirectories[idx];
	}

	/**
	 * Returns a specific job-and-allocation directory, which is a root dir plus the sub-dir for job-and-allocation.
	 * The index must be between 0 (incl.) and {@link #rootDirectoryCount()} (excl.).
	 */
	public File selectJobAndAllocationBaseDirectory(int idx) {
		return new File(selectRootDirectory(idx), createJobAndAllocationSubDirString());
	}

	/**
	 * Returns the total number of root directories.
	 */
	public int rootDirectoryCount() {
		return rootDirectories.length;
	}

	@Override
	public String toString() {
		return "LocalRecoveryDirectoryProvider{" +
			"rootDirectories=" + Arrays.toString(rootDirectories) +
			", jobID=" + jobID +
			", allocationID=" + allocationID +
			", jobVertexID=" + jobVertexID +
			", subtaskIndex=" + subtaskIndex +
			'}';
	}

	@VisibleForTesting
	String createJobAndAllocationSubDirString() {
		return "jid_" + jobID + "_aid_" + allocationID;
	}

	@VisibleForTesting
	String createCheckpointSubDirString(long checkpointId) {
		return "chk_" + checkpointId;
	}

	@VisibleForTesting
	String createSpecificCheckpointSubDirString() {
		return "vtx_" + jobVertexID + Path.SEPARATOR + subtaskIndex;
	}
}
