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
import java.util.Arrays;

/**
 * Provides root directories and the subtask-specific path to build directories for file-based local recovery. Calls
 * to {@link #rootDirectory(long)} rotate over all available root directories.
 */
public class LocalRecoveryDirectoryProviderImpl implements LocalRecoveryDirectoryProvider {

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

	public LocalRecoveryDirectoryProviderImpl(
		File rootDir,
		@Nonnull JobID jobID,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex) {
		this(new File[]{rootDir}, jobID, allocationID, jobVertexID, subtaskIndex);
	}

	public LocalRecoveryDirectoryProviderImpl(
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

	@Override
	public File rootDirectory(long checkpointId) {
		return selectRootDirectory((((int) checkpointId) & Integer.MAX_VALUE) % rootDirectories.length);
	}

	@Override
	public File allocationBaseDirectory(long checkpointId) {
		return new File(rootDirectory(checkpointId), allocationSubDirString());
	}

	@Override
	public File jobAndCheckpointBaseDirectory(long checkpointId) {
		return new File(allocationBaseDirectory(checkpointId), createJobCheckpointSubDirString(checkpointId));
	}

	@Override
	public File subtaskSpecificCheckpointDirectory(long checkpointId) {
		return new File(jobAndCheckpointBaseDirectory(checkpointId), createSubtaskSubDirString());
	}

	@Override
	public File selectRootDirectory(int idx) {
		return rootDirectories[idx];
	}

	@Override
	public File selectJobAndAllocationBaseDirectory(int idx) {
		return new File(selectRootDirectory(idx), allocationSubDirString());
	}

	@Override
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
	String allocationSubDirString() {
		return "aid_" + allocationID;
	}

	@VisibleForTesting
	String createJobCheckpointSubDirString(long checkpointId) {
		return "jid_" + jobID + Path.SEPARATOR + "chk_" + checkpointId;
	}

	@VisibleForTesting
	String createSubtaskSubDirString() {
		return "vtx_" + jobVertexID + Path.SEPARATOR + subtaskIndex;
	}
}
