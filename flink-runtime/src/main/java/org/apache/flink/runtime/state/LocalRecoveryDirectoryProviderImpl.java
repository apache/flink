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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.util.Arrays;

/**
 * Implementation of {@link LocalRecoveryDirectoryProvider}.
 */
public class LocalRecoveryDirectoryProviderImpl implements LocalRecoveryDirectoryProvider {

	/** Serial version. */
	private static final long serialVersionUID = 1L;

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(LocalRecoveryDirectoryProviderImpl.class);

	/** All available root directories that this can potentially deliver. */
	@Nonnull
	private final File[] allocationBaseDirs;

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
		File allocationBaseDir,
		@Nonnull JobID jobID,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex) {
		this(new File[]{allocationBaseDir}, jobID, allocationID, jobVertexID, subtaskIndex);
	}

	public LocalRecoveryDirectoryProviderImpl(
		@Nonnull File[] allocationBaseDirs,
		@Nonnull JobID jobID,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex) {

		Preconditions.checkArgument(allocationBaseDirs.length > 0);
		this.allocationBaseDirs = allocationBaseDirs;
		this.jobID = jobID;
		this.allocationID = allocationID;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;

		for (File allocationBaseDir : allocationBaseDirs) {
			Preconditions.checkNotNull(allocationBaseDir);
			if (!allocationBaseDir.isDirectory()) {
				throw new IllegalStateException("Local recovery allocation base directory " + allocationBaseDir + " does not exist!");
			}
		}
	}

	@Override
	public File allocationBaseDirectory(long checkpointId) {
		return selectAllocationBaseDirectory((((int) checkpointId) & Integer.MAX_VALUE) % allocationBaseDirs.length);
	}

	@Override
	public File subtaskBaseDirectory(long checkpointId) {
		return new File(allocationBaseDirectory(checkpointId), subtaskDirString());
	}

	@Override
	public File subtaskSpecificCheckpointDirectory(long checkpointId) {
		return new File(subtaskBaseDirectory(checkpointId), checkpointDirString(checkpointId));
	}

	@Override
	public File selectAllocationBaseDirectory(int idx) {
		return allocationBaseDirs[idx];
	}

	@Override
	public File selectSubtaskBaseDirectory(int idx) {
		return new File(selectAllocationBaseDirectory(idx), subtaskDirString());
	}

	@Override
	public int allocationBaseDirsCount() {
		return allocationBaseDirs.length;
	}

	@Override
	public String toString() {
		return "LocalRecoveryDirectoryProvider{" +
			"rootDirectories=" + Arrays.toString(allocationBaseDirs) +
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
	String subtaskDirString() {
		return "jid_" + jobID + Path.SEPARATOR + "vtx_" + jobVertexID + "_sti_" + subtaskIndex;
	}

	@VisibleForTesting
	String checkpointDirString(long checkpointId) {
		return "chk_" + checkpointId;
	}
}
