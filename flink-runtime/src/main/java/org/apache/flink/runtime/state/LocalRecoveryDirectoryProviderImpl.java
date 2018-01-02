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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Provides root directories and the subtask-specific path to build directories for file-based local recovery. Calls
 * to {@link #rootDirectory(long)} rotate over all available root directories.
 */
public class LocalRecoveryDirectoryProviderImpl implements LocalRecoveryDirectoryProvider {

	/** Serial version. */
	private static final long serialVersionUID = 1L;

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(LocalRecoveryDirectoryProviderImpl.class);

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

		for (File rootDir : rootDirectories) {
			Preconditions.checkNotNull(rootDir);
			if (!rootDir.isDirectory()) {
				throw new IllegalStateException("Local recovery root directory " + rootDir + " does not exist!");
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
		return new File(allocationBaseDirectory(checkpointId), jobCheckpointSubDirString(checkpointId));
	}

	@Override
	public File subtaskSpecificCheckpointDirectory(long checkpointId) {
		return new File(jobAndCheckpointBaseDirectory(checkpointId), subtaskSubDirString());
	}

	@Override
	public File selectRootDirectory(int idx) {
		return rootDirectories[idx];
	}

	@Override
	public File selectAllocationBaseDirectory(int idx) {
		return new File(selectRootDirectory(idx), allocationSubDirString());
	}

	@Override
	public int rootDirectoryCount() {
		return rootDirectories.length;
	}

	@Override
	public void cleanupAllocationBaseDirectories() throws IOException {

		String jobSubDirString = jobSubDirString();

		for (int i = 0; i < rootDirectoryCount(); ++i) {

			// iterate all allocation base directories ...
			File allocationBaseDir = selectAllocationBaseDirectory(i);

			if (allocationBaseDir.isDirectory()) {

				// ... detect all files that are not the subdir for the current job, running in this slot ...
				File[] cleanupFiles = allocationBaseDir.listFiles((dir, name) -> !jobSubDirString.equals(name));
				if (cleanupFiles != null) {

					// ... and delete them.
					IOException collectedExceptions = null;
					for (File file : cleanupFiles) {
						try {
							FileUtils.deleteFileOrDirectory(file);
						} catch (IOException e) {
							collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
						}
					}

					if (collectedExceptions != null) {
						throw collectedExceptions;
					}
				}
			}
		}
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
	String jobSubDirString() {
		return "jid_" + jobID;
	}

	@VisibleForTesting
	String jobCheckpointSubDirString(long checkpointId) {
		return  jobSubDirString() + Path.SEPARATOR + "chk_" + checkpointId;
	}

	@VisibleForTesting
	String subtaskSubDirString() {
		return "vtx_" + jobVertexID + Path.SEPARATOR + subtaskIndex;
	}
}
