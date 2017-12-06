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

import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides root directories and the subtask-specific path to build directories for file-based local recovery. Calls
 * to {@link #nextRootDirectory()} rotate over all available root directories. this class is thread-safe.
 */
public class LocalRecoveryDirectoryProvider implements Serializable {

	private static final long serialVersionUID = 1L;

	/** All available root directories that this can potentially deliver. */
	private final File[] rootDirectories;

	/** A subtask-specific string that describes a path to it's local state from a given checkpoint root. */
	private final String subtaskSpecificPath;

	/** Current index for rotational selection of delivered root dirs. */
	private AtomicInteger idx;

	public LocalRecoveryDirectoryProvider(File rootDir, String subtaskSpecificPath) {
		this(new File[]{rootDir}, subtaskSpecificPath);
	}

	public LocalRecoveryDirectoryProvider(File[] rootDirectories, String subtaskSpecificPath) {

		this.rootDirectories = Preconditions.checkNotNull(rootDirectories);
		this.subtaskSpecificPath = Preconditions.checkNotNull(subtaskSpecificPath);
		Preconditions.checkArgument(rootDirectories.length > 0);

		for (File directory : rootDirectories) {
			Preconditions.checkNotNull(directory);
			if (!directory.isDirectory()) {
				throw new IllegalStateException("Local recovery root directory " + directory + " does not exist!");
			}
		}

		this.idx = new AtomicInteger(-1);
	}

	/**
	 * Returns the next root directory w.r.t. our rotation over all available root dirs.
	 */
	public File nextRootDirectory() {
		return rootDirectories[(idx.incrementAndGet() & Integer.MAX_VALUE) % rootDirectories.length];
	}

	/**
	 * Returns a specific root dir for the given index < {@link #rootDirectoryCount()}.
	 */
	public File selectRootDirectory(int idx) {
		return rootDirectories[idx];
	}

	/**
	 * Returns the total number of root directories.
	 */
	public int rootDirectoryCount() {
		return rootDirectories.length;
	}

	/**
	 * Returns a string that describes a subtask-path. We typically append this somewhere under the root dir or a
	 * checkpoint dir inside the root dir.
	 */
	public String getSubtaskSpecificPath() {
		return subtaskSpecificPath;
	}

	/**
	 * Helper method that returns a file name based on {@link #getSubtaskSpecificPath()} and the given checkpointId.
	 */
	public String specificFileForCheckpointId(long checkpointId) {
		return "chk-" + checkpointId + File.separator +
			getSubtaskSpecificPath() + File.separator +
			UUID.randomUUID();
	}

	@Override
	public String toString() {
		return "LocalRecoveryDirectoryProvider{" +
			"rootDirectories=" + Arrays.toString(rootDirectories) +
			", subtaskSpecificPath='" + subtaskSpecificPath + '\'' +
			", idx=" + idx +
			'}';
	}
}
