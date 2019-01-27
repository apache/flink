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

package org.apache.flink.runtime.jobgraph;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Savepoint restore settings.
 */
public class SavepointRestoreSettings implements Serializable {

	private static final long serialVersionUID = 87377506900849777L;

	/** No restore should happen. */
	private final static SavepointRestoreSettings NONE = new SavepointRestoreSettings(null, false, false);

	/** By default, be strict when restoring from a savepoint.  */
	private final static boolean DEFAULT_ALLOW_NON_RESTORED_STATE = false;

	/** Savepoint restore path. */
	private final String restorePath;

	/**
	 * Flag indicating whether non restored state is allowed if the savepoint
	 * contains state for an operator that is not part of the job.
	 */
	private final boolean allowNonRestoredState;

	/**
	 * Flag indicating whether to resume from latest completed checkpoint.
	 * If restore from savepoint or specific checkpoint, this value is false;
	 * If resume from given checkpoint directory at job level, this value is true.
	 */
	private final boolean resumeFromLatestCheckpoint;

	/**
	 * Creates the restore settings.
	 *
	 * @param restorePath Savepoint restore path.
	 * @param allowNonRestoredState Ignore unmapped state.
	 * @param resumeFromLatestCheckpoint Resume from latest completed checkpoint automatically.
	 */
	private SavepointRestoreSettings(String restorePath, boolean allowNonRestoredState, boolean resumeFromLatestCheckpoint) {
		this.restorePath = restorePath;
		this.allowNonRestoredState = allowNonRestoredState;
		this.resumeFromLatestCheckpoint = resumeFromLatestCheckpoint;
	}

	/**
	 * Returns whether to restore from savepoint.
	 * @return <code>true</code> if should restore from savepoint.
	 */
	public boolean restoreSavepoint() {
		return restorePath != null;
	}

	/**
	 * Returns the path to the savepoint to restore from.
	 * @return Path to the savepoint to restore from or <code>null</code> if
	 * should not restore.
	 */
	public String getRestorePath() {
		return restorePath;
	}

	/**
	 * Returns whether non restored state is allowed if the savepoint contains
	 * state that cannot be mapped back to the job.
	 *
	 * @return <code>true</code> if non restored state is allowed if the savepoint
	 * contains state that cannot be mapped  back to the job.
	 */
	public boolean allowNonRestoredState() {
		return allowNonRestoredState;
	}

	/**
	 * Returns whether to resume latest completed checkpoint automatically.
	 *
	 * @return <code>true</code> if resuming from latest completed checkpoint automatically.
	 */
	public boolean resumeFromLatestCheckpoint() {
		return resumeFromLatestCheckpoint;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) { return true; }
		if (o == null || getClass() != o.getClass()) { return false; }

		SavepointRestoreSettings that = (SavepointRestoreSettings) o;
		return allowNonRestoredState == that.allowNonRestoredState &&
				(restorePath != null ? restorePath.equals(that.restorePath) : that.restorePath == null);
	}

	@Override
	public int hashCode() {
		int result = restorePath != null ? restorePath.hashCode() : 0;
		result = 31 * result + (allowNonRestoredState ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		if (restoreSavepoint()) {
			return "SavepointRestoreSettings.forPath(" +
					"restorePath='" + restorePath + '\'' +
					", allowNonRestoredState=" + allowNonRestoredState +
					')';
		} else {
			return "SavepointRestoreSettings.none()";
		}
	}

	// ------------------------------------------------------------------------

	public static SavepointRestoreSettings none() {
		return NONE;
	}

	public static SavepointRestoreSettings forPath(String savepointPath) {
		return forPath(savepointPath, DEFAULT_ALLOW_NON_RESTORED_STATE);
	}

	public static SavepointRestoreSettings forPath(String savepointPath, boolean allowNonRestoredState) {
		checkNotNull(savepointPath, "Savepoint restore path.");
		return new SavepointRestoreSettings(savepointPath, allowNonRestoredState, false);
	}

	public static SavepointRestoreSettings forResumePath(String checkpointPath, boolean allowNonRestoredState) {
		checkNotNull(checkpointPath, "Checkpoint resume path.");
		return new SavepointRestoreSettings(checkpointPath, allowNonRestoredState, true);
	}

}
