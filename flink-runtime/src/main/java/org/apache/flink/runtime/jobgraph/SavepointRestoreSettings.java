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
	private final static SavepointRestoreSettings NONE = new SavepointRestoreSettings(null, false);

	/** By default, be strict when restoring from a savepoint.  */
	private final static boolean DEFAULT_IGNORE_UNMAPPED_STATE = false;

	/** Savepoint restore path. */
	private final String restorePath;

	/**
	 * Flag indicating whether the restore should ignore if the savepoint contains
	 * state for an operator that is not part of the job.
	 */
	private final boolean ignoreUnmappedState;

	/**
	 * Creates the restore settings.
	 *
	 * @param restorePath Savepoint restore path.
	 * @param ignoreUnmappedState Ignore unmapped state.
	 */
	private SavepointRestoreSettings(String restorePath, boolean ignoreUnmappedState) {
		this.restorePath = restorePath;
		this.ignoreUnmappedState = ignoreUnmappedState;
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
	 * Returns whether the restore should ignore whether the savepoint contains
	 * state that cannot be mapped to the job.
	 *
	 * @return <code>true</code> if restore should ignore whether the savepoint contains
	 * state that cannot be mapped to the job.
	 */
	public boolean ignoreUnmappedState() {
		return ignoreUnmappedState;
	}

	@Override
	public String toString() {
		if (restoreSavepoint()) {
			return "SavepointRestoreSettings.forPath(" +
					"restorePath='" + restorePath + '\'' +
					", ignoreUnmappedState=" + ignoreUnmappedState +
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
		return forPath(savepointPath, DEFAULT_IGNORE_UNMAPPED_STATE);
	}

	public static SavepointRestoreSettings forPath(String savepointPath, boolean ignoreUnmappedState) {
		checkNotNull(savepointPath, "Savepoint restore path.");
		return new SavepointRestoreSettings(savepointPath, ignoreUnmappedState);
	}

}
