package org.apache.flink.runtime.state;/*
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

import org.apache.flink.util.ExceptionUtils;

/**
 *
 */
public class SnapshotResult<T extends StateObject> implements StateObject {

	private static final long serialVersionUID = 1L;

	private final T jobManagerOwnedSnapshot;
	private final T taskLocalSnapshot;

	public SnapshotResult(T jobManagerOwnedSnapshot, T taskLocalSnapshot) {

		if (jobManagerOwnedSnapshot == null && taskLocalSnapshot != null) {
			throw new IllegalStateException("Cannot report local state snapshot without corresponding remote state!");
		}

		this.jobManagerOwnedSnapshot = jobManagerOwnedSnapshot;
		this.taskLocalSnapshot = taskLocalSnapshot;
	}

	public T getJobManagerOwnedSnapshot() {
		return jobManagerOwnedSnapshot;
	}

	public T getTaskLocalSnapshot() {
		return taskLocalSnapshot;
	}

	public boolean hasState() {
		return jobManagerOwnedSnapshot != null;
	}

	@Override
	public void discardState() throws Exception {

		Exception aggregatedExceptions = null;

		if (jobManagerOwnedSnapshot != null) {
			try {
				jobManagerOwnedSnapshot.discardState();
			} catch (Exception remoteDiscardEx) {
				aggregatedExceptions = remoteDiscardEx;
			}
		}

		if (taskLocalSnapshot != null) {
			try {
				taskLocalSnapshot.discardState();
			} catch (Exception localDiscardEx) {
				aggregatedExceptions = ExceptionUtils.firstOrSuppressed(localDiscardEx, aggregatedExceptions);
			}
		}

		if (aggregatedExceptions != null) {
			throw aggregatedExceptions;
		}
	}

	@Override
	public long getStateSize() {
		return jobManagerOwnedSnapshot != null ? jobManagerOwnedSnapshot.getStateSize() : 0L;
	}
}
