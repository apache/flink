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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;

/**
 * Encapsulates all the meta data for a checkpoint.
 */
public class CheckpointMetaData implements Serializable {

	private static final long serialVersionUID = -2387652345781312442L;

	/** The ID of the checkpoint. */
	private final long checkpointId;

	/** The timestamp of the checkpoint. */
	private final long timestamp;

	public CheckpointMetaData(long checkpointId, long timestamp) {
		this.checkpointId = checkpointId;
		this.timestamp = timestamp;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CheckpointMetaData that = (CheckpointMetaData) o;

		return (checkpointId == that.checkpointId)
				&& (timestamp == that.timestamp);
	}

	@Override
	public int hashCode() {
		int result = (int) (checkpointId ^ (checkpointId >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "CheckpointMetaData{" +
				"checkpointId=" + checkpointId +
				", timestamp=" + timestamp +
				'}';
	}
}
