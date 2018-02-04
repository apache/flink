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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Options for performing the checkpoint.
 *
 * <p>The {@link CheckpointProperties} are related and cover properties that
 * are only relevant at the {@link CheckpointCoordinator}. These options are
 * relevant at the {@link AbstractInvokable} instances running on task managers.
 */
public class CheckpointOptions implements Serializable {

	private static final long serialVersionUID = 5010126558083292915L;

	/** Type of the checkpoint. */
	private final CheckpointType checkpointType;

	/** Target location for the checkpoint. */
	private final CheckpointStorageLocationReference targetLocation;

	public CheckpointOptions(
			CheckpointType checkpointType,
			CheckpointStorageLocationReference targetLocation) {

		this.checkpointType = checkNotNull(checkpointType);
		this.targetLocation = checkNotNull(targetLocation);
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the type of checkpoint to perform.
	 */
	public CheckpointType getCheckpointType() {
		return checkpointType;
	}

	/**
	 * Returns the target location for the checkpoint.
	 */
	public CheckpointStorageLocationReference getTargetLocation() {
		return targetLocation;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 31 * targetLocation.hashCode() + checkpointType.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj != null && obj.getClass() == CheckpointOptions.class) {
			final CheckpointOptions that = (CheckpointOptions) obj;
			return this.checkpointType == that.checkpointType &&
					this.targetLocation.equals(that.targetLocation);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "CheckpointOptions: " + checkpointType + " @ " + targetLocation;
	}

	// ------------------------------------------------------------------------
	//  Factory methods
	// ------------------------------------------------------------------------

	private static final CheckpointOptions CHECKPOINT_AT_DEFAULT_LOCATION =
			new CheckpointOptions(CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());

	public static CheckpointOptions forCheckpointWithDefaultLocation() {
		return CHECKPOINT_AT_DEFAULT_LOCATION;
	}
}
