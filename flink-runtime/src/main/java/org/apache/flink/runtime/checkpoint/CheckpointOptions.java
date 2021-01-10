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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Options for performing the checkpoint. Note that different {@link
 * org.apache.flink.runtime.io.network.api.CheckpointBarrier barriers} may have different options.
 *
 * <p>The {@link CheckpointProperties} are related and cover properties that are only relevant at
 * the {@link CheckpointCoordinator}. These options are relevant at the {@link AbstractInvokable}
 * instances running on task managers.
 */
public class CheckpointOptions implements Serializable {

    public static final long NO_ALIGNMENT_TIME_OUT = Long.MAX_VALUE;

    private static final long serialVersionUID = 5010126558083292915L;

    /** Type of the checkpoint. */
    private final CheckpointType checkpointType;

    /** Target location for the checkpoint. */
    private final CheckpointStorageLocationReference targetLocation;

    private final boolean isExactlyOnceMode;

    private final boolean isUnalignedCheckpoint;

    private final long alignmentTimeout;

    public static CheckpointOptions notExactlyOnce(
            CheckpointType type, CheckpointStorageLocationReference location) {
        return new CheckpointOptions(type, location, false, false, NO_ALIGNMENT_TIME_OUT);
    }

    public static CheckpointOptions alignedNoTimeout(
            CheckpointType type, CheckpointStorageLocationReference location) {
        return new CheckpointOptions(type, location, true, false, NO_ALIGNMENT_TIME_OUT);
    }

    public static CheckpointOptions unaligned(CheckpointStorageLocationReference location) {
        return new CheckpointOptions(
                CheckpointType.CHECKPOINT, location, true, true, NO_ALIGNMENT_TIME_OUT);
    }

    public static CheckpointOptions alignedWithTimeout(
            CheckpointStorageLocationReference location, long alignmentTimeout) {
        return new CheckpointOptions(
                CheckpointType.CHECKPOINT, location, true, false, alignmentTimeout);
    }

    public static CheckpointOptions forConfig(
            CheckpointType checkpointType,
            CheckpointStorageLocationReference locationReference,
            boolean isExactlyOnceMode,
            boolean isUnalignedEnabled,
            long alignmentTimeout) {
        if (!isExactlyOnceMode) {
            return notExactlyOnce(checkpointType, locationReference);
        } else if (checkpointType.isSavepoint()) {
            return alignedNoTimeout(checkpointType, locationReference);
        } else if (!isUnalignedEnabled) {
            return alignedNoTimeout(checkpointType, locationReference);
        } else if (alignmentTimeout == 0 || alignmentTimeout == NO_ALIGNMENT_TIME_OUT) {
            return unaligned(locationReference);
        } else {
            return alignedWithTimeout(locationReference, alignmentTimeout);
        }
    }

    @VisibleForTesting
    public CheckpointOptions(
            CheckpointType checkpointType, CheckpointStorageLocationReference targetLocation) {
        this(checkpointType, targetLocation, true, false, NO_ALIGNMENT_TIME_OUT);
    }

    public CheckpointOptions(
            CheckpointType checkpointType,
            CheckpointStorageLocationReference targetLocation,
            boolean isExactlyOnceMode,
            boolean isUnalignedCheckpoint,
            long alignmentTimeout) {

        checkArgument(
                !isUnalignedCheckpoint || !checkpointType.isSavepoint(),
                "Savepoint can't be unaligned");
        checkArgument(
                alignmentTimeout == NO_ALIGNMENT_TIME_OUT || !isUnalignedCheckpoint,
                "Unaligned checkpoint can't have timeout (%s)",
                alignmentTimeout);
        this.checkpointType = checkNotNull(checkpointType);
        this.targetLocation = checkNotNull(targetLocation);
        this.isExactlyOnceMode = isExactlyOnceMode;
        this.isUnalignedCheckpoint = isUnalignedCheckpoint;
        this.alignmentTimeout = alignmentTimeout;
    }

    public boolean needsAlignment() {
        return isExactlyOnceMode()
                && (getCheckpointType().isSavepoint() || !isUnalignedCheckpoint());
    }

    public long getAlignmentTimeout() {
        return alignmentTimeout;
    }

    public boolean isTimeoutable() {
        return isExactlyOnceMode
                && !isUnalignedCheckpoint
                && (alignmentTimeout > 0 && alignmentTimeout != NO_ALIGNMENT_TIME_OUT);
    }

    // ------------------------------------------------------------------------

    /** Returns the type of checkpoint to perform. */
    public CheckpointType getCheckpointType() {
        return checkpointType;
    }

    /** Returns the target location for the checkpoint. */
    public CheckpointStorageLocationReference getTargetLocation() {
        return targetLocation;
    }

    public boolean isExactlyOnceMode() {
        return isExactlyOnceMode;
    }

    public boolean isUnalignedCheckpoint() {
        return isUnalignedCheckpoint;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(
                targetLocation,
                checkpointType,
                isExactlyOnceMode,
                isUnalignedCheckpoint,
                alignmentTimeout);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj != null && obj.getClass() == CheckpointOptions.class) {
            final CheckpointOptions that = (CheckpointOptions) obj;
            return this.checkpointType == that.checkpointType
                    && this.targetLocation.equals(that.targetLocation)
                    && this.isExactlyOnceMode == that.isExactlyOnceMode
                    && this.isUnalignedCheckpoint == that.isUnalignedCheckpoint
                    && this.alignmentTimeout == that.alignmentTimeout;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "CheckpointOptions {"
                + "checkpointType = "
                + checkpointType
                + ", targetLocation = "
                + targetLocation
                + ", isExactlyOnceMode = "
                + isExactlyOnceMode
                + ", isUnalignedCheckpoint = "
                + isUnalignedCheckpoint
                + ", alignmentTimeout = "
                + alignmentTimeout
                + "}";
    }

    // ------------------------------------------------------------------------
    //  Factory methods
    // ------------------------------------------------------------------------

    private static final CheckpointOptions CHECKPOINT_AT_DEFAULT_LOCATION =
            new CheckpointOptions(
                    CheckpointType.CHECKPOINT, CheckpointStorageLocationReference.getDefault());

    @VisibleForTesting
    public static CheckpointOptions forCheckpointWithDefaultLocation() {
        return CHECKPOINT_AT_DEFAULT_LOCATION;
    }

    public CheckpointOptions toUnaligned() {
        checkState(!isUnalignedCheckpoint);
        return unaligned(targetLocation);
    }
}
