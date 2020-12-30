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

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Statistics for a restored checkpoint. */
public class RestoredCheckpointStats implements Serializable {

    private static final long serialVersionUID = 2305815319666360821L;

    /** ID of the restored checkpoint. */
    private final long checkpointId;

    /** Properties of the restored checkpoint. */
    private final CheckpointProperties props;

    /** Timestamp when the checkpoint was restored at the coordinator. */
    private final long restoreTimestamp;

    /** Optional external path. */
    @Nullable private final String externalPath;

    /**
     * Creates a new restored checkpoint stats.
     *
     * @param checkpointId ID of the checkpoint.
     * @param props Checkpoint properties of the checkpoint.
     * @param restoreTimestamp Timestamp when the checkpoint was restored.
     * @param externalPath Optional external path if persisted externally.
     */
    RestoredCheckpointStats(
            long checkpointId,
            CheckpointProperties props,
            long restoreTimestamp,
            @Nullable String externalPath) {

        this.checkpointId = checkpointId;
        this.props = checkNotNull(props, "Checkpoint Properties");
        this.restoreTimestamp = restoreTimestamp;
        this.externalPath = externalPath;
    }

    /**
     * Returns the ID of this checkpoint.
     *
     * @return ID of this checkpoint.
     */
    public long getCheckpointId() {
        return checkpointId;
    }

    /**
     * Returns the properties of the restored checkpoint.
     *
     * @return Properties of the restored checkpoint.
     */
    public CheckpointProperties getProperties() {
        return props;
    }

    /**
     * Returns the timestamp when the checkpoint was restored.
     *
     * @return Timestamp when the checkpoint was restored.
     */
    public long getRestoreTimestamp() {
        return restoreTimestamp;
    }

    /**
     * Returns the external path if this checkpoint was persisted externally.
     *
     * @return External path of this checkpoint or <code>null</code>.
     */
    @Nullable
    public String getExternalPath() {
        return externalPath;
    }
}
