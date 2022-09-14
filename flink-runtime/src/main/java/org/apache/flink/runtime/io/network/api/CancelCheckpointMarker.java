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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

/**
 * The CancelCheckpointMarker travels through the data streams, similar to the {@link
 * CheckpointBarrier}, but signals that a certain checkpoint should be canceled. Any in-progress
 * alignment for that checkpoint needs to be canceled and regular processing should be resumed.
 */
public class CancelCheckpointMarker extends RuntimeEvent {

    /** The id of the checkpoint to be canceled. */
    private final long checkpointId;

    public CancelCheckpointMarker(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    // ------------------------------------------------------------------------
    // These known and common event go through special code paths, rather than
    // through generic serialization.

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("this method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("this method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return (int) (checkpointId ^ (checkpointId >>> 32));
    }

    @Override
    public boolean equals(Object other) {
        return other != null
                && other.getClass() == CancelCheckpointMarker.class
                && this.checkpointId == ((CancelCheckpointMarker) other).checkpointId;
    }

    @Override
    public String toString() {
        return "CancelCheckpointMarker " + checkpointId;
    }
}
