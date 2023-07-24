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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.util.Optional;

/**
 * A placeholder state handle for shared state that will replaced by an original that was created in
 * a previous checkpoint. So we don't have to send a state handle twice, e.g. in case of {@link
 * ByteStreamStateHandle}. This class is used in the referenced states of {@link
 * IncrementalRemoteKeyedStateHandle}.
 */
public class PlaceholderStreamStateHandle implements StreamStateHandle {

    private static final long serialVersionUID = 1L;

    private final PhysicalStateHandleID physicalID;
    private final long stateSize;

    public PlaceholderStreamStateHandle(PhysicalStateHandleID physicalID, long stateSize) {
        this.physicalID = physicalID;
        this.stateSize = stateSize;
    }

    @Override
    public FSDataInputStream openInputStream() {
        throw new UnsupportedOperationException(
                "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    @Override
    public Optional<byte[]> asBytesIfInMemory() {
        throw new UnsupportedOperationException(
                "This is only a placeholder to be replaced by a real StreamStateHandle in the checkpoint coordinator.");
    }

    @Override
    public PhysicalStateHandleID getStreamStateHandleID() {
        return physicalID;
    }

    @Override
    public void discardState() throws Exception {
        // nothing to do.
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }
}
