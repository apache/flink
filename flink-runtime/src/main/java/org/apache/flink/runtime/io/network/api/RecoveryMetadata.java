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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

/** {@link RecoveryMetadata} contains the metadata used during a recovery process. */
@Internal
public class RecoveryMetadata extends RuntimeEvent {
    private final int finalBufferSubpartitionId;

    public RecoveryMetadata(int finalBufferSubpartitionId) {
        this.finalBufferSubpartitionId = finalBufferSubpartitionId;
    }

    /** @return the index of the subpartition where the last buffer stored in a snapshot locates. */
    public int getFinalBufferSubpartitionId() {
        return finalBufferSubpartitionId;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hashCode(this.finalBufferSubpartitionId);
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null
                && obj.getClass() == RecoveryMetadata.class
                && ((RecoveryMetadata) obj).finalBufferSubpartitionId
                        == this.finalBufferSubpartitionId;
    }

    @Override
    public String toString() {
        return "RecoveryMetadata{subpartitionId=" + finalBufferSubpartitionId + "}";
    }
}
