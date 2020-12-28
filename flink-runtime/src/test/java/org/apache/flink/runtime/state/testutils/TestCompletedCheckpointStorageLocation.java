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

package org.apache.flink.runtime.state.testutils;

import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link CompletedCheckpointStorageLocation} for tests. */
public class TestCompletedCheckpointStorageLocation implements CompletedCheckpointStorageLocation {

    private final StreamStateHandle metadataHandle;

    private final String pointer;

    private boolean disposed;

    public TestCompletedCheckpointStorageLocation() {
        this(new EmptyStreamStateHandle(), "<pointer>");
    }

    public TestCompletedCheckpointStorageLocation(
            StreamStateHandle metadataHandle, String pointer) {
        this.metadataHandle = checkNotNull(metadataHandle);
        this.pointer = checkNotNull(pointer);
    }

    public boolean isDisposed() {
        return disposed;
    }

    @Override
    public String getExternalPointer() {
        return pointer;
    }

    @Override
    public StreamStateHandle getMetadataHandle() {
        return metadataHandle;
    }

    @Override
    public void disposeStorageLocation() throws IOException {
        disposed = true;
    }
}
