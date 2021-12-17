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

package org.apache.flink.runtime.state;

import java.io.IOException;

/**
 * A storage location for one particular checkpoint, offering data persistent, metadata persistence,
 * and lifecycle/cleanup methods.
 *
 * <p>CheckpointStorageLocations are typically created and initialized via {@link
 * CheckpointStorageAccess#initializeLocationForCheckpoint(long)} or {@link
 * CheckpointStorageAccess#initializeLocationForSavepoint(long, String)}.
 */
public interface CheckpointStorageLocation extends CheckpointStreamFactory {

    /**
     * Creates the output stream to persist the checkpoint metadata to.
     *
     * @return The output stream to persist the checkpoint metadata to.
     * @throws IOException Thrown, if the stream cannot be opened due to an I/O error.
     */
    CheckpointMetadataOutputStream createMetadataOutputStream() throws IOException;

    /**
     * Disposes the checkpoint location in case the checkpoint has failed. This method disposes all
     * the data at that location, not just the data written by the particular node or process that
     * calls this method.
     */
    void disposeOnFailure() throws IOException;

    /**
     * Gets a reference to the storage location. This reference is sent to the target storage
     * location via checkpoint RPC messages and checkpoint barriers, in a format avoiding
     * backend-specific classes.
     *
     * <p>If there is no custom location information that needs to be communicated, this method can
     * simply return {@link CheckpointStorageLocationReference#getDefault()}.
     */
    CheckpointStorageLocationReference getLocationReference();
}
